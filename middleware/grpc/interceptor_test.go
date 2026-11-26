package grpc_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	grpclimit "github.com/ismoilroziboyev/universalratelimiter/middleware/grpc"
	"github.com/ismoilroziboyev/universalratelimiter/storage/memory"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testService struct {
	pb.UnimplementedTestServiceServer
}

func (testService) UnaryCall(context.Context, *pb.SimpleRequest) (*pb.SimpleResponse, error) {
	return &pb.SimpleResponse{}, nil
}

func (testService) FullDuplexCall(stream grpc.BidiStreamingServer[pb.StreamingOutputCallRequest, pb.StreamingOutputCallResponse]) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			// The interceptor must preserve an admission error even if
			// the application treats a receive error as a normal close.
			return nil
		}
		if err := stream.Send(&pb.StreamingOutputCallResponse{Payload: message.Payload}); err != nil {
			return err
		}
	}
}

func client(t *testing.T, config grpclimit.Config) pb.TestServiceClient {
	t.Helper()
	limit, err := grpclimit.New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(limit.Unary), grpc.ChainStreamInterceptor(limit.Stream))
	pb.RegisterTestServiceServer(server, testService{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewTestServiceClient(conn)
}

func bucket(t *testing.T, capacity int64) *ratelimiter.RateLimiter {
	t.Helper()
	store, err := memory.NewWithConfig(memory.Config{Clock: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	limiter, err := ratelimiter.New(ratelimiter.WithStorage(store),
		ratelimiter.WithTokenBucket(ratelimiter.TokenBucketConfig{Capacity: capacity, RefillRate: 2}))
	if err != nil {
		t.Fatal(err)
	}
	return limiter
}

func TestUnaryDynamicCostAndRetryInfo(t *testing.T) {
	rpc := client(t, grpclimit.Config{Limiter: bucket(t, 3),
		UnaryCostFunc: func(_ context.Context, method string, request any) (int64, error) {
			if method != pb.TestService_UnaryCall_FullMethodName {
				t.Errorf("unexpected method %q", method)
			}
			return int64(request.(*pb.SimpleRequest).ResponseSize), nil
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, call := range []struct {
		cost int32
		code codes.Code
	}{
		{2, codes.OK},
		{2, codes.ResourceExhausted},
		{4, codes.ResourceExhausted},
		{0, codes.InvalidArgument},
	} {
		_, err := rpc.UnaryCall(ctx, &pb.SimpleRequest{ResponseSize: call.cost})
		if status.Code(err) != call.code {
			t.Fatalf("cost %d: got %v, want %s", call.cost, err, call.code)
		}
		if call.code == codes.ResourceExhausted {
			details := status.Convert(err).Details()
			if call.cost == 4 {
				if len(details) != 0 {
					t.Fatalf("permanent rejection included retry details: %v", details)
				}
			} else if len(details) != 1 {
				t.Fatalf("retry details: %v", details)
			} else if retry, ok := details[0].(*errdetails.RetryInfo); !ok || retry.GetRetryDelay().AsDuration() != 500*time.Millisecond {
				t.Fatalf("retry info: %v", details[0])
			}
		}
	}
}

func TestStreamOpenAndMessageOptIn(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity int64
		weighted bool
	}{
		{name: "opening only", capacity: 1},
		{name: "incoming messages", capacity: 4, weighted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := grpclimit.Config{Limiter: bucket(t, tc.capacity)}
			if tc.weighted {
				config.StreamMessageCostFunc = func(_ context.Context, _ string, msg any) (int64, error) {
					return int64(len(msg.(*pb.StreamingOutputCallRequest).Payload.Body)), nil
				}
			}
			rpc := client(t, config)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream, err := rpc.FullDuplexCall(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for index := range 2 {
				if err := stream.Send(&pb.StreamingOutputCallRequest{Payload: &pb.Payload{Body: []byte("ok")}}); err != nil {
					t.Fatal(err)
				}
				reply, err := stream.Recv()
				if index == 1 && tc.weighted {
					if status.Code(err) != codes.ResourceExhausted {
						t.Fatalf("message rejection: %v", err)
					}
					return
				}
				if err != nil || string(reply.GetPayload().GetBody()) != "ok" {
					t.Fatalf("echo failed: %v %v", reply, err)
				}
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("stream end: %v", err)
			}
			next, err := rpc.FullDuplexCall(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := next.Recv(); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("stream opening rejection: %v", err)
			}
		})
	}
}

type failingLimiter struct{}

func (failingLimiter) Allow(context.Context, string, int64) (ratelimiter.Result, error) {
	return ratelimiter.Result{}, errors.New("backend offline")
}

func TestStatusMappingAndSkip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config grpclimit.Config
		cancel bool
		want   codes.Code
	}{
		{name: "fail closed", want: codes.Unavailable},
		{name: "fail open", config: grpclimit.Config{FailureMode: ratelimiter.FailOpen}, want: codes.OK},
		{name: "canceled", config: grpclimit.Config{FailureMode: ratelimiter.FailOpen}, cancel: true, want: codes.Canceled},
		{name: "skipped", config: grpclimit.Config{SkipFunc: func(context.Context, string) bool { return true }}, want: codes.OK},
		{name: "identity", config: grpclimit.Config{KeyFunc: func(context.Context, string) (string, error) {
			return "", status.Error(codes.Unauthenticated, "verified identity required")
		}}, want: codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.config.Limiter = failingLimiter{}
			limit, err := grpclimit.New(tc.config)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			_, err = limit.Unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Check"},
				func(context.Context, any) (any, error) { return nil, nil })
			if status.Code(err) != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}

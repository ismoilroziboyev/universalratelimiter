package memory

import "time"

func (s *Store) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.expire()
		}
	}
}

func (s *Store) expire() {
	for i := range s.shards {
		part := &s.shards[i]
		part.mu.Lock()
		// Read time under the same lock used by operations.
		now, err := s.timestamp()
		if err == nil {
			for key, state := range part.states {
				if state.Expires <= now {
					delete(part.states, key)
					s.count.Add(-1)
				}
			}
			if len(part.states) == 0 {
				part.states = nil
			}
		}
		part.mu.Unlock()
	}
}

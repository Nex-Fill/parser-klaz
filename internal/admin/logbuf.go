package admin

import (
	"io"
	"sync"
	"time"
)

type LogEntry struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

type LogBuffer struct {
	mu      sync.RWMutex
	entries []LogEntry
	maxSize int
	pos     int
	full    bool
}

func NewLogBuffer(size int) *LogBuffer {
	return &LogBuffer{
		entries: make([]LogEntry, size),
		maxSize: size,
	}
}

func (lb *LogBuffer) Write(p []byte) (n int, err error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.entries[lb.pos] = LogEntry{
		Time:    time.Now(),
		Message: string(p),
	}
	lb.pos = (lb.pos + 1) % lb.maxSize
	if lb.pos == 0 {
		lb.full = true
	}
	return len(p), nil
}

func (lb *LogBuffer) Entries() []LogEntry {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	var result []LogEntry
	if lb.full {
		result = make([]LogEntry, lb.maxSize)
		copy(result, lb.entries[lb.pos:])
		copy(result[lb.maxSize-lb.pos:], lb.entries[:lb.pos])
	} else {
		result = make([]LogEntry, lb.pos)
		copy(result, lb.entries[:lb.pos])
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

var _ io.Writer = (*LogBuffer)(nil)

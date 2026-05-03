package domain

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ClickID    string
	CampaignID uuid.UUID
	Type       string
	Payload    []byte
	IP         string
	UA         string
	CreatedAt  time.Time
}

func (e *Event) Reset() {
	e.ClickID = ""
	e.CampaignID = uuid.Nil
	e.Type = ""
	if cap(e.Payload) > 4096 {
		e.Payload = make([]byte, 0, 1024)
	} else {
		e.Payload = e.Payload[:0]
	}
	e.IP = ""
	e.UA = ""
	e.CreatedAt = time.Time{}
}

var EventPool = sync.Pool{
	New: func() any {
		return &Event{
			Payload: make([]byte, 0, 1024),
		}
	},
}

type EventStore interface {
	StoreBatch(ctx context.Context, events []*Event) error
	Close() error
}

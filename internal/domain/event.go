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
	UserID     string
	Type       string
	Payload    []byte
	IP         string
	UA          string
	FraudReason string
	CreatedAt   time.Time
}

func (e *Event) Reset() {
	e.ClickID = ""
	e.CampaignID = uuid.Nil
	e.UserID = ""
	e.Type = ""
	if cap(e.Payload) > 4096 {
		e.Payload = make([]byte, 0, 1024)
	} else {
		e.Payload = e.Payload[:0]
	}
	e.IP = ""
	e.UA = ""
	e.FraudReason = ""
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

package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	OrderPlaced      = "order.placed"
	PaymentCompleted = "payment.completed"
	PaymentFailed    = "payment.failed"
	OrderReady       = "order.ready"
)

type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Source    string          `json:"source"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

func New(eventType, source string, data any) (Envelope, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, err
	}

	return Envelope{
		ID:        uuid.NewString(),
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Data:      payload,
	}, nil
}

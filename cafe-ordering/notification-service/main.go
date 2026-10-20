package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"cafe/shared/broker"
	"cafe/shared/events"
	"cafe/shared/httpx"
	"cafe/shared/store"

	"github.com/google/uuid"
)

const (
	serviceName = "notification-service"
	queueName   = "notification-service.queue"
)

type Service struct {
	db *sql.DB
}

type EventData struct {
	OrderID       string  `json:"order_id"`
	PaymentID     string  `json:"payment_id"`
	TicketID      string  `json:"ticket_id"`
	Total         float64 `json:"total"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
	Reason        string  `json:"reason"`
	PaymentMethod string  `json:"payment_method"`
}

type Notification struct {
	ID        string    `json:"id"`
	EventID   string    `json:"event_id"`
	OrderID   string    `json:"order_id"`
	EventType string    `json:"event_type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	databaseURL := os.Getenv("NOTIFICATIONS_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://cafe:cafe_dev_password@localhost:5435/notifications_db?sslmode=disable"
	}

	db, err := store.Open(databaseURL)
	if err != nil {
		log.Fatalf("Notification database connection failed: %v", err)
	}
	defer db.Close()

	s := &Service{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /notifications", s.listNotifications)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8084"
	}

	go s.consumeEvents()

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Notification Service listening on http://localhost:%s", port)
	log.Fatal(server.ListenAndServe())
}

func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "Database unavailable")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]string{
		"service": serviceName,
		"status":  "ok",
	})
}

func (s *Service) listNotifications(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("order_id")

	var (
		rows *sql.Rows
		err  error
	)

	if orderID != "" {
		if _, parseErr := uuid.Parse(orderID); parseErr != nil {
			httpx.Error(w, http.StatusBadRequest, "order_id must be a valid UUID")
			return
		}

		rows, err = s.db.QueryContext(r.Context(), `
			SELECT id, event_id, order_id, event_type, message, created_at
			FROM notifications
			WHERE order_id = $1
			ORDER BY created_at DESC
			LIMIT 50
		`, orderID)
	} else {
		rows, err = s.db.QueryContext(r.Context(), `
			SELECT id, event_id, order_id, event_type, message, created_at
			FROM notifications
			ORDER BY created_at DESC
			LIMIT 50
		`)
	}

	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not load notifications")
		return
	}
	defer rows.Close()

	notifications := make([]Notification, 0)
	for rows.Next() {
		var item Notification
		if err := rows.Scan(
			&item.ID,
			&item.EventID,
			&item.OrderID,
			&item.EventType,
			&item.Message,
			&item.CreatedAt,
		); err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not read notifications")
			return
		}
		notifications = append(notifications, item)
	}

	if err := rows.Err(); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not read notifications")
		return
	}

	httpx.JSON(w, http.StatusOK, notifications)
}

func (s *Service) consumeEvents() {
	for {
		if err := s.consume(); err != nil {
			log.Printf("Notification consumer stopped: %v; retrying in 5 seconds", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (s *Service) consume() error {
	conn, ch, err := broker.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	defer ch.Close()

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return err
	}

	eventTypes := []string{
		events.OrderPlaced,
		events.PaymentCompleted,
		events.PaymentFailed,
		events.OrderReady,
	}

	for _, eventType := range eventTypes {
		if err := ch.QueueBind(queueName, eventType, broker.ExchangeName, false, nil); err != nil {
			return err
		}
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}

	messages, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Println("Notification Service waiting for café events")
	for msg := range messages {
		if err := s.processEvent(msg.Body); err != nil {
			log.Printf("Could not process notification event: %v", err)
			if nackErr := msg.Nack(false, true); nackErr != nil {
				return nackErr
			}
			continue
		}

		if err := msg.Ack(false); err != nil {
			return err
		}
	}
	return errors.New("notification consumer channel closed")
}

func (s *Service) processEvent(body []byte) error {
	var envelope events.Envelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}

	if _, err := uuid.Parse(envelope.ID); err != nil {
		return errors.New("event has an invalid event ID")
	}

	var data EventData
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return err
	}

	if _, err := uuid.Parse(data.OrderID); err != nil {
		return errors.New("event has an invalid order_id")
	}

	message, err := makeMessage(envelope.Type, data)
	if err != nil {
		return err
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO notifications
			(id, event_id, order_id, event_type, message)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING
	`, uuid.NewString(), envelope.ID, data.OrderID, envelope.Type, message)
	if err != nil {
		return err
	}

	count, err := result.RowsAffected()
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO notification_processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
	`, envelope.ID)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if count > 0 {
		log.Printf("Saved %s notification for order %s", envelope.Type, data.OrderID)
	} else {
		log.Printf("Duplicate event %s ignored", envelope.ID)
	}

	return nil
}

func makeMessage(eventType string, data EventData) (string, error) {
	switch eventType {
	case events.OrderPlaced:
		return "We received your order " + shortOrderID(data.OrderID) +
			". Total: " + formatNu(data.Total) + ". Processing payment.", nil

	case events.PaymentCompleted:
		return "Payment of " + formatNu(data.Amount) + " received for order " +
			shortOrderID(data.OrderID) + ". The kitchen is preparing your order.", nil

	case events.PaymentFailed:
		reason := data.Reason
		if reason == "" {
			reason = "payment was declined"
		}
		return "Payment for order " + shortOrderID(data.OrderID) +
			" failed: " + reason + ".", nil

	case events.OrderReady:
		return "Your order " + shortOrderID(data.OrderID) +
			" is READY for pick-up at the counter.", nil

	default:
		return "", errors.New("unsupported event type: " + eventType)
	}
}

func shortOrderID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func formatNu(amount float64) string {
	return "Nu." + formatAmount(amount)
}

func formatAmount(amount float64) string {
	whole := int64(amount)
	fraction := int64((amount-float64(whole))*100 + 0.5)
	if fraction >= 100 {
		whole++
		fraction = 0
	}
	return itoa(whole) + "." + twoDigits(fraction)
}

func itoa(value int64) string {
	if value == 0 {
		return "0"
	}
	var buffer [24]byte
	position := len(buffer)
	for value > 0 {
		position--
		buffer[position] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[position:])
}

func twoDigits(value int64) string {
	return string([]byte{byte('0' + value/10), byte('0' + value%10)})
}

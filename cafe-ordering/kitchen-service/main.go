package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cafe/shared/broker"
	"cafe/shared/events"
	"cafe/shared/httpx"
	"cafe/shared/store"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	serviceName = "kitchen-service"
	queueName   = "kitchen-service.queue"
	dlqName     = "kitchen-service.dlq"
	dlxName     = "cafe.events.dlx"
)

type OrderItem struct {
	MenuItemID string  `json:"menu_item_id"`
	Name       string  `json:"name"`
	Quantity   int     `json:"quantity"`
	UnitPrice  float64 `json:"unit_price"`
}

type PaymentCompletedData struct {
	OrderID   string      `json:"order_id"`
	PaymentID string      `json:"payment_id"`
	Amount    float64     `json:"amount"`
	Status    string      `json:"status"`
	Items     []OrderItem `json:"items"`
}

type Ticket struct {
	ID        string          `json:"id"`
	OrderID   string          `json:"order_id"`
	Items     json.RawMessage `json:"items"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type Service struct {
	db          *sql.DB
	failureMode atomic.Bool
}

func main() {
	databaseURL := os.Getenv("KITCHEN_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://cafe:cafe_dev_password@localhost:5435/kitchen_db?sslmode=disable"
	}

	db, err := store.Open(databaseURL)
	if err != nil {
		log.Fatalf("Kitchen database connection failed: %v", err)
	}
	defer db.Close()

	s := &Service{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /tickets", s.listTickets)
	mux.HandleFunc("POST /tickets/{id}/start", s.startTicket)
	mux.HandleFunc("POST /tickets/{id}/ready", s.readyTicket)
	mux.HandleFunc("/admin/failure-mode", s.failureModeHandler)
	mux.HandleFunc("POST /admin/replay-dlq", s.replayDLQ)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8083"
	}

	go s.consumePayments()
	go s.runOutboxPublisher()

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Kitchen Service listening on http://localhost:%s", port)
	log.Fatal(server.ListenAndServe())
}

func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "Database unavailable")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]any{
		"service":      serviceName,
		"status":       "ok",
		"failure_mode": s.failureMode.Load(),
	})
}

func (s *Service) listTickets(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT id, order_id, items, status, created_at, updated_at
		FROM kitchen_tickets
		ORDER BY created_at DESC
	`)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not load kitchen tickets")
		return
	}
	defer rows.Close()

	tickets := make([]Ticket, 0)
	for rows.Next() {
		var ticket Ticket
		if err := rows.Scan(
			&ticket.ID,
			&ticket.OrderID,
			&ticket.Items,
			&ticket.Status,
			&ticket.CreatedAt,
			&ticket.UpdatedAt,
		); err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not read kitchen tickets")
			return
		}
		tickets = append(tickets, ticket)
	}

	if err := rows.Err(); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not read kitchen tickets")
		return
	}

	httpx.JSON(w, http.StatusOK, tickets)
}

func (s *Service) startTicket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusBadRequest, "Invalid ticket ID")
		return
	}

	result, err := s.db.ExecContext(r.Context(), `
		UPDATE kitchen_tickets
		SET status = 'PREPARING', updated_at = NOW()
		WHERE id = $1 AND status = 'NEW'
	`, id)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not start ticket")
		return
	}

	count, err := result.RowsAffected()
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not verify ticket update")
		return
	}
	if count == 0 {
		httpx.Error(w, http.StatusConflict, "Ticket not found or is not NEW")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]string{
		"ticket_id": id,
		"status":    "PREPARING",
	})
}

func (s *Service) readyTicket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusBadRequest, "Invalid ticket ID")
		return
	}

	ctx := r.Context()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not begin ticket transaction")
		return
	}
	defer tx.Rollback()

	var orderID string
	err = tx.QueryRowContext(ctx, `
		UPDATE kitchen_tickets
		SET status = 'READY', updated_at = NOW()
		WHERE id = $1 AND status = 'PREPARING'
		RETURNING order_id
	`, id).Scan(&orderID)

	if errors.Is(err, sql.ErrNoRows) {
		httpx.Error(w, http.StatusConflict, "Ticket not found or must be PREPARING first")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not mark ticket ready")
		return
	}

	event, err := events.New(events.OrderReady, serviceName, map[string]string{
		"order_id":  orderID,
		"ticket_id": id,
		"status":    "READY",
	})
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not create order.ready event")
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not encode order.ready event")
		return
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO kitchen_outbox_events (id, event_type, source, payload)
		VALUES ($1, $2, $3, $4::jsonb)
	`, event.ID, event.Type, event.Source, string(payload))
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not save order.ready event")
		return
	}

	if err := tx.Commit(); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not commit ticket update")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]string{
		"ticket_id": id,
		"order_id":  orderID,
		"status":    "READY",
	})
}

func (s *Service) failureModeHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		httpx.JSON(w, http.StatusOK, map[string]bool{
			"enabled": s.failureMode.Load(),
		})
	case http.MethodPost:
		var request struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			httpx.Error(w, http.StatusBadRequest, "Expected JSON with an enabled boolean")
			return
		}
		s.failureMode.Store(request.Enabled)
		httpx.JSON(w, http.StatusOK, map[string]bool{
			"enabled": s.failureMode.Load(),
		})
	default:
		httpx.Error(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (s *Service) consumePayments() {
	for {
		if err := s.consumePaymentEvents(); err != nil {
			log.Printf("Kitchen consumer stopped: %v; retrying in 5 seconds", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (s *Service) consumePaymentEvents() error {
	conn, ch, err := broker.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	defer ch.Close()

	if err := ch.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(dlqName, dlqName, dlxName, false, nil); err != nil {
		return err
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": dlqName,
	}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, args); err != nil {
		return err
	}
	if err := ch.QueueBind(queueName, events.PaymentCompleted, broker.ExchangeName, false, nil); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}

	messages, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Println("Kitchen waiting for payment.completed events")
	for msg := range messages {
		if err := s.onPaymentCompleted(msg.Body); err != nil {
			log.Printf("Kitchen event failed: %v; sending message to DLQ", err)
			if nackErr := msg.Nack(false, false); nackErr != nil {
				return nackErr
			}
			continue
		}
		if err := msg.Ack(false); err != nil {
			return err
		}
	}
	return errors.New("kitchen consumer channel closed")
}

func (s *Service) onPaymentCompleted(body []byte) error {
	if s.failureMode.Load() {
		return errors.New("ticket printer offline (failure mode enabled)")
	}

	var envelope events.Envelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if envelope.Type != events.PaymentCompleted {
		return errors.New("unexpected event type: " + envelope.Type)
	}

	var payment PaymentCompletedData
	if err := json.Unmarshal(envelope.Data, &payment); err != nil {
		return err
	}
	if _, err := uuid.Parse(payment.OrderID); err != nil {
		return errors.New("payment.completed event has an invalid order_id")
	}
	if len(payment.Items) == 0 {
		return errors.New("payment.completed event has no items; update Payment Service to include items")
	}
	if _, err := uuid.Parse(envelope.ID); err != nil {
		return errors.New("event has an invalid ID")
	}

	itemsJSON, err := json.Marshal(payment.Items)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var alreadyProcessed bool
	err = tx.QueryRowContext(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM kitchen_processed_events WHERE event_id = $1
		)
	`, envelope.ID).Scan(&alreadyProcessed)
	if err != nil {
		return err
	}
	if alreadyProcessed {
		return nil
	}

	ticketID := uuid.NewString()
	_, err = tx.ExecContext(context.Background(), `
		INSERT INTO kitchen_tickets (id, order_id, items, status)
		VALUES ($1, $2, $3::jsonb, 'NEW')
		ON CONFLICT (order_id) DO NOTHING
	`, ticketID, payment.OrderID, string(itemsJSON))
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(context.Background(), `
		INSERT INTO kitchen_processed_events (event_id)
		VALUES ($1) ON CONFLICT DO NOTHING
	`, envelope.ID)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Kitchen ticket created for order %s", payment.OrderID)
	return nil
}

func (s *Service) replayDLQ(w http.ResponseWriter, r *http.Request) {
	conn, ch, err := broker.Connect()
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "RabbitMQ unavailable")
		return
	}
	defer conn.Close()
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not enable publisher confirms")
		return
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	replayed := 0
	for {
		msg, ok, err := ch.Get(dlqName, false)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not read dead-letter queue")
			return
		}
		if !ok {
			break
		}

		err = ch.PublishWithContext(
			r.Context(),
			broker.ExchangeName,
			events.PaymentCompleted,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    msg.MessageId,
				Body:         msg.Body,
			},
		)
		if err != nil {
			_ = msg.Nack(false, true)
			httpx.Error(w, http.StatusInternalServerError, "Could not republish dead-letter message")
			return
		}

		select {
		case confirmation, open := <-confirms:
			if !open || !confirmation.Ack {
				_ = msg.Nack(false, true)
				httpx.Error(w, http.StatusInternalServerError, "RabbitMQ did not confirm replay")
				return
			}
		case <-r.Context().Done():
			_ = msg.Nack(false, true)
			httpx.Error(w, http.StatusRequestTimeout, "Replay request timed out")
			return
		}

		if err := msg.Ack(false); err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not acknowledge dead-letter message")
			return
		}
		replayed++
	}

	httpx.JSON(w, http.StatusOK, map[string]int{
		"replayed": replayed,
	})
}

func (s *Service) runOutboxPublisher() {
	for {
		conn, ch, err := broker.Connect()
		if err != nil {
			log.Printf("Kitchen outbox: RabbitMQ unavailable: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		if err := ch.Confirm(false); err != nil {
			log.Printf("Kitchen outbox: cannot enable confirms: %v", err)
			_ = ch.Close()
			_ = conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		s.publishPending(ch, confirms)

		_ = ch.Close()
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

func (s *Service) publishPending(
	ch *amqp.Channel,
	confirms <-chan amqp.Confirmation,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_type, payload
		FROM kitchen_outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 100
	`)
	if err != nil {
		log.Printf("Read kitchen outbox: %v", err)
		return
	}

	type pendingEvent struct {
		ID      string
		Type    string
		Payload []byte
	}
	pending := make([]pendingEvent, 0)

	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.ID, &event.Type, &event.Payload); err != nil {
			_ = rows.Close()
			log.Printf("Read kitchen outbox row: %v", err)
			return
		}
		pending = append(pending, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		log.Printf("Read kitchen outbox rows: %v", err)
		return
	}
	_ = rows.Close()

	for _, event := range pending {
		err := ch.PublishWithContext(ctx, broker.ExchangeName, event.Type, false, false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    event.ID,
				Timestamp:    time.Now().UTC(),
				Body:         event.Payload,
			})
		if err != nil {
			log.Printf("Publish kitchen event %s: %v", event.ID, err)
			return
		}

		select {
		case confirmation, open := <-confirms:
			if !open || !confirmation.Ack {
				log.Printf("Kitchen event %s was not confirmed", event.ID)
				return
			}
		case <-ctx.Done():
			log.Printf("Timed out publishing kitchen event %s", event.ID)
			return
		}

		_, err = s.db.ExecContext(context.Background(), `
			UPDATE kitchen_outbox_events
			SET published_at = NOW()
			WHERE id = $1 AND published_at IS NULL
		`, event.ID)
		if err != nil {
			log.Printf("Mark kitchen event %s published: %v", event.ID, err)
			return
		}
		log.Printf("Published kitchen event %s (%s)", event.ID, event.Type)
	}
}

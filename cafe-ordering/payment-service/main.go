package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"cafe/shared/broker"
	"cafe/shared/events"
	"cafe/shared/httpx"
	"cafe/shared/store"
)

const (
	serviceName = "payment-service"
	queueName   = "payment-service.queue"
)

type OrderItemData struct {
	MenuItemID string  `json:"menu_item_id"`
	Name       string  `json:"name"`
	Quantity   int     `json:"quantity"`
	UnitPrice  float64 `json:"unit_price"`
}

type OrderPlacedData struct {
	OrderID       string          `json:"order_id"`
	Total         float64         `json:"total"`
	PaymentMethod string          `json:"payment_method"`
	Items         []OrderItemData `json:"items"`
}

type PaymentResult struct {
	OrderID   string          `json:"order_id"`
	PaymentID string          `json:"payment_id"`
	Amount    float64         `json:"amount"`
	Status    string          `json:"status"`
	Reason    string          `json:"reason,omitempty"`
	Items     []OrderItemData `json:"items,omitempty"`
}

var db *sql.DB

func main() {
	databaseURL := os.Getenv("PAYMENTS_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		databaseURL = "postgres://cafe:cafe_dev_password@localhost:5435/payments_db?sslmode=disable"
	}

	var err error
	db, err = store.Open(databaseURL)
	if err != nil {
		log.Fatalf("Cannot connect to payments database: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			httpx.JSON(w, http.StatusServiceUnavailable, map[string]string{
				"service": serviceName,
				"status":  "database unavailable",
			})
			return
		}

		httpx.JSON(w, http.StatusOK, map[string]string{
			"service": serviceName,
			"status":  "ok",
		})
	})

	mux.HandleFunc("/payments", listPayments)

	// Consume order events and publish payment events independently.
	go consumeOrders()
	go runOutboxPublisher()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Payment Service listening on http://localhost:%s", port)
	log.Fatal(server.ListenAndServe())
}

func listPayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT id, order_id, amount, payment_method, status, reason, created_at
		FROM payments
		ORDER BY created_at DESC
	`)
	if err != nil {
		http.Error(w, "Could not retrieve payments", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type paymentRecord struct {
		ID            string    `json:"id"`
		OrderID       string    `json:"order_id"`
		Amount        float64   `json:"amount"`
		PaymentMethod string    `json:"payment_method"`
		Status        string    `json:"status"`
		Reason        string    `json:"reason,omitempty"`
		CreatedAt     time.Time `json:"created_at"`
	}

	records := make([]paymentRecord, 0)

	for rows.Next() {
		var p paymentRecord
		var reason sql.NullString

		if err := rows.Scan(
			&p.ID,
			&p.OrderID,
			&p.Amount,
			&p.PaymentMethod,
			&p.Status,
			&reason,
			&p.CreatedAt,
		); err != nil {
			http.Error(w, "Could not read payment records", http.StatusInternalServerError)
			return
		}

		if reason.Valid {
			p.Reason = reason.String
		}

		records = append(records, p)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Could not read payment records", http.StatusInternalServerError)
		return
	}

	httpx.JSON(w, http.StatusOK, records)
}

// Reconnect to RabbitMQ if the consumer connection fails.
func consumeOrders() {
	for {
		if err := consume(); err != nil {
			log.Printf("Payment consumer stopped: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func consume() error {
	conn, ch, err := broker.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		queueName,
		events.OrderPlaced,
		broker.ExchangeName,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}

	messages, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Println("Waiting for order.placed events...")

	for msg := range messages {
		// processOrder commits the payment and its outgoing event
		// before this consumer acknowledges the incoming message.
		if err := processOrder(msg); err != nil {
			log.Printf("Failed to process message: %v", err)

			if nackErr := msg.Nack(false, true); nackErr != nil {
				return nackErr
			}
			continue
		}

		if err := msg.Ack(false); err != nil {
			return err
		}
	}

	return errors.New("RabbitMQ consumer channel closed")
}

// Process an order and atomically store its payment and outgoing event.
// This function does not publish directly to RabbitMQ.
func processOrder(msg amqp.Delivery) error {
	var envelope events.Envelope
	if err := json.Unmarshal(msg.Body, &envelope); err != nil {
		return err
	}

	if envelope.Type != events.OrderPlaced {
		return fmt.Errorf("unexpected event type: %s", envelope.Type)
	}

	var order OrderPlacedData
	if err := json.Unmarshal(envelope.Data, &order); err != nil {
		return err
	}

	if order.OrderID == "" {
		return errors.New("order.placed event has no order_id")
	}

	orderUUID, err := uuid.Parse(order.OrderID)
	if err != nil {
		return fmt.Errorf("invalid order ID %q: %w", order.OrderID, err)
	}

	method := order.PaymentMethod
	if method == "" {
		method = "card"
	}

	result := PaymentResult{
		OrderID: order.OrderID,
		Amount:  order.Total,
		Items:   order.Items,
	}

	switch {
	case method == "declined_card":
		result.Status = "FAILED"
		result.Reason = "Card was declined"
	case order.Total > 5000:
		result.Status = "FAILED"
		result.Reason = "Order total exceeds Nu. 5000"
	default:
		result.Status = "COMPLETED"
	}

	result.PaymentID = uuid.NewString()

	eventType := events.PaymentCompleted
	if result.Status == "FAILED" {
		eventType = events.PaymentFailed
	}

	outgoing, err := events.New(eventType, serviceName, result)
	if err != nil {
		return err
	}

	body, err := json.Marshal(outgoing)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prevent duplicate payment records when the same order event
	// is delivered again.
	var existingStatus string
	err = tx.QueryRowContext(
		context.Background(),
		`SELECT status FROM payments WHERE order_id = $1`,
		orderUUID,
	).Scan(&existingStatus)

	if err == nil {
		// A previous transaction already processed this order.
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	_, err = tx.ExecContext(
		context.Background(),
		`INSERT INTO payments
			(id, order_id, amount, payment_method, status, reason)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))
		 ON CONFLICT (order_id) DO NOTHING`,
		result.PaymentID,
		orderUUID,
		result.Amount,
		method,
		result.Status,
		result.Reason,
	)
	if err != nil {
		return err
	}

	// Confirm that this transaction inserted the new payment.
	var insertedID string
	err = tx.QueryRowContext(
		context.Background(),
		`SELECT id FROM payments WHERE order_id = $1`,
		orderUUID,
	).Scan(&insertedID)
	if err != nil {
		return err
	}

	if insertedID != result.PaymentID {
		// Another delivery already inserted the payment.
		return nil
	}

	// Persist the outgoing event in the same transaction as the payment.
	_, err = tx.ExecContext(
		context.Background(),
		`INSERT INTO outbox_events (id, event_type, source, payload)
		 VALUES ($1, $2, $3, $4::jsonb)`,
		outgoing.ID,
		eventType,
		serviceName,
		string(body),
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf(
		"Saved payment for order %s: %s (Nu. %.2f); event queued for publication",
		order.OrderID,
		result.Status,
		order.Total,
	)

	return nil
}

// Continuously retry pending outbox events.
// Events are marked published only after RabbitMQ confirms receipt.
func runOutboxPublisher() {
	for {
		conn, ch, err := broker.Connect()
		if err != nil {
			log.Printf("RabbitMQ unavailable for payment outbox; retrying: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		if err := ch.Confirm(false); err != nil {
			log.Printf("Could not enable publisher confirms: %v", err)
			ch.Close()
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		runOutboxBatch(ch, confirms)

		if err := ch.Close(); err != nil {
			log.Printf("Close payment publisher channel: %v", err)
		}
		if err := conn.Close(); err != nil {
			log.Printf("Close payment publisher connection: %v", err)
		}

		time.Sleep(2 * time.Second)
	}
}

func runOutboxBatch(
	ch *amqp.Channel,
	confirms <-chan amqp.Confirmation,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(
		ctx,
		`SELECT id, event_type, payload
		 FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT 100`,
	)
	if err != nil {
		log.Printf("Read payment outbox: %v", err)
		return
	}

	type pendingEvent struct {
		ID      string
		Type    string
		Payload []byte
	}

	var pending []pendingEvent

	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.ID, &event.Type, &event.Payload); err != nil {
			rows.Close()
			log.Printf("Read payment outbox row: %v", err)
			return
		}
		pending = append(pending, event)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		log.Printf("Read payment outbox rows: %v", err)
		return
	}
	rows.Close()

	for _, event := range pending {
		err := ch.PublishWithContext(
			ctx,
			broker.ExchangeName,
			event.Type,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    event.ID,
				Timestamp:    time.Now().UTC(),
				Body:         event.Payload,
			},
		)
		if err != nil {
			log.Printf("Publish payment event %s: %v", event.ID, err)
			return
		}

		select {
		case confirmation, ok := <-confirms:
			if !ok || !confirmation.Ack {
				log.Printf("RabbitMQ did not confirm payment event %s", event.ID)
				return
			}
		case <-ctx.Done():
			log.Printf("Timed out waiting for payment event %s confirmation", event.ID)
			return
		}

		_, err = db.ExecContext(
			context.Background(),
			`UPDATE outbox_events
			 SET published_at = NOW()
			 WHERE id = $1 AND published_at IS NULL`,
			event.ID,
		)
		if err != nil {
			log.Printf("Mark payment event %s published: %v", event.ID, err)
			return
		}

		log.Printf("Published payment event %s (%s)", event.ID, event.Type)
	}
}

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
	amqp "github.com/rabbitmq/amqp091-go"
)

type MenuItem struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

type OrderItemRequest struct {
	MenuItemID string `json:"menu_item_id"`
	Quantity   int    `json:"quantity"`
}

type OrderItem struct {
	MenuItemID string  `json:"menu_item_id"`
	Name       string  `json:"name"`
	Quantity   int     `json:"quantity"`
	UnitPrice  float64 `json:"unit_price"`
}

type Order struct {
	ID        string      `json:"id"`
	Items     []OrderItem `json:"items"`
	Total     float64     `json:"total"`
	Status    string      `json:"status"`
	CreatedAt time.Time   `json:"created_at"`
}

type Service struct {
	db *sql.DB
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://cafe:cafe_dev_password@localhost:5435/orders_db?sslmode=disable"
	}

	db, err := store.Open(dbURL)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	s := &Service{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /menu", s.getMenu)
	mux.HandleFunc("POST /orders", s.createOrder)
	mux.HandleFunc("GET /orders", s.listOrders)
	mux.HandleFunc("GET /orders/{id}", s.getOrder)

	server := &http.Server{
		Addr:              ":8081",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go s.runOutboxPublisher()

	log.Println("Order Service listening on http://localhost:8081")
	log.Fatal(server.ListenAndServe())
}

func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "Database unavailable")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]string{
		"service":  "order-service",
		"status":   "ok",
		"database": "connected",
	})
}

func (s *Service) getMenu(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(
		r.Context(),
		`SELECT id, name, price FROM menu_items ORDER BY id`,
	)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not load menu")
		return
	}
	defer rows.Close()

	items := make([]MenuItem, 0)

	for rows.Next() {
		var item MenuItem
		if err := rows.Scan(&item.ID, &item.Name, &item.Price); err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not read menu")
			return
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not read menu")
		return
	}

	httpx.JSON(w, http.StatusOK, items)
}

func (s *Service) createOrder(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var request struct {
		Items         []OrderItemRequest `json:"items"`
		PaymentMethod string             `json:"payment_method"`
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		httpx.Error(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	// Use card when the client does not specify a payment method.
	if request.PaymentMethod == "" {
		request.PaymentMethod = "card"
	}

	// These methods support successful and declined-payment testing.
	if request.PaymentMethod != "card" &&
		request.PaymentMethod != "declined_card" {
		httpx.Error(w, http.StatusBadRequest, "Unsupported payment method")
		return
	}

	if len(request.Items) == 0 {
		httpx.Error(w, http.StatusBadRequest, "Order must contain at least one item")
		return
	}
	if len(request.Items) > 50 {
		httpx.Error(w, http.StatusBadRequest, "Too many items in one order")
		return
	}

	ctx := r.Context()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not begin order transaction")
		return
	}
	defer tx.Rollback()

	order := Order{
		ID:        uuid.NewString(),
		Status:    "PLACED",
		CreatedAt: time.Now().UTC(),
		Items:     make([]OrderItem, 0, len(request.Items)),
	}

	for _, requested := range request.Items {
		if requested.Quantity < 1 || requested.Quantity > 100 {
			httpx.Error(w, http.StatusBadRequest, "Quantity must be between 1 and 100")
			return
		}

		var item MenuItem
		err := tx.QueryRowContext(
			ctx,
			`SELECT id, name, price FROM menu_items WHERE id = $1`,
			requested.MenuItemID,
		).Scan(&item.ID, &item.Name, &item.Price)

		if errors.Is(err, sql.ErrNoRows) {
			httpx.Error(w, http.StatusBadRequest, "Unknown menu item: "+requested.MenuItemID)
			return
		}
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not look up menu item")
			return
		}

		order.Items = append(order.Items, OrderItem{
			MenuItemID: item.ID,
			Name:       item.Name,
			Quantity:   requested.Quantity,
			UnitPrice:  item.Price,
		})
		order.Total += item.Price * float64(requested.Quantity)
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO orders (id, total, status, created_at)
		 VALUES ($1, $2, $3, $4)`,
		order.ID, order.Total, order.Status, order.CreatedAt,
	)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not save order")
		return
	}

	for _, item := range order.Items {
		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO order_items (order_id, menu_item_id, quantity, unit_price)
			 VALUES ($1, $2, $3, $4)`,
			order.ID, item.MenuItemID, item.Quantity, item.UnitPrice,
		)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not save order items")
			return
		}
	}

	// Include the payment method in the event consumed by Payment Service.
	event, err := events.New(events.OrderPlaced, "order-service", map[string]any{
		"order_id":       order.ID,
		"items":          order.Items,
		"total":          order.Total,
		"payment_method": request.PaymentMethod,
	})
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not create order event")
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not encode order event")
		return
	}

	// Save the event in the same transaction as the order.
	// The background publisher sends it to RabbitMQ.
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO outbox_events (id, event_type, source, payload)
		 VALUES ($1, $2, $3, $4::jsonb)`,
		event.ID, event.Type, event.Source, string(payload),
	)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not save order event")
		return
	}

	if err := tx.Commit(); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not commit order")
		return
	}

	httpx.JSON(w, http.StatusCreated, order)
}

func (s *Service) listOrders(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(
		r.Context(),
		`SELECT id, total, status, created_at
		 FROM orders ORDER BY created_at DESC`,
	)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not load orders")
		return
	}
	defer rows.Close()

	result := make([]Order, 0)

	for rows.Next() {
		var order Order
		if err := rows.Scan(
			&order.ID, &order.Total, &order.Status, &order.CreatedAt,
		); err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not read orders")
			return
		}

		order.Items, err = s.loadOrderItems(r.Context(), s.db, order.ID)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "Could not read order items")
			return
		}

		result = append(result, order)
	}

	if err := rows.Err(); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not read orders")
		return
	}

	httpx.JSON(w, http.StatusOK, result)
}

func (s *Service) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusBadRequest, "Invalid order ID")
		return
	}

	var order Order
	err := s.db.QueryRowContext(
		r.Context(),
		`SELECT id, total, status, created_at
		 FROM orders WHERE id = $1`,
		id,
	).Scan(&order.ID, &order.Total, &order.Status, &order.CreatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "Order not found")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not load order")
		return
	}

	order.Items, err = s.loadOrderItems(r.Context(), s.db, order.ID)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "Could not read order items")
		return
	}

	httpx.JSON(w, http.StatusOK, order)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *Service) loadOrderItems(
	ctx context.Context,
	q queryer,
	orderID string,
) ([]OrderItem, error) {
	rows, err := q.QueryContext(
		ctx,
		`SELECT oi.menu_item_id, mi.name, oi.quantity, oi.unit_price
		 FROM order_items oi
		 JOIN menu_items mi ON mi.id = oi.menu_item_id
		 WHERE oi.order_id = $1
		 ORDER BY oi.id`,
		orderID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OrderItem, 0)

	for rows.Next() {
		var item OrderItem
		if err := rows.Scan(
			&item.MenuItemID, &item.Name, &item.Quantity, &item.UnitPrice,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	return items, rows.Err()
}

// Reconnect to RabbitMQ when unavailable and repeatedly publish pending events.
func (s *Service) runOutboxPublisher() {
	for {
		conn, ch, err := broker.Connect()
		if err != nil {
			log.Printf("RabbitMQ unavailable; retrying outbox publisher: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		if err := ch.Confirm(false); err != nil {
			log.Printf("Could not enable RabbitMQ confirms: %v", err)
			ch.Close()
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		s.publishPending(ch, confirms)

		ch.Close()
		conn.Close()
		time.Sleep(2 * time.Second)
	}
}

func (s *Service) publishPending(
	ch *amqp.Channel,
	confirms <-chan amqp.Confirmation,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, event_type, payload
		 FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT 100`,
	)
	if err != nil {
		log.Printf("Read outbox: %v", err)
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
			log.Printf("Read outbox row: %v", err)
			return
		}
		pending = append(pending, event)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		log.Printf("Read outbox rows: %v", err)
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
				Body:         event.Payload,
			},
		)
		if err != nil {
			log.Printf("Publish event %s: %v", event.ID, err)
			return
		}

		select {
		case confirmation, ok := <-confirms:
			if !ok || !confirmation.Ack {
				log.Printf("RabbitMQ did not confirm event %s", event.ID)
				return
			}
		case <-ctx.Done():
			log.Printf("Timed out waiting for event confirmation %s", event.ID)
			return
		}

		_, err = s.db.ExecContext(
			context.Background(),
			`UPDATE outbox_events
			 SET published_at = NOW()
			 WHERE id = $1 AND published_at IS NULL`,
			event.ID,
		)
		if err != nil {
			log.Printf("Mark event published %s: %v", event.ID, err)
			return
		}

		log.Printf("Published event %s (%s)", event.ID, event.Type)
	}
}

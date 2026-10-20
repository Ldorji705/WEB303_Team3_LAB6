package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"cafe/shared/httpx"

	"github.com/google/uuid"
)

type MenuItem struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

type OrderItem struct {
	MenuItemID string `json:"menu_item_id"`
	Quantity   int    `json:"quantity"`
}

type Order struct {
	ID        string      `json:"id"`
	Items     []OrderItem `json:"items"`
	Total     float64     `json:"total"`
	Status    string      `json:"status"`
	CreatedAt time.Time   `json:"created_at"`
}

var menu = []MenuItem{
	{ID: "latte", Name: "Cafe Latte", Price: 130},
	{ID: "americano", Name: "Americano", Price: 100},
	{ID: "momo", Name: "Veg Momo", Price: 150},
	{ID: "sandwich", Name: "Grilled Sandwich", Price: 180},
}

var (
	mu     sync.RWMutex
	orders = make(map[string]Order)
)

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{
			"service": "order-service",
			"status":  "ok",
		})
	})

	mux.HandleFunc("GET /menu", func(w http.ResponseWriter, r *http.Request) {
		httpx.JSON(w, http.StatusOK, menu)
	})

	mux.HandleFunc("POST /orders", createOrder)
	mux.HandleFunc("GET /orders", listOrders)
	mux.HandleFunc("GET /orders/{id}", getOrder)

	server := &http.Server{
		Addr:              ":8081",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("Order Service listening on http://localhost:8081")
	log.Fatal(server.ListenAndServe())
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Items []OrderItem `json:"items"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		httpx.Error(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if len(request.Items) == 0 {
		httpx.Error(w, http.StatusBadRequest, "Order must contain at least one item")
		return
	}

	var total float64

	for _, item := range request.Items {
		if item.Quantity < 1 {
			httpx.Error(w, http.StatusBadRequest, "Quantity must be at least 1")
			return
		}

		found := false
		for _, menuItem := range menu {
			if menuItem.ID == item.MenuItemID {
				total += menuItem.Price * float64(item.Quantity)
				found = true
				break
			}
		}

		if !found {
			httpx.Error(w, http.StatusBadRequest, "Unknown menu item: "+item.MenuItemID)
			return
		}
	}

	order := Order{
		ID:        uuid.NewString(),
		Items:     request.Items,
		Total:     total,
		Status:    "PLACED",
		CreatedAt: time.Now().UTC(),
	}

	mu.Lock()
	orders[order.ID] = order
	mu.Unlock()

	httpx.JSON(w, http.StatusCreated, order)
}

func listOrders(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	result := make([]Order, 0, len(orders))
	for _, order := range orders {
		result = append(result, order)
	}
	mu.RUnlock()

	httpx.JSON(w, http.StatusOK, result)
}

func getOrder(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		httpx.Error(w, http.StatusBadRequest, "Order ID is required")
		return
	}

	mu.RLock()
	order, found := orders[id]
	mu.RUnlock()

	if !found {
		httpx.Error(w, http.StatusNotFound, "Order not found")
		return
	}

	httpx.JSON(w, http.StatusOK, order)
}

func parseQuantity(value string) int {
	quantity, _ := strconv.Atoi(value)
	return quantity
}

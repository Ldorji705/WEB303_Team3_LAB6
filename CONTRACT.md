# CONTRACT - agreed by all four members
Do not change anything here without telling the whole team.

## Services
| Service | Owner | Port | Database file |
|---|---|---|---|
| Order Service | Lhundup | 3001 | order.db |
| Menu & Stock Service | Ugyen | 3002 | stock.db |
| Kitchen Service | Nyendrak | 3003 | kitchen.db |
| Notification Service | Sonam | 3004 | notification.db |
| React frontend (Vite) | shared | 5173 | none |

No service reads another service's database.

## Event envelope
{ "eventId": "uuid", "eventType": "OrderPlaced", "timestamp": "ISO date",
  "correlationId": "order-12", "data": {} }

## Events
| Event | Published by | Consumed by | data fields |
|---|---|---|---|
| OrderPlaced | Order | Stock, Notification | orderId, studentName, items[{itemId,name,qty}], pickupTime |
| StockReserved | Stock | Kitchen, Order | orderId, studentName, items, pickupTime |
| StockRejected | Stock | Order, Notification | orderId, studentName, reason |
| OrderPreparing | Kitchen | Order | orderId, studentName, items, pickupTime |
| OrderReady | Kitchen | Order, Notification | orderId, studentName, items, pickupTime |

## RabbitMQ
- Exchange cafe.events (topic, durable). Routing key = event name.
- Dead-letter exchange cafe.dlx.
- One durable queue per consumer, named service.event_name, each with a <queue>.dlq:
  stock.order_placed, kitchen.stock_reserved, order.stock_reserved, order.stock_rejected,
  order.order_preparing, order.order_ready, notification.order_placed,
  notification.stock_rejected, notification.order_ready

## REST endpoints
- Order: POST /orders, GET /orders, GET /orders/:id
- Stock: GET /menu, POST /menu/:id/restock
- Kitchen: GET /tickets, POST /tickets/:id/start, POST /tickets/:id/ready
- Notification: GET /notifications
- All services: GET /health

## Order status flow
PLACED -> CONFIRMED -> PREPARING -> READY, or REJECTED. Status never moves backwards.
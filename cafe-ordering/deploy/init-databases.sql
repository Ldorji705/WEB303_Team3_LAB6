
CREATE TABLE IF NOT EXISTS menu_items (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
);

INSERT INTO menu_items (id, name, price)
VALUES
    ('latte', 'Cafe Latte', 130.00),
    ('americano', 'Americano', 100.00),
    ('momo', 'Veg Momo', 150.00),
    ('sandwich', 'Grilled Sandwich', 180.00)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    total NUMERIC(10, 2) NOT NULL CHECK (total >= 0),
    status TEXT NOT NULL DEFAULT 'PLACED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    menu_item_id TEXT NOT NULL REFERENCES menu_items(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events(created_at)
    WHERE published_at IS NULL;



CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    address TEXT,
    email TEXT,
    phone_number TEXT
);

CREATE TABLE IF NOT EXISTS public.suppliers (
    supplier_id INTEGER PRIMARY KEY,
    supplier_name TEXT,
    contact_name TEXT,
    address TEXT,
    phone_number TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    supplier_id INTEGER
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE,
    customer_id INTEGER,
    total_price NUMERIC(12, 2)
);

CREATE TABLE IF NOT EXISTS public.order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price_at_purchase NUMERIC(12, 2)
);

CREATE TABLE IF NOT EXISTS public.payment (
    payment_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    payment_method TEXT,
    amount NUMERIC(12, 2),
    transaction_status TEXT
);

CREATE TABLE IF NOT EXISTS public.shipments (
    shipment_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    shipment_date DATE,
    carrier TEXT,
    tracking_number TEXT,
    delivery_date DATE,
    shipment_status TEXT
);

CREATE TABLE IF NOT EXISTS public.reviews (
    review_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    customer_id INTEGER,
    rating INTEGER,
    review_text TEXT,
    review_date DATE
);

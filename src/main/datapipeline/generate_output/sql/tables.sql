CREATE TABLE fact_sales (
    sales_id BIGINT PRIMARY KEY,
    store_id VARCHAR(255),
    bus_dt DATE,
    subtotal DOUBLE PRECISION,
    discount_total BIGINT,
    check_total DOUBLE PRECISION,
    paid_total DOUBLE PRECISION,
    tax_collected DOUBLE PRECISION,
    guest_count INTEGER,
    table_name VARCHAR(255),
    server_id BIGINT,
    employee_id BIGINT,
    opened_date TIMESTAMP
);

CREATE TABLE dim_date (
    date DATE PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    weekday VARCHAR(50)
);
CREATE TABLE dim_store (
    store_id VARCHAR(255) PRIMARY KEY,
    storeLocation VARCHAR(255) NOT NULL
);
CREATE TABLE dim_menu_item (
    menuItemId BIGINT PRIMARY KEY,
    priceLevel BIGINT,
    activeTaxes VARCHAR(255)
);
CREATE TABLE dim_taxes (
    taxId BIGINT PRIMARY KEY,
    taxAmount DOUBLE PRECISION,
    taxRate BIGINT
);


CREATE SCHEMA IF NOT EXISTS datamarts;

-- Создание сущности datamart

CREATE TABLE IF NOT EXISTS datamarts."transaction"(
    transaction_id VARCHAR(18),
    product_id VARCHAR(12),
    recorded_on TIMESTAMP,
    quantity INT NOT NULL,
    price NUMERIC NOT NULL,
    price_full NUMERIC NOT NULL,
    order_type_id VARCHAR(30) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    pos_name VARCHAR(255) NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    profit NUMERIC,
    update_date TIMESTAMP,
    tech_valid_from TIMESTAMP,
    tech_valid_to TIMESTAMP,
    PRIMARY KEY (transaction_id, product_id, recorded_on),
    FOREIGN KEY (product_id) REFERENCES dds.product (product_id)
);

-- Создание сущности order

CREATE TABLE IF NOT EXISTS datamarts."order"(
    available_on DATE,
    product_name VARCHAR(255) NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    cost_per_item NUMERIC NOT NULL,
    available_quantity NUMERIC NOT NULL,
    pos_name VARCHAR(255) NOT NULL,
    update_date DATE,
    tech_valid_from TIMESTAMP,
    tech_valid_to TIMESTAMP,
    PRIMARY KEY (available_on, product_name, pos_name)
);

-- Создание сущности logs

CREATE TABLE IF NOT EXISTS datamarts.logs(
);

TRUNCATE datamarts."transaction";
TRUNCATE datamarts.logs;
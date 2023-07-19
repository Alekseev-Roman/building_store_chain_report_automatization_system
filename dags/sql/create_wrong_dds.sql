DROP SCHEMA IF EXISTS wrong_data CASCADE;
CREATE SCHEMA wrong_data;

-- Создание последовательности и сущности brand

CREATE TABLE IF NOT EXISTS wrong_data.brand (
  brand_id VARCHAR(30),
  brand VARCHAR(255),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности category

CREATE TABLE IF NOT EXISTS wrong_data.category (
  category_id VARCHAR(8),
  category_name VARCHAR(255),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности stores

CREATE TABLE IF NOT EXISTS wrong_data.stores (
    pos VARCHAR(12),
    pos_name VARCHAR,
    comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности product

CREATE TABLE IF NOT EXISTS wrong_data.product (
  product_id VARCHAR(12),
  name_short VARCHAR(255),
  category_id VARCHAR(8),
  pricing_line_id VARCHAR(12),
  brand_id VARCHAR(30),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности stock

CREATE TABLE IF NOT EXISTS wrong_data.stock (
  available_on VARCHAR(30),
  product_id VARCHAR(12),
  pos VARCHAR(30),
  available_quantity VARCHAR(30),
  cost_per_item VARCHAR(30),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности transaction

CREATE TABLE IF NOT EXISTS wrong_data."transaction" (
  transaction_id VARCHAR(18),
  product_id VARCHAR(12),
  recorded_on VARCHAR(30),
  quantity VARCHAR(30),
  price VARCHAR(30),
  price_full VARCHAR(30),
  order_type_id VARCHAR(30),
  pos VARCHAR(30),
  comment VARCHAR(255) DEFAULT NULL
);






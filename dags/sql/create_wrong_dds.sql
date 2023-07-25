CREATE SCHEMA IF NOT EXISTS wrong_dds;

-- Создание последовательности и сущности brand

CREATE TABLE IF NOT EXISTS wrong_dds.brand (
  brand_id VARCHAR(30),
  brand VARCHAR(255),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности category

CREATE TABLE IF NOT EXISTS wrong_dds.category (
  category_id VARCHAR(8),
  category_name VARCHAR(255),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности stores

CREATE TABLE IF NOT EXISTS wrong_dds.stores (
    pos VARCHAR(12),
    pos_name VARCHAR,
    comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности product

CREATE TABLE IF NOT EXISTS wrong_dds.product (
  product_id VARCHAR(12),
  name_short VARCHAR(255),
  category_id VARCHAR(8),
  pricing_line_id VARCHAR(12),
  brand_id VARCHAR(30),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности stock

CREATE TABLE IF NOT EXISTS wrong_dds.stock (
  available_on VARCHAR(30),
  product_id VARCHAR(12),
  pos VARCHAR(30),
  available_quantity VARCHAR(30),
  cost_per_item VARCHAR(30),
  comment VARCHAR(255) DEFAULT NULL
);

-- Создание сущности transaction

CREATE TABLE IF NOT EXISTS wrong_dds."transaction" (
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

TRUNCATE wrong_dds."transaction" CASCADE;
TRUNCATE wrong_dds.stock CASCADE;
TRUNCATE wrong_dds.product CASCADE;
TRUNCATE wrong_dds.brand CASCADE;
TRUNCATE wrong_dds.category CASCADE;
TRUNCATE wrong_dds.stores CASCADE;




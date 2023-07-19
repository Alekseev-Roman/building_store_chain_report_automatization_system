--DROP SCHEMA IF EXISTS dds CASCADE;

CREATE SCHEMA IF NOT EXISTS dds;

-- Создание и заполнение сущности stores

CREATE TABLE IF NOT EXISTS dds.stores (
    pos VARCHAR(12) PRIMARY KEY NOT NULL,
    pos_name VARCHAR(50)
);

INSERT INTO dds.stores (pos, pos_name)
VALUES ('Магазин 1', 'СтройТорг Первый'),
       ('Магазин 2', 'СтройТорг на Кузнецком'),
       ('Магазин 3', 'СтройТорг Кузьминки'),
       ('Магазин 4', 'СтройТорг Алабино'),
       ('Магазин 5', 'СтройТорг Крюково'),
       ('Магазин 6', 'СтройТорг Михайлвская Слобода'),
       ('Магазин 8', 'СтройТорг Шушары'),
       ('Магазин 9', 'СтройТорг Мурино'),
       ('Магазин 10', 'СтройТорг на Невском')
ON CONFLICT DO NOTHING;

-- Создание последовательности и сущности brand

DROP SEQUENCE IF EXISTS brand_id_seq CASCADE;
CREATE SEQUENCE brand_id_seq START 1000;

CREATE TABLE IF NOT EXISTS dds.brand (
  brand_id INT default nextval('brand_id_seq') PRIMARY KEY NOT NULL,
  brand VARCHAR(255) NOT NULL
);

-- Создание сущности category

CREATE TABLE IF NOT EXISTS dds.category (
  category_id VARCHAR(8) PRIMARY KEY NOT NULL,
  category_name VARCHAR(255) NOT NULL
);

-- Создание сущности product

CREATE TABLE IF NOT EXISTS dds.product (
  product_id VARCHAR(12) PRIMARY KEY NOT NULL,
  name_short VARCHAR(255) NOT NULL,
  category_id VARCHAR(8) NOT NULL,
  pricing_line_id VARCHAR(12),
  brand_id INT NOT NULL,
  FOREIGN KEY (category_id) REFERENCES dds.category (category_id),
  FOREIGN KEY (brand_id) REFERENCES dds.brand (brand_id)
);

-- Создание сущности stock

CREATE TABLE IF NOT EXISTS dds.stock (
  available_on DATE NOT NULL,
  product_id VARCHAR(12) NOT NULL,
  pos VARCHAR(30) NOT NULL,
  available_quantity NUMERIC NOT NULL,
  cost_per_item NUMERIC NOT NULL,
  PRIMARY KEY (available_on, product_id, pos),
  FOREIGN KEY (product_id) REFERENCES dds.product (product_id),
  FOREIGN KEY (pos) REFERENCES dds.stores (pos)
);

-- Создание сущности transaction

CREATE TABLE IF NOT EXISTS dds."transaction" (
  transaction_id VARCHAR(18) NOT NULL,
  product_id VARCHAR(12) NOT NULL,
  recorded_on TIMESTAMP NOT NULL,
  quantity NUMERIC NOT NULL,
  price NUMERIC NOT NULL,
  price_full NUMERIC NOT NULL,
  order_type_id VARCHAR(30) NOT NULL,
  pos VARCHAR(30) NOT NULL,
  PRIMARY KEY (transaction_id, product_id, recorded_on),
  FOREIGN KEY (product_id) REFERENCES dds.product (product_id),
  FOREIGN KEY (pos) REFERENCES dds.stores (pos)
);

DELETE FROM  dds."transaction" CASCADE;
DELETE FROM  dds.stock CASCADE;
DELETE FROM  dds.product CASCADE;
DELETE FROM  dds.brand CASCADE;
DELETE FROM  dds.category CASCADE;


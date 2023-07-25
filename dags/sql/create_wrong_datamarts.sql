CREATE SCHEMA IF NOT EXISTS wrong_datamarts;

-- Создание сущности datamart

CREATE TABLE IF NOT EXISTS wrong_datamarts.datamart(
    transaction_id VARCHAR(255),
    product_id VARCHAR(255),
    recorded_on VARCHAR(255),
    quantity VARCHAR(255),
    price VARCHAR(255),
    price_full VARCHAR(255),
    order_type_id VARCHAR(255),
    product_name VARCHAR(255),
    pos_name VARCHAR(255),
    category_name VARCHAR(255),
    profit VARCHAR(255),
    comment VARCHAR(255),
    UPDATE_DATE VARCHAR(255),
    TECH_VALID_FROM VARCHAR(255),
    TECH_VALID_TO VARCHAR(255)
);


TRUNCATE wrong_datamarts.datamart;



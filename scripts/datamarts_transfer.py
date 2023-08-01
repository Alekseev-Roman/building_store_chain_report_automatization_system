import pandas as pd
import numpy as np
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


#   Обработка данных по заказам
def produce_transactions(dds_postgres_hook, update_data):
    #   Время начала обработки
    start_time = datetime.datetime.now()

    #   Запросы на получение данных
    transaction = dds_postgres_hook.get_pandas_df('''
        SELECT DISTINCT dds."transaction".transaction_id, dds."transaction".product_id, dds."transaction".recorded_on, 
        dds."transaction".quantity, dds."transaction".price, dds."transaction".price_full, dds.brand.brand,
        dds."transaction".order_type_id, CONCAT(dds.product.name_short, ' ', dds.brand.brand) AS product_name, 
        dds.stores.pos_name, dds.category.category_name, price - cost_per_item AS profit
        FROM dds."transaction"
            LEFT JOIN dds.stores ON dds."transaction".pos = dds.stores.pos
            LEFT JOIN dds.product ON dds."transaction".product_id = dds.product.product_id
            LEFT JOIN dds.brand ON dds.product.brand_id = dds.brand.brand_id
            LEFT JOIN dds.category ON dds.category.category_id = dds.product.category_id
            LEFT JOIN (
                SELECT dds.stock.product_id, dds.stock.cost_per_item
                FROM dds.stock
                GROUP BY product_id, cost_per_item
            ) AS stock ON stock.product_id = dds."transaction".product_id
        WHERE dds."transaction".order_type_id = 'BUY'
        ''')

    transaction = transaction.drop_duplicates(subset=['transaction_id', 'product_id', 'recorded_on'])

    wrong_datamart = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_datamarts.datamart')

    transaction = transaction.assign(
        update_date=update_data, tech_valid_from=start_time, tech_valid_to=datetime.datetime.now()
    )

    return transaction[['transaction_id', 'product_id', 'recorded_on', 'quantity', 'price', 'price_full', 'brand',
                        'order_type_id', 'product_name', 'pos_name', 'category_name', 'profit',
                        'update_date', 'tech_valid_from', 'tech_valid_to']], \
        wrong_datamart[['transaction_id', 'product_id', 'recorded_on', 'quantity', 'price', 'price_full',
                        'order_type_id', 'product_name', 'pos_name', 'category_name', 'profit', 'comment',
                        'update_date', 'tech_valid_from', 'tech_valid_to']]


#   Обработка данных для закупок
def produce_orders(dds_postgres_hook, update_data):
    #   Запросы на получение данных
    stores = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.stores').set_index('pos')
    brand = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.brand').set_index('brand_id')
    category = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.category').set_index('category_id')
    product = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.product')
    stock = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.stock')

    order = dds_postgres_hook.get_pandas_df('SELECT * FROM datamarts."order"')

    #   Время начала обработки
    start_time = datetime.datetime.now()

    #   Удаление слишком старых записей
    order = order[abs((order['update_date'].astype('datetime64') - pd.to_datetime('today').normalize()) / np.timedelta64(1, 'Y')) < 1]

    product = product.join(brand, on='brand_id', how='left')
    product = product.join(category, on='category_id', how='left').set_index('product_id')
    product = product.assign(product_name=product['name_short'] + ' ' + product['brand'])
    stock = stock.join(product, on='product_id', how='left')
    stock = stock.join(stores, on='pos', how='left')
    stock = stock.assign(
        update_date=update_data, tech_valid_from=start_time, tech_valid_to=datetime.datetime.now()
    )

    #   Объединение предыдущих записей и новых
    order = pd.concat([
        order,
        stock[['available_on', 'product_name', 'category_name', 'cost_per_item', 'available_quantity', 'pos_name',
               'update_date', 'tech_valid_from', 'tech_valid_to']]
    ])

    return order


#   Выгрузка данных для обработки
def datamart_transfer():
    update_data = datetime.datetime.now()
    dds_postgres_hook = PostgresHook(postgres_conn_id='internship_3_db')
    dds_engine = dds_postgres_hook.get_sqlalchemy_engine()

    #   Запросы на получение данных
    transaction_datamart, wrong_datamart = produce_transactions(dds_postgres_hook, update_data)
    order_datamart = produce_orders(dds_postgres_hook, update_data)

    #   Сохранение данных в dds
    transaction_datamart.to_sql('transaction', dds_engine, if_exists='append', schema='datamarts', index=False)
    wrong_datamart.to_sql('datamart', dds_engine, if_exists='append', schema='wrong_datamarts', index=False)
    order_datamart.to_sql('order', dds_engine, if_exists='replace', schema='datamarts', index=False)



if __name__ == '__main__':
    datamart_transfer()

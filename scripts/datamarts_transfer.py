import pandas as pd
import numpy as np
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


#   Обработка данных по заказам
def produce_transactions(dds_postgres_hook, update_data):
    #   Запросы на получение данных
    stores = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.stores').set_index('pos')
    transaction = dds_postgres_hook.get_pandas_df('SELECT * FROM dds."transaction"')
    brand = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.brand').set_index('brand_id')
    category = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.category').set_index('category_id')
    product = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.product')
    stock = dds_postgres_hook.get_pandas_df('SELECT product_id, cost_per_item FROM dds.stock')
    wrong_datamart = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_datamarts.datamart')

    #   Время начала обработки
    start_time = datetime.datetime.now()

    transaction = transaction.join(stores, on='pos', how='left')
    product = product.join(category, on='category_id', how='left').set_index('product_id')
    stock = stock.drop_duplicates(subset='product_id')
    stock = stock[stock['cost_per_item'].notnull()]
    stock = stock.set_index('product_id')
    product = product.join(stock, on='product_id', how='left')
    transaction = transaction.join(product, on='product_id', how='left')
    transaction = transaction.join(brand, on='brand_id', how='left')
    transaction = transaction.assign(product_name=transaction['name_short'] + ' ' + transaction['brand'])
    transaction = transaction.assign(profit=transaction['price'] - transaction['cost_per_item'])
    transaction = transaction.assign(
        update_date=update_data, tech_valid_from=start_time, tech_valid_to=datetime.datetime.now()
    )

    transaction = transaction[transaction['order_type_id'] == 'BUY']

    # Удаление записей, которые не содержат cost_per_item
    buf_transaction = transaction[transaction['profit'].notnull()]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_datamart = pd.concat([wrong_datamart, buf_wrong])
    wrong_datamart.loc[wrong_datamart['comment'].isnull(), 'comment'] = 'Отсутствует cost_per_item'
    transaction = buf_transaction

    return transaction[['transaction_id', 'product_id', 'recorded_on', 'quantity', 'price', 'price_full',
                        'order_type_id', 'product_name', 'pos_name', 'category_name', 'profit', 'update_date',
                        'tech_valid_from', 'tech_valid_to']], \
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

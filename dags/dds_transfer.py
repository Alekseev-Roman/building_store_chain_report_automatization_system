import pandas as pd
import numpy as np
import datetime
from airflow.hooks.postgres_hook import PostgresHook


#   Коррекция brand
def correct_brand(sources_postgres_hook, dds_postgres_hook):
    brand = sources_postgres_hook.get_pandas_df('SELECT * FROM sources.brand')
    wrong_brand = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_data.brand')

    #   Отрицательное или нечисловое значение в brand_id
    buf_brand = brand[brand['brand_id'].str.isdigit()]
    buf_wrong = pd.concat([brand, buf_brand]).drop_duplicates(keep=False)
    wrong_brand = pd.concat([wrong_brand, buf_wrong])
    wrong_brand.loc[wrong_brand['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в brand_id'
    brand = buf_brand

    #   Пустое значение brand
    buf_brand = brand[brand['brand'] != '']
    buf_wrong = pd.concat([brand, buf_brand]).drop_duplicates(keep=False)
    wrong_brand = pd.concat([wrong_brand, buf_wrong])
    wrong_brand.loc[wrong_brand['comment'].isnull(), 'comment'] = \
        'Пустое значение brand'
    brand = buf_brand

    #   Дубикат пары (brand_id, brand)
    buf_brand = brand.drop_duplicates(subset=['brand_id', 'brand'])
    buf_wrong = pd.concat([brand, buf_brand]).drop_duplicates(keep=False)
    wrong_brand = pd.concat([wrong_brand, buf_wrong])
    wrong_brand.loc[wrong_brand['comment'].isnull(), 'comment'] = \
        'Дубикат пары (brand_id, brand)'
    brand = buf_brand

    #   Дубикат brand_id
    buf_brand = brand.drop_duplicates(subset='brand_id')
    buf_wrong = pd.concat([brand, buf_brand]).drop_duplicates(keep=False)
    wrong_brand = pd.concat([wrong_brand, buf_wrong])
    wrong_brand.loc[wrong_brand['comment'].isnull(), 'comment'] = \
        'Дубикат brand_id'
    brand = buf_brand

    #   Дубикат brand
    buf_brand = brand.drop_duplicates(subset='brand')
    buf_wrong = pd.concat([brand, buf_brand]).drop_duplicates(keep=False)
    wrong_brand = pd.concat([wrong_brand, buf_wrong])
    wrong_brand.loc[wrong_brand['comment'].isnull(), 'comment'] = \
        'Дубикат brand'
    brand = buf_brand

    return brand, wrong_brand


#   Коррекция category
def correct_category(sources_postgres_hook, dds_postgres_hook):
    category = sources_postgres_hook.get_pandas_df('SELECT * FROM sources.category')
    wrong_category = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_data.category')

    #   Пустое значение category_id
    buf_category = category[category['category_id'] != '']
    buf_wrong = pd.concat([category, buf_category]).drop_duplicates(keep=False)
    wrong_category = pd.concat([wrong_category, buf_wrong])
    wrong_category.loc[wrong_category['comment'].isnull(), 'comment'] = \
        'Пустое значение category_id'
    category = buf_category

    #   Пустое значение category_name
    buf_category = category[category['category_name'] != '']
    buf_wrong = pd.concat([category, buf_category]).drop_duplicates(keep=False)
    wrong_category = pd.concat([wrong_category, buf_wrong])
    wrong_category.loc[wrong_category['comment'].isnull(), 'comment'] = \
        'Пустое значение category_name'
    category = buf_category

    #   Дубликат пары (category_id, category_name)
    buf_category = category.drop_duplicates(subset=['category_id', 'category_name'])
    buf_wrong = pd.concat([category, buf_category]).drop_duplicates(keep=False)
    wrong_category = pd.concat([wrong_category, buf_wrong])
    wrong_category.loc[wrong_category['comment'].isnull(), 'comment'] = \
        'Дубликат пары (category_id, category_name)'
    category = buf_category

    #   Дубликат category_id
    buf_category = category.drop_duplicates(subset='category_id')
    buf_wrong = pd.concat([category, buf_category]).drop_duplicates(keep=False)
    wrong_category = pd.concat([wrong_category, buf_wrong])
    wrong_category.loc[wrong_category['comment'].isnull(), 'comment'] = \
        'Дубликат пары category_id'
    category = buf_category

    #   Дубликат category_name
    buf_category = category.drop_duplicates(subset='category_name')
    buf_wrong = pd.concat([category, buf_category]).drop_duplicates(keep=False)
    wrong_category = pd.concat([wrong_category, buf_wrong])
    wrong_category.loc[wrong_category['comment'].isnull(), 'comment'] = \
        'Дубликат пары category_name'
    category = buf_category

    return category, wrong_category


#   Коррекция product
def correct_product(sources_postgres_hook, dds_postgres_hook, brand, category):
    product = sources_postgres_hook.get_pandas_df('SELECT * FROM sources.product')
    wrong_product = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_data.product')

    #   Отрицательное или нечисловое значение в brand_id
    buf_product = product[product['brand_id'].str.isdigit()]
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в brand_id'
    product = buf_product

    #   Пустое значение product_id
    buf_product = product[product['product_id'] != '']
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Пустое значение product_id'
    product = buf_product

    #   Пустое значение category_id
    buf_product = product[product['category_id'] != '']
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Пустое значение category_id'
    product = buf_product

    #   Пустое значение name_short
    buf_product = product[product['name_short'] != '']
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Пустое значение name_short'
    product = buf_product

    #   Пустое значение pricing_line_id
    buf_product = product[product['pricing_line_id'] != '']
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Пустое значение pricing_line_id'
    product = buf_product

    #   Некорректное name_short
    buf_wrong = product[product['name_short'].str.isdigit()]
    print(buf_wrong)
    buf_product = pd.concat([product, buf_wrong]).drop_duplicates(keep=False)
    print(buf_product)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Некорректное name_short'
    product = buf_product

    #   Дубликаты первичного ключа (product_id)
    buf_product = product.drop_duplicates(subset='product_id')
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Дубликаты первичного ключа (product_id)'
    product = buf_product

    #   Дубликаты name_short
    buf_product = product.drop_duplicates(subset='name_short')
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Дубликаты name_short'
    product = buf_product

    #   Значение category_id отсутствует в таблице category
    buf_product = product[product['category_id'].isin(category['category_id'])]
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Значение category_id отсутствует в таблице category'
    product = buf_product

    #   Значение brand_id отсутствует в таблице brand
    buf_product = product[product['brand_id'].isin(brand['brand_id'])]
    buf_wrong = pd.concat([product, buf_product]).drop_duplicates(keep=False)
    wrong_product = pd.concat([wrong_product, buf_wrong])
    wrong_product.loc[wrong_product['comment'].isnull(), 'comment'] = \
        'Значение brand_id отсутствует в таблице brand'
    product = buf_product

    return product, wrong_product


#   Коррекция stock
def correct_stock(sources_postgres_hook, dds_postgres_hook, product, stores):
    stock = sources_postgres_hook.get_pandas_df('SELECT * FROM sources.stock')
    wrong_stock = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_data.stock')

    #   Отрицательное или нечисловое значение в available_quantity
    buf_stock = stock[stock['available_quantity'].str.replace('.', '', 1).str.isdigit()]
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в available_quantity'
    stock = buf_stock

    #   Отрицательное или нечисловое значение в cost_per_item
    buf_stock = stock[stock['cost_per_item'].str.replace('.', '', 1).str.isdigit()]
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в cost_per_item'
    stock = buf_stock

    #   Ненатуральное значение в product_id
    buf_stock = stock[stock['product_id'].str.isdigit()]
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Ненатуральное значение в product_id'
    stock = buf_stock

    #   Пустое значение pos
    buf_stock = stock[stock['pos'] != '']
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Пустое значение pos'
    stock = buf_stock

    #   Пустое значение в available_on
    buf_stock = stock[stock['available_on'] != '']
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Пустое значение available_on'
    stock = buf_stock

    #   Дубликаты первичного ключа (available_on, product_id, pos)
    buf_stock = stock.drop_duplicates(subset=['available_on', 'product_id', 'pos'])
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Дубликаты первичного ключа (available_on, product_id, pos)'
    stock = buf_stock

    #   Значение product_id отсутствует в таблице product
    buf_stock = stock[stock['product_id'].isin(product['product_id'])]
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Значение product_id отсутствует в таблице product'
    stock = buf_stock

    #   Значение pos отсутствует в таблице stores
    buf_stock = stock[stock['pos'].isin(stores['pos'])]
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Значение pos отсутствует в таблице stores'
    stock = buf_stock

    #   Значение в available_on не является датой
    stock['available_on'] = \
        pd.TimedeltaIndex(stock['available_on'].astype(int), unit='d') + datetime.datetime(1899, 12, 30)
    buf_stock = stock[pd.notnull(stock['available_on'])]
    buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    wrong_stock = pd.concat([wrong_stock, buf_wrong])
    wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
        'Значение в available_on не является датой'
    stock = buf_stock

    # #   Значение available_on старше двух лет
    # buf_stock = stock[abs((stock['available_on'] - pd.to_datetime('today').normalize())
    #                                   / np.timedelta64(1, 'Y')) < 2]
    # buf_wrong = pd.concat([stock, buf_stock]).drop_duplicates(keep=False)
    # wrong_stock = pd.concat([wrong_stock, buf_wrong])
    # wrong_stock.loc[wrong_stock['comment'].isnull(), 'comment'] = \
    #     'Значение available_on старше двух лет'
    # stock = buf_stock

    return stock, wrong_stock


#   Коррекция transaction
def correct_transaction(dds_postgres_hook, transaction, product, stores):
    wrong_transaction = dds_postgres_hook.get_pandas_df('SELECT * FROM wrong_data."transaction"')

    #   Отрицательное или нечисловое значение в quantity
    buf_transaction = transaction[transaction['quantity'].str.replace('.', '', 1).str.isdigit()]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в quantity'
    transaction = buf_transaction

    #   Отрицательное или нечисловое значение в price
    buf_transaction = transaction[transaction['price'].str.replace('.', '', 1).str.isdigit()]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в price'
    transaction = buf_transaction

    #   Отрицательное или нечисловое значение в price_full
    buf_transaction = transaction[transaction['price_full'].str.replace('.', '', 1).str.isdigit()]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Отрицательное или нечисловое значение в price_full'
    transaction = buf_transaction

    #   Пустое значение в transaction_id
    buf_transaction = transaction[transaction['transaction_id'] != '']
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Пустое значение в transaction_id'
    transaction = buf_transaction

    #   Пустое значение в order_type_id
    buf_transaction = transaction[transaction['product_id'] != '']
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Пустое значение в order_type_id'
    transaction = buf_transaction

    #   Пустое значение в order_type_id
    buf_transaction = transaction[transaction['order_type_id'] != '']
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Пустое значение в order_type_id'
    transaction = buf_transaction

    #   Дубликаты первичного ключа (available_on, product_id, pos)
    buf_transaction = transaction.drop_duplicates(subset=['transaction_id', 'product_id', 'recorded_on'])
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Дубликаты первичного ключа (transaction_id, product_id, recorded_on)'
    transaction = buf_transaction

    #   Значение product_id отсутствует в таблице product
    buf_transaction = transaction[transaction['product_id'].isin(product['product_id'])]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Значение product_id отсутствует в таблице product'
    transaction = buf_transaction

    #   Значение pos отсутствует в таблице stores
    buf_transaction = transaction[transaction['pos'].isin(stores['pos'])]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Значение pos отсутствует в таблице stores'
    transaction = buf_transaction

    #   Значение recorded_on не является датой
    transaction['recorded_on'] = pd.to_datetime(transaction['recorded_on'], errors='coerce')
    buf_transaction = transaction[pd.notnull(transaction['recorded_on'])]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Значение в recorded_on не является датой'
    transaction = buf_transaction

    #   Значение recorded_on старше двух лет
    buf_transaction = transaction[abs((transaction['recorded_on'] - pd.to_datetime('today').normalize())
                                  / np.timedelta64(1, 'Y')) < 2]
    buf_wrong = pd.concat([transaction, buf_transaction]).drop_duplicates(keep=False)
    wrong_transaction = pd.concat([wrong_transaction, buf_wrong])
    wrong_transaction.loc[wrong_transaction['comment'].isnull(), 'comment'] = \
        'Значение recorded_on старше двух лет'
    transaction = buf_transaction

    return transaction, wrong_transaction


#   Выгрузка данных для обработки
def dds_transfer():
    sources_postgres_hook = PostgresHook(postgres_conn_id='internship_sources')
    dds_postgres_hook = PostgresHook(postgres_conn_id='internship_3_db')
    dds_engine = dds_postgres_hook.get_sqlalchemy_engine()

    #   Загрузка данных из файлов
    transaction_stores = pd.read_csv(
        'dags/csv/transactions-stores.csv', sep=';', encoding='CP1251'
    ).set_index('transaction_id')

    #   Запросы на получение данных
    stores = dds_postgres_hook.get_pandas_df('SELECT * FROM dds.stores')
    transaction = sources_postgres_hook.get_pandas_df('SELECT * FROM sources."transaction"')

    transaction = transaction.join(transaction_stores, how='left', on='transaction_id')

    #   Получение обработанных данных
    brand, wrong_brand = correct_brand(sources_postgres_hook, dds_postgres_hook)
    category, wrong_category = correct_category(sources_postgres_hook, dds_postgres_hook)
    product, wrong_product = correct_product(sources_postgres_hook, dds_postgres_hook, brand, category)
    stock, wrong_stock = correct_stock(sources_postgres_hook, dds_postgres_hook, product, stores)
    transaction, wrong_transaction = correct_transaction(dds_postgres_hook, transaction, product, stores)

    #   Сохранение данных в dds
    brand.to_sql('brand', dds_engine, if_exists='append', schema='dds', index=False)
    category.to_sql('category', dds_engine, if_exists='append', schema='dds', index=False)
    product.to_sql('product', dds_engine, if_exists='append', schema='dds', index=False)
    stock.to_sql('stock', dds_engine, if_exists='append', schema='dds', index=False)
    transaction.to_sql('transaction', dds_engine, if_exists='append', schema='dds', index=False)

    #   Сохранение некорректных данных
    wrong_brand.to_sql('brand', dds_engine, if_exists='append', schema='wrong_data', index=False)
    wrong_category.to_sql('category', dds_engine, if_exists='append', schema='wrong_data', index=False)
    wrong_product.to_sql('product', dds_engine, if_exists='append', schema='wrong_data', index=False)
    wrong_stock.to_sql('stock', dds_engine, if_exists='append', schema='wrong_data', index=False)
    wrong_transaction.to_sql('transaction', dds_engine, if_exists='append', schema='wrong_data', index=False)


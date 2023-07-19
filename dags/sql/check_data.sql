-- Подсчёт общего количества строк в таблице "brand":

SELECT COUNT(*) AS total_rows
FROM sources.brand;

-- Проверка на соответствие значений в колонке brand_id (должны быть только числовые значения) в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand_id !~ '^[0-9]+$';

-- Вывод brand_id, которые имеют дубликаты в таблице "brand":

SELECT brand_id
FROM sources.brand
GROUP BY brand_id
HAVING COUNT(*) > 1;

-- Поиск пропущенных значений в столбце brand_id в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand_id IS NULL
  OR brand_id = '';

-- Поиск пропущенных значений в столбце brand в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand IS NULL
  OR brand = '';

-- Идентифицикация brand_id, для которых имеется более одного уникального значения в столбце brand в таблице "brand":

SELECT brand_id,
       COUNT(DISTINCT brand) AS COUNT
FROM sources.brand
GROUP BY brand_id
HAVING COUNT(DISTINCT brand) > 1;

-- Идентифицикация brand, для которых имеется более одного уникального значения в столбце brand_id в таблице "brand":

SELECT brand,
       COUNT(DISTINCT brand_id) AS COUNT
FROM sources.brand
GROUP BY brand
HAVING COUNT(DISTINCT brand_id) > 1;

-- Вывод записей, в которых столбец brand содержит символы, отличные от букв (латинских или русских), цифр, точки, запятой и дополнительных символов в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand ~ '[^a-zA-Z0-9 .,а-яА-ЯёЁ\-+%!&()''`\"]';

-- Вывод записей, где поле brand начинается со строчной буквы в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand SIMILAR TO '[a-zа-я]%';

-- Вывод записей, где поле brand начинается пробела в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand LIKE ' %';

-- Вывод записей, где поле brand_id начинается пробела в таблице "brand":

SELECT *
FROM sources.brand
WHERE brand_id LIKE ' %';

-- Подсчёт общего количества строк в таблице "category":

SELECT COUNT(*) AS total_rows
FROM sources.category;

-- Вывод записей из таблицы "category", которые не соответствуют условию в столбце category_id в таблице "category":

SELECT *
FROM sources.category
WHERE category_id !~ '^[A-Z0-9А-Я]{1,8}$';

-- Вывод записей из таблицы "category", которые не соответствуют условию в столбце category_name в таблице "category":

SELECT *
FROM sources.category
WHERE category_name ~ '^\d+$';

-- Вывод category_id, которые имеют дубликаты в таблице "category":

SELECT category_id
FROM sources.category
GROUP BY category_id
HAVING COUNT(*) > 1;

-- Поиск пропущенных значений в столбце category_id в таблице "category":

SELECT *
FROM sources.category
WHERE category_id IS NULL
  OR category_id = '';

-- Поиск пропущенных значений в столбце category_name в таблице "category":

SELECT *
FROM sources.category
WHERE category_name IS NULL
  OR category_name = '';

-- Идентифицикация category_id, для которых имеется более одного уникального значения в столбце category_name в таблице "category":

SELECT category_id,
       COUNT(DISTINCT category_name) AS COUNT
FROM sources.category
GROUP BY category_id
HAVING COUNT(DISTINCT category_name) > 1;

-- Идентифицикация category_name, для которых имеется более одного уникального значения в столбце category_id в таблице "category":

SELECT category_name,
       COUNT(DISTINCT category_id) AS COUNT
FROM sources.category
GROUP BY category_name
HAVING COUNT(DISTINCT category_id) > 1;

-- Индетификация записей, которые используют специальные символы ("?", "!") в столбце category_name в таблице "category":

SELECT *
FROM sources.category
WHERE category_name LIKE '%!%'
  OR category_name LIKE '%?%';

-- Вывод записей, в которых category_name написано на иностранном языке в таблице "category":

SELECT *
FROM sources.category
WHERE category_name ~ '[А-ЯA-Z]{2,}';

-- Индетификация записей, которые используют аббвеатуры или две и более заглавные буквы в столбце category_name в таблице "category":

SELECT *
FROM sources.category
WHERE category_name ~ '[А-Я]{2,}';

-- Индетификация записей, в которых сразу после буквы идёт цифра в столбце category_name в таблице "category":

SELECT *
FROM sources.category
WHERE category_name ~ '[а-яА-Яa-zA-Z][0-9]';

-- Подсчёт общего количества строк в таблице "product":

SELECT COUNT(*) AS total_rows
FROM sources.product;

-- Вывод product_id, которые имеют дубликаты в таблице "product":

SELECT product_id
FROM sources.product
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Поиск пропущенных значений в столбце product_id в таблице "product":

SELECT *
FROM sources.product
WHERE product_id IS NULL
  OR product_id = '';

-- Поиск пропущенных значений в столбце name_short в таблице "product":

SELECT *
FROM sources.product
WHERE name_short IS NULL
  OR name_short = '';

-- Поиск пропущенных значений в столбце category_id в таблице "product":

SELECT *
FROM sources.product
WHERE category_id IS NULL
  OR category_id = '';

-- Поиск пропущенных значений в столбце pricing_line_id в таблице "product":

SELECT *
FROM sources.product
WHERE pricing_line_id IS NULL
  OR pricing_line_id = '';

-- Поиск пропущенных значений в столбце brand_id в таблице "product":

SELECT *
FROM sources.product
WHERE brand_id IS NULL
  OR brand_id = '';

-- Вывод численных значений в столбце name_short, содержащим строковые данные в таблице "product":

SELECT name_short
FROM sources.product
WHERE name_short ~ '^\d+$';

-- Вывод строк, которые ссылаются на несуществующий "brand_id" в таблице "product":

SELECT p.*
FROM sources.product p
LEFT JOIN sources.brand b ON p.brand_id = b.brand_id
WHERE b.brand_id IS NULL;

-- Вывод строк, которые ссылаются на несуществующий "category_id" в таблице "product":

SELECT p.*
FROM sources.product p
LEFT JOIN sources.category c ON p.category_id = c.category_id
WHERE c.category_id IS NULL;

-- Вывод уникальных name_short, у которых 2 и более product_id в таблице "product":

SELECT name_short
FROM sources.product
GROUP BY name_short
HAVING COUNT(DISTINCT product_id) > 1;

-- Вывод уникальных name_short, у которых 2 и более category_id в таблице "product":

SELECT name_short
FROM sources.product
GROUP BY name_short
HAVING COUNT(DISTINCT category_id) > 1;

-- Вывод уникальных name_short, у которых 2 и более brand_id в таблице "product":

SELECT name_short
FROM sources.product
GROUP BY name_short
HAVING COUNT(DISTINCT brand_id) > 1;

-- Вывод уникальных product_id, у которых 2 и более category_id в таблице "product":

SELECT product_id
FROM sources.product
GROUP BY product_id
HAVING COUNT(DISTINCT category_id) > 1;

-- Вывод уникальных product_id, у которых 2 и более brand_id в таблице "product":

SELECT product_id
FROM sources.product
GROUP BY product_id
HAVING COUNT(DISTINCT brand_id) > 1;

-- Проверка столбца product_id на наличие недопустимых символов в таблице "product":

SELECT *
FROM sources.product
WHERE product_id ~ '[^\w\d\s]';

-- Проверка столбца pricing_line_id на наличие недопустимых символов в таблице "product":

SELECT *
FROM sources.product
WHERE pricing_line_id ~ '[^\w\d\s]';

-- Проверка столбца brand_id на наличие недопустимых символов в таблице "product":

SELECT *
FROM sources.product
WHERE brand_id ~ '[^\w\d\s]';

-- Вывод записей, которые в столбце name_short начинаются со строчной буквы в таблице "product":

SELECT *
FROM sources.product
WHERE name_short ~ '^[а-я]';

-- Вывод записей, которые в столбце name_short начинаются с пробела в таблице "product":

SELECT *
FROM sources.product
WHERE name_short LIKE ' %';

-- Подсчёт общего количества строк в таблице "stock":

SELECT COUNT(*) AS total_rows
FROM sources.stock;

-- Вывод available_on, product_id и pos которые имеют дубликаты в таблице "stock":

SELECT available_on,
       product_id,
       pos
FROM sources.stock
GROUP BY available_on,
         product_id,
         pos
HAVING COUNT(*) > 1;

-- Поиск пропущенных значений в столбце available_on в таблице "stock":

SELECT *
FROM sources.stock
WHERE available_on = ''
  OR available_on IS NULL;

-- Поиск пропущенных значений в столбце product_id в таблице "stock":

SELECT *
FROM sources.stock
WHERE product_id = ''
  OR product_id IS NULL;

-- Поиск пропущенных значений в столбце pos в таблице "stock":

SELECT *
FROM sources.stock
WHERE pos = ''
  OR pos IS NULL;

-- Поиск пропущенных значений в столбце available_quantity в таблице "stock":

SELECT *
FROM sources.stock
WHERE available_quantity = ''
  OR available_quantity IS NULL;

-- Поиск пропущенных значений в столбце cost_per_item в таблице "stock":

SELECT *
FROM sources.stock
WHERE cost_per_item = ''
  OR cost_per_item IS NULL;

-- Вывод product_id, которые ссылаются на несуществующий "product_id" в таблице "stock":

SELECT DISTINCT s.product_id
FROM sources.stock s
LEFT JOIN sources.product p ON s.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Вывод строк, в которых available_on имеет некорректный формат в таблице "stock":

SELECT *
FROM sources.stock
WHERE NOT (available_on ~ '^(\d{2})\.(\d{2})\.(\d{4})');

-- Вывод строк, в которых available_on больше сегодняшней даты и первышает давность более чем на 2 года:

SELECT *
FROM sources.stock
WHERE available_on > CURRENT_DATE
  AND available_on < CURRENT_DATE - INTERVAL '2 years';

-- Вывод записей, которые в столбце «pos» не начинаются со слова «Магазин» в таблице "stock":

SELECT *
FROM sources.stock
WHERE pos !~ 'Магазин';

-- Проверка наличия отрицательной себестоимости единицы товара (cost_per_item) в таблице "stock":

SELECT *
FROM sources.stock
WHERE NULLIF(cost_per_item, '')::numeric < 0;

-- Проверка отсутствия пробела в начале строки атрибута "available_on" в таблице "stock":

SELECT *
FROM sources.stock
WHERE (available_on <> ''
       AND available_on LIKE ' %')-- Проверка отсутствия пробела в начале строки атрибута "product_id" в таблице "stock":

  SELECT *
  FROM sources.stock WHERE (product_id <> ''
                            AND product_id LIKE ' %')-- Проверка отсутствия пробела в начале строки атрибута "pos" в таблице "stock":

  SELECT *
  FROM sources.stock WHERE (pos <> ''
                            AND pos LIKE ' %')-- Проверка отсутствия пробела в начале строки атрибута  "available_quantity" в таблице "stock":

  SELECT *
  FROM sources.stock WHERE (available_quantity::text <> ''
                            AND available_quantity::text LIKE ' %')-- Проверка отсутствия пробела в начале строки атрибута "cost_per_item" в таблице "stock":

  SELECT *
  FROM sources.stock WHERE (cost_per_item::text <> ''
                            AND cost_per_item::text LIKE ' %')-- Подсчёт общего количества строк в таблице "transaction":

  SELECT COUNT(*) AS total_rows
  FROM sources.transaction;

-- Вывод transaction_id, product_id и recorded_on, которые имеют дубликаты в таблице "transaction":

SELECT transaction_id,
       product_id,
       recorded_on
FROM sources.transaction
GROUP BY transaction_id,
         product_id,
         recorded_on
HAVING COUNT(*) > 1;

-- Поиск пропущенных значений в столбце transaction_id в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE transaction_id = ''
  OR transaction_id IS NULL;

-- Поиск пропущенных значений в столбце product_id в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE product_id = ''
  OR product_id IS NULL;

-- Поиск пропущенных значений в столбце recorded_on в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE recorded_on = ''
  OR recorded_on IS NULL;

-- Поиск пропущенных значений в столбце quantity в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE quantity = ''
  OR quantity IS NULL;

-- Поиск пропущенных значений в столбце price в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price = ''
  OR price IS NULL;

-- Поиск пропущенных значений в столбце price_full в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price_full = ''
  OR price_full IS NULL;

-- Поиск пропущенных значений в столбце order_type_id в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE order_type_id = ''
  OR order_type_id IS NULL;

-- Вывод product_id, которые ссылаются на несуществующий "product_id" в таблице "transaction":

SELECT t.product_id
FROM sources.transaction t
LEFT JOIN sources.product p ON t.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Вывод записей, у которых дата в столбце recorded_on не соответствует формату в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE NOT (recorded_on ~ '^((0[1-9]|1[0-9]|2[0-9]|3[01])\.(0[1-9]|1[0-2])\.(20[2-9][0-9]|2[3-9][0-9]{2}|[3-9][0-9]{3}) (0[0-9]|1[0-9]|2[0-3]):[0-5][0-9])$'
           OR recorded_on ~ '^((0[1-9]|1[0-9]|2[0-9]|3[01])\.(0[1-9]|1[0-2])\.(20[2-9][0-9]|2[3-9][0-9]{2}|[3-9][0-9]{3}) ([0-9]|1[0-9]|2[0-3]):[0-5][0-9])$')
  OR recorded_on IS NULL;

--  Вывод всехе записей, у которых значение столбца recorded_on находится в будущем относительно текущей даты или более чем 2 года назад от текущей даты в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE TO_TIMESTAMP(recorded_on, 'DD.MM.YYYY HH24:MI') > CURRENT_DATE
  OR TO_TIMESTAMP(recorded_on, 'DD.MM.YYYY HH24:MI') < CURRENT_DATE - INTERVAL '2 years';

-- Вывод записей, у которых цена товара при продаже была больше, чем полная стоимость товара в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price_full <> ''
  AND price <> ''
  AND price_full::numeric < price::numeric;

-- Проверка на отрицательную стоимость товара при продаже в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price <> ''
  AND price::numeric < 0;

-- Проверка на отрицательную полную стоимость товара в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price_full < 0;

-- Проверка на одинаковые идентификаторы транзакции с разными значениями статуса транзакции в таблице "transaction":

SELECT transaction_id,
       recorded_on,
       COUNT(DISTINCT order_type_id) AS count_order_type
FROM sources.transaction
GROUP BY transaction_id,
         recorded_on
HAVING COUNT(DISTINCT order_type_id) > 1;

-- Проверка на количество товара, равное нулю или отрицательное в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE quantity::numeric <= 0
  AND quantity !='';

-- Вывод записей, у которых «order_type_id» не принял статус «BUY» в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE order_type_id <> 'BUY';

-- Проверка отсутствия пробела в начале строки атрибута "transaction_id" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE transaction_id <> ''
  AND transaction_id LIKE ' %';

-- Проверка отсутствия пробела в начале строки атрибута "product_id" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE product_id::text <> ''
  AND product_id::text LIKE ' %';

-- Проверка отсутствия пробела в начале строки атрибута "recorded_on" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE recorded_on::text <> ''
  AND recorded_on::text LIKE ' %';

-- Проверка отсутствия пробела в начале строки атрибута "quantity" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE quantity::text <> ''
  AND quantity::text LIKE ' %';

-- Проверка отсутствия пробела в начале строки атрибута "price" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price::text <> ''
  AND price::text LIKE ' %';

-- Проверка отсутствия пробела в начале строки атрибута "price_full" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE price_full::text <> ''
  AND price_full::text LIKE ' %';

-- Проверка отсутствия пробела в начале строки атрибута "order_type_id" в таблице "transaction":

SELECT *
FROM sources.transaction
WHERE order_type_id <> ''
  AND order_type_id LIKE ' %';
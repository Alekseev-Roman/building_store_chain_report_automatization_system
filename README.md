# ДАГи для переноса и обработки информации
Разработчик: Алексеев Роман<br />
Дата создания: 19.07.23

## Проектная документация
Документация доступна по [ссылке](https://drive.google.com/drive/folders/1-6OgR-mLq0kQXJrx7dnfPgfwb8s1KgOL)

## Изменения
1. Добавлен ДАГ **transfer_to_datamarts_dag**.
2. Добавлено описание ДАГа **transfer_to_datamarts_dag**.
3. В ДАГе **transfer_to_dds_dag** PythonOperator заменен на BashOperator.
4. Удалены EmptyOperator.
5. Скорректировано описание ДАГа **transfer_to_dds_dag** и подготовки в соответствии с изменениями.
6. Добавлен *.yaml* файл.
7. Python-скрипты вынесены в отдельную директорию *scripts*. Для этого внесены изменения в *.yaml* файл.
8. Удалены *.csv* файлы.

## Подготовка
Перед запуском ДАГа необходимо:
1. Установить Docker согласно [инструкции](https://docs.docker.com/engine/install/) на сайте.
2. Установить Apache Airflow согласно [инструкции](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) на сайте.
   При создании использовать *yaml*-файл из репозитория.
4. Создать директории: **dags**, **config**, **logs**, **scripts**, **plugins**.
5. В локальные директории **dags** и **scripts** добавить файлы из директорий *dags* и *scripts* из репозитория.
6. В директории **dags** создать директорию *csv*, в которую поместить требуемые файлы формата *.csv* (На данный момент требуется файл *transactions-stores.csv*).
7. Запустить Apache Airflow при помощи ввода команды:<br />
   `docker compose up`<br />
   или<br />
   `sudo docker compose up`<br />
8. При помощи web-интерфейса Airflow, перейдя по ссылке [http://localhost:8080](http://localhost:8080), подключиться к БД **internship_3_db** и **internship_sources**, указав одноименные connection ID.<br />

Если все шаги выполнены верно, в списке ДАГов появится ДАГи: **transfer_to_datamarts_dag** и **transfer_to_dds_dag**<br />

## Описание ДАГа transfer_to_dds_dag
Разработанный ДАГ необходим для переноса информации из слоя sources в слой dds и одновременного удаления некорректных данных.<br />
ДАГ состоит из 3 шагов:
1. **clean_step** - шаг очистки слоя dds. На данном шаге очищаются все сущности схемы dds, кроме stores. Если каких-то сущностей или самой схемы нет, они создаются.
2. **create_wrong_schema_step** - шаг создания схемы для некорректных данных. При каждом запуске данные в сущностях удаляются.
3. **extract_data_step** - шаг переноса и обработки данных. Данный шаг выполняет bash-команду, которая запускает Python-скрипт из файла dds_transfer.py.

Код SQL-запросов находится в файлах *clean_dds.sql* для **clean_step** и *create_wrong_dds.sql* для **create_wrong_schema_step** в директории *dags/sql*.

### Выгрузка данных
Выгрузка данных на шаге **extract_data_step** осуществляется при помощи PostgresHook. Выгружается вся информация из всех сущностей слоя sources: product, stock, brand, category, transaction.<br />
В связи с тем, что в sources находится неполная сущность transaction, у которой отсутствует поле *pos*, данные об этом загружаются из файла *transactions-stores.csv*. Файл *transactions-stores.csv* находится в директории: *dags/csv*. После загрузки данные объединяются с данными из transaction при помощи LEFT JOIN.

### Обработка данных
Обработка осуществляется при помощи вызова функций: *correct_brand*, *correct_category*, *correct_product*, *correct_stock*, *correct_transaction* для данных из соответствующих сущностей.<br />
Для работы с данными используется библиотека **Pandas**.<br />
Также использованы библиотеки **NumPy** и **datetime**.<br />
В данных функциях поля проверяются по критериям:
- Является ли содержимое числового поля неотрицательным числом.
- Не является ли объект пустым.
- Наличие дубликатов первичных ключей.
- Соответствие объектов полей дат формату дат.
- Существование внешних ключей.
- Разница между датой и сегодняшним днем менее 2 лет.<br />

Также реализована проверка на возраст даты для поля *available_on* сущности stock, но ее код закомментирован (строки 272-279), т.к. все записи в stock старше двух лет.

### Сохранение некорректных данных.
В каждой функции для обработки данных осуществляется сохранение некорректных записей с добавлением комментария об ошибке.<br />
Данные сохраняются в схеме wrong_dds в соответствующих сущностях: product, stock, brand, category, transaction.<br />
Данные сущности не имеют первичных ключей, а также все поля имеют тип VARCHAR, чтобы было возможно сохранять некорректные данные с различными ошибками.

### Сохранение данных
Обработанные данные сохраняются в слой dds в соответствующие сущности.<br />
Для сохранения используется sqlalchemy_engine.

## Описание ДАГа transfer_to_datamarts_dag
Данный ДАГ запускает скрипты для обработки и сохранения данных из dds в datamart<br />
ДАГ состоит из 3 шагов:
1. **clean_step** - шаг очистки слоя dds. На данном шаге очищаются все сущности схемы datamarts, кроме order. Если каких-то сущностей или самой схемы нет, они создаются.
2. **create_wrong_step** - шаг создания схемы для некорректных данных. При каждой выгрузке данные в сущностях удаляются.
3. **extract_data_step** - шаг переноса и обработки данных. Данный шаг выполняет bash-команду, которая запускает Python-скрипт из файла datamarts_transfer.py.

Также реализован нулевой шаг **wait_for_transfer_to_dds**, который позволяет начать выполнение следующих шагов только при условии завершения работы ДАГа **transfer_to_dds_dag**. Код данного шага закомментирован (строки 7-9), т.к. данный код не позволяет запускать ДАГ вручную, только автоматически по расписанию. Данный шаг необходим, чтобы в слой datamarts попадали свежие данные.

Код SQL-запросов находится в файлах *clean_datamarts.sql* для **clean_step** и *create_wrong_datamarts.sql* для **create_wrong_step** в директории *dags/sql*.

### Выгрузка данных
Данные выгружаются аналогичным способом, как и в **transfer_to_dds_dag**, при помощи PostgresHook.

### Обработка данных
Обработка осуществляется при помощи вызова функций: *produce_orders* и *produce_transactions*.<br />
Для работы с данными используется библиотека **Pandas**.<br />
Также использованы библиотеки **NumPy** и **datetime**.<br />
При помощи JOIN LEFT сущности объединяются по внешним ключам, тем самым получаются итоговые сущности order - информация для формирования списка закупок, и transaction - данные по транзакциям для расчета прибыли.<br />

В сущностях добавлен ряд полей:
* update_date - дата начала работы Python-скрипта.
* tech_valid_from - время начала обработки данных.
* tech_valid_to - время конца обработки данных.

В сущности order добавлено поле *comment*, которое необходимо для оставления информации менеджером (количества закупаемого товара) для сравнения в будущем по периодам.

### Сохранение некорректных данных.
В связи с тем, что во время работы ДАГа **transfer_to_dds_dag** часть данных были перемещены в wrong_dds, в некоторых случаях в сущности stock нет записей для определенных *product_id*. В связи с этим, создана схема wrong_datamarts, в которую сохраняются некорректные данные с комментариями.

### Сохранение данных
Обработанные данные сохраняются в схему datamarts в соответствующие сущности.<br />
Для сохранения используется sqlalchemy_engine.



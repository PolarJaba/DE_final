import config as c
import psycopg2
import pandas as pd


# Подключение к базе данных
connection = psycopg2.connect(
    host=c.host,
    port=c.port,
    user=c.username,
    password=c.password,
    database=c.database
)

print('Загрузка данных в raw-слой')
# Создание и наполнение таблицы слоя сырых данных
with connection.cursor() as cur:
    query_create_schema = "CREATE SCHEMA IF NOT EXISTS final;"
    cur.execute(query_create_schema)
    query_create = ("CREATE TABLE IF NOT EXISTS final.raw_data (vendorid BIGINT, trip_pickup_datetime TIMESTAMP, "
                    "trip_dropoff_datetime TIMESTAMP, passengers_count INT, trip_distance FLOAT, ratecodeid INT, "
                    "store_and_fwd_flag VARCHAR(8), pulocationid VARCHAR(8), dolocationid VARCHAR(8), payment_type INT,"
                    "fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tools_amount FLOAT, "
                    "improvement_surchange FLOAT, total_amount FLOAT, congestion_surchange FLOAT);")
    cur.execute(query_create)
    cur.execute("COPY final.raw_data FROM '/init_db/data/yellow_tripdata_2020-01.csv' "
                "DELIMITER ',' ENCODING 'UTF8' CSV HEADER;")
    connection.commit()
    cur.close()

print('Создан слой сырых данных')
print('Подготовка данных core-слоя... . .  .  .   .')

# загрузка данных из БД postgres
with connection.cursor() as cur:
    load_raw_data = ("SELECT trip_pickup_datetime, trip_dropoff_datetime,  passengers_count, trip_distance, "
                    "fare_amount, tip_amount, total_amount FROM final.raw_data WHERE passengers_count >= 0 " 
                    "AND trip_distance > 0 AND fare_amount > 0 AND total_amount > 0 AND "
                    "trip_dropoff_datetime IS NOT NULL;")
    cur.execute(load_raw_data)
    raw_data = cur.fetchall()

data = pd.DataFrame(raw_data, columns=['trip_pickup_datetime', 'trip_dropoff_datetime',  'passengers_count', 
                                       'trip_distance', 'fare_amount', 'tip_amount', 'total_amount'])

data['duration'] = (data['trip_dropoff_datetime'] - data['trip_pickup_datetime']).dt.total_seconds() / 3600
data['trip_speed'] = data['trip_distance'] / data['duration']

# Максимально допустимая скорость (поездка в черте города, более высокий порог седней скорости абсурден)
anomalous_speed_threshold = 150

# Получение датафрейма с очищенными данными
cleaned_data = data[(data['trip_speed'] <= anomalous_speed_threshold)]
core_table_data = cleaned_data[['trip_dropoff_datetime', 'passengers_count', 'trip_distance', 'fare_amount', 
                                'tip_amount', 'total_amount']]
data_to_insert = [tuple(row) for row in core_table_data.to_numpy()]

print('Загрузка данных')
# Создание и наполнение таблицы core-слоя
with connection.cursor() as cur:
    query_create = ("CREATE TABLE IF NOT EXISTS final.core_data (trip_dropoff_datetime TIMESTAMP, passengers_count INT," 
                    "trip_distance FLOAT, fare_amount FLOAT, tip_amount FLOAT, total_amount FLOAT);")
    cur.execute(query_create)
    insert_query = ("INSERT INTO final.core_data(trip_dropoff_datetime, passengers_count, trip_distance, "
                    "fare_amount, tip_amount, total_amount) VALUES (%s, %s, %s, %s, %s, %s);")
    cur.executemany(insert_query, data_to_insert)
    connection.commit()
    cur.close()

print('Загрузка завершена')

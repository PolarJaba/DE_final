import config as c
import psycopg2


# Подключение к базе данных
connection = psycopg2.connect(
    host=c.host,
    port=c.port,
    user=c.username,
    password=c.password,
    database=c.database
)

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


# Создание и наполнение таблицы core-слоя
with connection.cursor() as cur:
    query_create = ("CREATE TABLE IF NOT EXISTS final.core_data (tpep_dropoff_datetime TIMESTAMP, passengers_count INT," 
                    "trip_distance FLOAT, fare_amount FLOAT, tip_amount FLOAT, total_amount FLOAT);")
    cur.execute(query_create)
    cur.execute("COPY final.core_data FROM '/init_db/data/core_table_data.csv' "
                "DELIMITER ',' ENCODING 'UTF8' CSV HEADER;")
    connection.commit()
    cur.close()

print('Загрузка завершена')

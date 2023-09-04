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

print("Создание витрины данных")
# Запрос
with connection.cursor() as cur:
    table_query = ("CREATE TABLE IF NOT EXISTS final.passengers_data_mart("
                   "\"date\" TIMESTAMP, "
                   "percentage_0p FLOAT, "
                   "percentage_1p FLOAT, "
                   "percentage_2p FLOAT, "
                   "percentage_3p FLOAT, "
                   "percentage_4p_plus FLOAT);")
    cur.execute(table_query)
    query = ("WITH vsp AS ("
             "SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\", passengers_count,"
             "ROW_NUMBER() OVER (ORDER BY date_trunc('day', cd.tpep_dropoff_datetime)) AS num "
             "FROM final.core_data cd), "
             "rnk_t_all AS ("
             "SELECT RANK() OVER (PARTITION BY \"date\" ORDER BY num) AS rnk, \"date\", "
             "COUNT(*) OVER (PARTITION BY \"date\") AS amt "
             "FROM vsp), "
             "rnk_t_0p AS ("
             "SELECT RANK() OVER (PARTITION BY \"date\" ORDER BY num) AS rnk, \"date\", "
             "COUNT(*) OVER (PARTITION BY \"date\") AS amt_0p "
             "FROM vsp "
             "WHERE passengers_count = 0), "
             "rnk_t_1p AS ("
             "SELECT RANK() OVER (PARTITION BY \"date\" ORDER BY num) AS rnk, \"date\", "
             "COUNT(*) OVER (PARTITION BY \"date\") AS amt_1p "
             "FROM vsp "
             "WHERE passengers_count = 1), "
             "rnk_t_2p AS ("
             "SELECT RANK() OVER (PARTITION BY \"date\" ORDER BY num) AS rnk, \"date\", "
             "COUNT(*) OVER (PARTITION BY \"date\") AS amt_2p "
             "FROM vsp "
             "WHERE passengers_count = 2), "
             "rnk_t_3p AS ("
             "SELECT RANK() OVER (PARTITION BY \"date\" ORDER BY num) AS rnk, \"date\", "
             "COUNT(*) OVER (PARTITION BY \"date\") AS amt_3p "
             "FROM vsp "
             "WHERE passengers_count = 3), "
             "rnk_t_more AS ("
             "SELECT RANK() OVER (PARTITION BY \"date\" ORDER BY num) AS rnk, \"date\", "
             "COUNT(*) OVER (PARTITION BY \"date\") AS amt_more "
             "FROM vsp "
             "WHERE passengers_count > 3), "
             "non_d_t_all AS (SELECT \"date\", amt FROM rnk_t_all WHERE rnk = 1), "
             "non_d_t_0p AS (SELECT \"date\", amt_0p FROM rnk_t_0p WHERE rnk = 1), "
             "non_d_t_1p AS (SELECT \"date\", amt_1p FROM rnk_t_1p WHERE rnk = 1), "
             "non_d_t_2p AS (SELECT \"date\", amt_2p FROM rnk_t_2p WHERE rnk = 1), "
             "non_d_t_3p AS (SELECT \"date\", amt_3p FROM rnk_t_3p WHERE rnk = 1), "
             "non_d_t_more AS (SELECT \"date\", amt_more FROM rnk_t_more WHERE rnk = 1) "
             "INSERT INTO final.passengers_data_mart (\"date\", percentage_0p, percentage_1p, percentage_2p, "
             "percentage_3p, percentage_4p_plus) "
             "(SELECT n1p.\"date\", round(amt_0p::NUMERIC/amt, 3), "
             "round(amt_1p::NUMERIC/amt, 3), round(amt_2p::NUMERIC/amt, 3), "
             "round(amt_3p::NUMERIC/amt, 3), round(amt_more::NUMERIC/amt, 3) "
             "FROM non_d_t_all n_all "
             "LEFT JOIN non_d_t_0p n0p ON n_all.\"date\" = n0p.\"date\" "
             "LEFT JOIN non_d_t_1p n1p ON n_all.\"date\" = n1p.\"date\" "
             "LEFT JOIN non_d_t_2p n2p ON n_all.\"date\" = n2p.\"date\" "
             "LEFT JOIN non_d_t_3p n3p ON n_all.\"date\" = n3p.\"date\" "
             "LEFT JOIN non_d_t_more n4p ON n_all.\"date\" = n4p.\"date\");")
    cur.execute(query)
    connection.commit()
    cur.close()

print('Создание витрины данных завершено')
print('Начинается создание витрины с данными о максимальной и минимальной стоимостях поездок')

with connection.cursor() as cur:
    table_query = ("CREATE TABLE IF NOT EXISTS final.cost_trip_datamart( "
                   "\"date\" TIMESTAMP, "
                   "max_cost_trip_0p FLOAT, "
                   "min_cost_trip_0p FLOAT, "
                   "max_cost_trip_1p FLOAT, "
                   "min_cost_trip_1p FLOAT, "
                   "max_cost_trip_2p FLOAT, "
                   "min_cost_trip_2p FLOAT, "
                   "max_cost_trip_3p FLOAT, "
                   "min_cost_trip_3p FLOAT, "
                   "max_cost_trip_4p_plus FLOAT, "
                   " min_cost_trip_4p_plus FLOAT);")
    cur.execute(table_query)
    query = ("WITH costs_0p_t AS ( "
             "SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\", MAX(total_amount) AS max_cost_trip_0p, "
             "MIN(total_amount) AS min_cost_trip_0p "
             "FROM final.core_data cd "
             "WHERE passengers_count = 0 "
             "GROUP BY date_trunc('day', cd.tpep_dropoff_datetime)), "
             "costs_1p_t AS ( "
             "SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\", MAX(total_amount) AS max_cost_trip_1p, "
             "MIN(total_amount) AS min_cost_trip_1p "
             "FROM final.core_data cd "
             "WHERE passengers_count = 1 "
             "GROUP BY date_trunc('day', cd.tpep_dropoff_datetime)), "
             "costs_2p_t AS ( "
             "SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\", MAX(total_amount) AS max_cost_trip_2p, "
             "MIN(total_amount) AS min_cost_trip_2p "
             "FROM final.core_data cd "
             "WHERE passengers_count = 2 "
             "GROUP BY date_trunc('day', cd.tpep_dropoff_datetime)), "
             "costs_3p_t AS ( "
             "SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\", MAX(total_amount) AS max_cost_trip_3p, "
             "MIN(total_amount) AS min_cost_trip_3p "
             "FROM final.core_data cd "
             "WHERE passengers_count = 3 "
             "GROUP BY date_trunc('day', cd.tpep_dropoff_datetime)), "
             "costs_4p_t AS ( "
             "SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\", MAX(total_amount) AS "
             "max_cost_trip_4p_plus, MIN(total_amount) AS min_cost_trip_4p_plus "
             "FROM final.core_data cd "
             "WHERE passengers_count > 3 "
             "GROUP BY date_trunc('day', cd.tpep_dropoff_datetime)) "
             "INSERT INTO final.cost_trip_datamart (\"date\", max_cost_trip_0p, min_cost_trip_0p, "
             "max_cost_trip_1p, min_cost_trip_1p, "
             "max_cost_trip_2p, min_cost_trip_2p, max_cost_trip_3p, min_cost_trip_3p, "
             "max_cost_trip_4p_plus, min_cost_trip_4p_plus) "
             "SELECT all_d.\"date\", max_cost_trip_0p, min_cost_trip_0p, max_cost_trip_1p, min_cost_trip_1p, "
             "max_cost_trip_2p, min_cost_trip_2p, max_cost_trip_3p, min_cost_trip_3p, "
             "max_cost_trip_4p_plus, min_cost_trip_4p_plus "
             "FROM (SELECT date_trunc('day', cd.tpep_dropoff_datetime) AS \"date\" "
             "FROM final.core_data cd "
             "GROUP BY date_trunc('day', cd.tpep_dropoff_datetime)) AS all_d "
             "LEFT JOIN costs_0p_t c0 ON all_d.\"date\" = c0.\"date\" "
             "LEFT JOIN costs_1p_t c1 ON all_d.\"date\" = c1.\"date\" "
             "LEFT JOIN costs_2p_t c2 ON all_d.\"date\" = c2.\"date\" "
             "LEFT JOIN costs_3p_t c3 ON all_d.\"date\" = c3.\"date\" "
             "LEFT JOIN costs_4p_t c4 ON all_d.\"date\" = c4.\"date\";")
    cur.execute(query)
    connection.commit()
    cur.close()

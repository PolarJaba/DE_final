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
             "SELECT date_trunc('day', cd.trip_dropoff_datetime) AS \"date\", passengers_count,"
             "ROW_NUMBER() OVER (ORDER BY date_trunc('day', cd.trip_dropoff_datetime)) AS num "
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


import pandas as pd
import config as c
import psycopg2

connection = psycopg2.connect(
    host=c.host,
    port=c.port,
    user=c.username,
    password=c.password,
    database=c.database
)

load_data_query = "SELECT * FROM final.passengers_data_mart"

cursor = connection.cursor()
cursor.execute(load_data_query)
data = cursor.fetchall()

datamart = pd.DataFrame(data, columns=['date', 'percentage_0p', 'percentage_1p', 'percentage_2p',
                                       'percentage_3p', 'percentage_4p'])

datamart.to_parquet('final_datamart_1.parquet', index=False)

print('Витрина данных успешно сохранена в формате parquet')

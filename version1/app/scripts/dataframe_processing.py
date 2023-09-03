import pandas as pd
import numpy as np

# Обработка и очистка данных

# Получение данных, при необходимости запустить программу измените путь
data = pd.read_csv('C:/Projects/final/init_db/data/yellow_tripdata_2020-01.csv')

# Преобразование формата
data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])

# Определение средней скорости поездки
data['duration'] = (data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime']).dt.total_seconds() / 3600
data['trip_speed'] = data['trip_distance'] / data['duration']

# Максимально допустимая скорость (поездка в черте города, более высокий порог седней скорости абсурден)
anomalous_speed_threshold = 150

# Получение датафрейма с очищенными данными
cleaned_df = data[(data['trip_speed'] <= anomalous_speed_threshold) &
                  (data['passenger_count'] is not None) &
                  (data['passenger_count'] >= 0) &
                  (data['trip_distance'] > 0) &
                  (data['fare_amount'] > 0) &
                  (data['total_amount'] > 0) &
                  (data['tpep_dropoff_datetime'] is not None)]

# Получение датафрейма с данными core-слоя (замените путь)
core_table_data = cleaned_df[['tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'fare_amount', 'tip_amount',
                              'total_amount']].astype({'passenger_count': np.int32})
core_table_data.to_csv(r'C:/Projects/final/init_db/data/core_table_data.csv', sep=',', index=False)

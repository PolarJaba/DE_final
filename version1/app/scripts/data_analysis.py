import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Получение данных, при необходимости запустить программу измените путь
# Взят очищенный файл
data = pd.read_csv('C:/Projects/final/init_db/data/core_table_data.csv')
df = data[['tip_amount', 'trip_distance', 'passenger_count']]


"""avg_tips = data.groupby(['trip_distance', 'passenger_count'])['tip_amount'].mean().unstack()

vmax_value = 250

plt.figure(figsize=(10, 5))
scatter = plt.scatter(data['trip_distance'], data['passenger_count'], c=data['tip_amount'], cmap='rainbow', alpha=0.7,
                      marker='o', vmax=vmax_value)
plt.xlabel('Расстояние')
plt.ylabel('Количество пассажиров')
plt.title('Как пройденное расстояние и количество пассажиров влияет на чаевые')
plt.colorbar(scatter, label='Сумма чаевых')
plt.grid(True)
plt.savefig('scatter_plot_1-2.png', dpi=300)"""

avg_tips_1 = data.groupby(['trip_distance', 'passenger_count'])['tip_amount'].mean()
print(avg_tips_1)

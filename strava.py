import openmeteo_requests
#from sqlalchemy import create_engine
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import timedelta, datetime


df_weather_data = pd.read_csv('com.samsung.shealth.exercise.weather.csv', delimiter=',', skiprows=1,header=0,index_col=False)
# Remove espaços e caracteres estranhos dos nomes das colunas
df_weather = df_weather_data.loc[:, ~df_weather_data.columns.str.contains('^Unnamed')]
df_weather_data.columns = df_weather_data.columns.str.strip()
print(df_weather_data.columns.tolist())


print(df_weather_data[['start_time']].head())	

df_weather_data = df_weather[['start_time', 'temperature', 'humidity']]

df_weather_data['start_time'] = df_weather_data['start_time'].astype(str)
df_weather_data['dataToJoin'] = df_weather_data['start_time'].str.split(' ').str[0]

df_weather_data.to_csv('desgraca', index=False)
         
	
df = pd.read_csv('com.samsung.shealth.exercise.consolidado.csv', delimiter=';', skiprows=2,header=0,index_col=False)

df = df[df['com.samsung.health.exercise.exercise_type'] == 1002]

if 'hours_adjusted' not in df.columns or not df['hours_adjusted'].all():
    df['com.samsung.health.exercise.start_time'] = pd.to_datetime(df['com.samsung.health.exercise.start_time'])
    df['com.samsung.health.exercise.end_time'] = pd.to_datetime(df['com.samsung.health.exercise.end_time'])
    
    df['com.samsung.health.exercise.start_time'] -= timedelta(hours=3)
    df['com.samsung.health.exercise.end_time'] -= timedelta(hours=3)
    
    df['hours_adjusted'] = True


print(df[['com.samsung.health.exercise.start_time', 'com.samsung.health.exercise.end_time']].head())

df_running_metrics = df[['total_calorie','com.samsung.health.exercise.duration', 'com.samsung.health.exercise.start_time',
						 'com.samsung.health.exercise.mean_heart_rate','com.samsung.health.exercise.distance',
						 'com.samsung.health.exercise.calorie','com.samsung.health.exercise.mean_speed',
						 'com.samsung.health.exercise.end_time']]

'''df_running_metrics = df[['total_calorie','heart_rate_sample_count', 'com.samsung.health.exercise.duration', 'com.samsung.health.exercise.start_time',
						 'com.samsung.health.exercise.mean_heart_rate', 'com.samsung.health.exercise.max_heart_rate',
                         'com.samsung.health.exercise.max_speed', 'com.samsung.health.exercise.mean_cadence',
                         'com.samsung.health.exercise.min_heart_rate', 'com.samsung.health.exercise.distance', 'com.samsung.health.exercise.calorie',
                         'com.samsung.health.exercise.max_cadence', 'com.samsung.health.exercise.vo2_max',
                         'com.samsung.health.exercise.mean_speed', 'com.samsung.health.exercise.end_time']]
'''
'''df_running_metrics['temperature_2m'] = None
df_running_metrics['relative_humidity_2m'] = None
df_running_metrics['wind_speed_10m'] = None '''

#Coleta a data e a hora
df_running_metrics['com.samsung.health.exercise.start_time'] = df_running_metrics['com.samsung.health.exercise.start_time'].astype(str)
df_running_metrics['data'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[0]
df_running_metrics['hora'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[1].str[:8]

df_running_metrics['dataToJoin'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[0]

#for i in range(0, len(df_running_metrics)):
    #searchHistoricWeather(df_running_metrics['data'].iloc[i], df_running_metrics['hora'].iloc[i])

#for i in range(0, 1):
    #searchHistoricWeather(df_running_metrics['data'].iloc[4], df_running_metrics['hora'].iloc[4])

#for index, row in df_running_metrics.iterrows():
	#if index == 87 or index == 51:
	#searchHistoricWeather(index, df_running_metrics['data'].loc[index], df_running_metrics['hora'].loc[index])


df_sleep = pd.read_csv('com.samsung.shealth.sleep.consolidado.csv', delimiter=';', skiprows=2)

'''df_sleep_data = df_sleep[['mental_recovery', 'physical_recovery', 'movement_awakening',
						  'sleep_cycle', 'efficiency', 'sleep_score', 'sleep_duration',
						  'com.samsung.health.sleep.start_time', 'com.samsung.health.sleep.end_time']]'''

df_sleep_data = df_sleep[['sleep_duration', 'com.samsung.health.sleep.start_time', 'com.samsung.health.sleep.end_time']]

df_sleep_data['dataToJoin'] = df_sleep_data['com.samsung.health.sleep.start_time'].str.split(' ').str[0]

# Converte sleep_duration para timedelta (para permitir somar horas)
df_sleep_data['sleep_duration'] = pd.to_timedelta(df_sleep_data['sleep_duration'], errors='coerce')

# Agrupa por data e soma a duração total de sono
'''df_sleep_data = (
    df_sleep_data
    .groupby('dataToJoin', as_index=False)
    .agg({
        'sleep_duration': 'sum',
        # você pode escolher manter outras métricas por média ou máximo
        'sleep_score': 'mean',
        'physical_recovery': 'mean',
        'mental_recovery': 'mean'
    })
)'''

df_merged = pd.merge(df_running_metrics, df_sleep_data, on='dataToJoin', how='inner')
df_final = pd.merge(df_merged, df_weather_data, on='dataToJoin', how='inner')
mask = ~df_final['temperature'].isnull()



# Aplicando a máscara e imprimindo os primeiros 5 valores
#print(df_merged[mask][['temperature_2m', 'com.samsung.health.exercise.start_time', 'com.samsung.health.exercise.end_time']].head())


df_recovery = pd.read_csv('com.samsung.shealth.exercise.recovery_heart_rate.csv', delimiter=";", skiprows=2,header=0,index_col=False)

df_recovery_data = df_recovery[['start_time', 'end_time', 'heart_rate']]

if 'hours_adjusted' not in df_recovery_data.columns or not df_recovery_data['hours_adjusted'].all():
    df_recovery_data['start_time'] = pd.to_datetime(df_recovery_data['start_time'])
    df_recovery_data['end_time'] = pd.to_datetime(df_recovery_data['end_time'])
    
    df_recovery_data['start_time'] -= timedelta(hours=3)
    df_recovery_data['end_time'] -= timedelta(hours=3)
    
    df_recovery_data['hours_adjusted'] = True
	



""" df_merged['com.samsung.health.exercise.end_time'] = df_merged['com.samsung.health.exercise.end_time'].dt.floor('S')
df_recovery_data['start_time'] = df_recovery_data['start_time'].dt.floor('S')

print(df_recovery_data['start_time'].head())

df_merged_all = pd.merge(df_merged, df_recovery_data, left_on='com.samsung.health.exercise.end_time', right_on='start_time', how='inner')
print(len(df_merged_all)) """

df_merged['com.samsung.health.exercise.end_time'] = df_merged['com.samsung.health.exercise.end_time'].dt.floor('S')
df_recovery_data['start_time'] = df_recovery_data['start_time'].dt.floor('S')

df_recovery_data = df_recovery_data[df_recovery_data['start_time'].isin(df_merged['com.samsung.health.exercise.end_time'])]
print(df_recovery_data.head(10))
#print(len(df_recovery_data))

df_final.to_csv('novosDadosTeste.csv', index=False)

print('✅ Dados salvos com sucesso no arquivo novosDadosTeste.csv!')     
import openmeteo_requests
#from sqlalchemy import create_engine
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import timedelta, datetime
import os

def get_closest_hourly_row(index, hourly_dataframe, hour_column, latitude, longitude):  
    time_obj = datetime.strptime(hour_column, '%H:%M:%S')
    
    if time_obj.minute < 30:
        rounded_time = time_obj.replace(minute=0, second=0, microsecond=0)
    else:
        rounded_time = (time_obj + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    
    # Adicionar o timezone offset "+00:00"
    rounded_time_str = rounded_time.strftime('%H:%M:%S') #+ '+00:00'

    hourly_dataframe['date'] = pd.to_datetime(hourly_dataframe['date'])
    #print(df_running_metrics.head())

    filtered_row = hourly_dataframe[hourly_dataframe['date'].dt.time == pd.to_datetime(rounded_time_str).time()]

    if not filtered_row.empty:
        temp_val = filtered_row['temperature_2m'].values[0]
        hum_val = filtered_row['relative_humidity_2m'].values[0]
        wind_val = filtered_row['wind_speed_10m'].values[0]

        if pd.isna(df_final.loc[index, 'temperature']):
            df_final.loc[index, 'temperature_x'] = temp_val
        else:
            df_final.loc[index, 'temperature_x'] = df_final.loc[index, 'temperature']

        if pd.isna(df_final.loc[index, 'humidity']):
            df_final.loc[index, 'humidity_x'] = hum_val
        else:
            df_final.loc[index, 'humidity_x'] = df_final.loc[index, 'humidity']

        if pd.isna(df_final.loc[index, 'wind_speed']):
            df_final.loc[index, 'wind_speed_x'] = wind_val
        else:
           df_final.loc[index, 'wind_speed_x'] = df_final.loc[index, 'wind_speed'] 

def searchHistoricWeather(index, data_column, hour_column, latitude, longitude):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": data_column,
        "end_date": data_column,
        "hourly": ["temperature_2m", "relative_humidity_2m", "weather_code", "wind_speed_10m"],
        "timezone": "America/Sao_Paulo"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()
    
    
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
     
    hourly_dataframe = pd.DataFrame(data = hourly_data)
    get_closest_hourly_row(index, hourly_dataframe, hour_column, latitude, longitude)
    #print(hourly_dataframe)

df_weather_data = pd.read_csv('com.samsung.shealth.exercise.weather.csv',decimal='.',delimiter=',',header=0,index_col=False)
# Remove espaços e caracteres estranhos dos nomes das colunas
df_weather = df_weather_data.loc[:, ~df_weather_data.columns.str.contains('^Unnamed')]
df_weather_data.columns = df_weather_data.columns.str.strip()
print(df_weather_data.columns.tolist())


print(df_weather_data[['start_time']].head())

#df_weather_data = df_weather[['start_time', 'temperature', 'humidity']]

df_weather_data['start_time'] = df_weather_data['start_time'].astype(str)
df_weather_data['dataToJoin'] = df_weather_data['start_time'].str.split(' ').str[0]
         
    
df = pd.read_csv('com.samsung.shealth.exercise.csv', delimiter=',',header=0,index_col=False)
# Remove espaços e caracteres estranhos dos nomes das colunas
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df.columns = df.columns.str.strip()

print(df.columns.tolist())

df = df[df['com.samsung.health.exercise.exercise_type'] == 1002]

if 'hours_adjusted' not in df.columns or not df['hours_adjusted'].all():
    df['com.samsung.health.exercise.start_time'] = pd.to_datetime(df['com.samsung.health.exercise.start_time'])
    df['com.samsung.health.exercise.end_time'] = pd.to_datetime(df['com.samsung.health.exercise.end_time'])
    
    df['com.samsung.health.exercise.start_time'] -= timedelta(hours=3)
    df['com.samsung.health.exercise.end_time'] -= timedelta(hours=3)
    
    df['hours_adjusted'] = True


print(df[['com.samsung.health.exercise.start_time', 'com.samsung.health.exercise.end_time']].head())

df_running_metrics = df[['total_calorie','heart_rate_sample_count', 'com.samsung.health.exercise.duration', 'com.samsung.health.exercise.start_time',
                         'com.samsung.health.exercise.mean_heart_rate', 'com.samsung.health.exercise.max_heart_rate',
                         'com.samsung.health.exercise.max_speed', 'com.samsung.health.exercise.mean_cadence',
                         'com.samsung.health.exercise.min_heart_rate', 'com.samsung.health.exercise.distance', 'com.samsung.health.exercise.calorie',
                         'com.samsung.health.exercise.max_cadence', 'com.samsung.health.exercise.vo2_max',
                         'com.samsung.health.exercise.mean_speed', 'com.samsung.health.exercise.end_time']]

df_running_metrics['temperature_x'] = None
df_running_metrics['humidity_x'] = None
df_running_metrics['wind_speed_x'] = None

#Coleta a data e a hora
df_running_metrics['com.samsung.health.exercise.start_time'] = df_running_metrics['com.samsung.health.exercise.start_time'].astype(str)
df_running_metrics['data'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[0]
df_running_metrics['hora'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[1].str[:8]

df_running_metrics['dataToJoin'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[0]


df_sleep = pd.read_csv('com.samsung.shealth.sleep.csv', delimiter=',',header=0,index_col=False)

df_sleep_data = df_sleep[['mental_recovery', 'physical_recovery', 'movement_awakening',
                          'sleep_cycle', 'efficiency', 'sleep_score', 'sleep_duration',
                          'com.samsung.health.sleep.start_time', 'com.samsung.health.sleep.end_time']]

df_sleep_data['dataToJoin'] = df_sleep_data['com.samsung.health.sleep.start_time'].str.split(' ').str[0]

# Converte sleep_duration para timedelta (para permitir somar horas)
#df_sleep_data['sleep_duration'] = pd.to_timedelta(df_sleep_data['sleep_duration'], errors='coerce')

# Agrupa por data e soma a duração total de sono
df_sleep_data = (
    df_sleep_data
    .groupby('dataToJoin', as_index=False)
    .agg({
        'sleep_duration': 'mean',
        # você pode escolher manter outras métricas por média ou máximo
        'sleep_score': 'mean',
        'physical_recovery': 'mean',
        'mental_recovery': 'mean'
    })
)
#junta exercise com sono
df_merged = pd.merge(df_running_metrics, df_sleep_data, on='dataToJoin', how='left')

# Agrupa por data e soma a duração total de sono
df_weather_data = (
    df_weather_data
    .groupby('dataToJoin', as_index=False)
    .agg({
        'temperature': 'mean',
        'wind_speed': 'mean',
        'humidity': 'mean',
        'latitude': 'mean',
        'longitude': 'mean'
    })
)

#junta merge com dados do tempo
df_final = pd.merge(df_merged, df_weather_data, on='dataToJoin', how='left')
# Definindo valores default de latitude e longitude
LAT_DEFAULT = -21.7642
LON_DEFAULT = -43.3503

df_final['latitude'] = df_final['latitude'].fillna(LAT_DEFAULT)
df_final['longitude'] = df_final['longitude'].fillna(LON_DEFAULT)

# Verifica se há linhas com dados climáticos faltando
'''mask_missing = (
    df_final['temperature'].isna() |
    df_final['humidity'].isna() |
    df_final['wind_speed'].isna()
)'''

# Percorre somente as linhas faltantes
'''for index, row in df_final[mask_missing].iterrows():
    print(f"🔎 Buscando clima histórico para linha {index} ({row['data']} {row['hora']})...")
    searchHistoricWeather(
        index=index,
        data_column=row['data'],      # formato YYYY-MM-DD
        hour_column=row['hora'],      # formato HH:MM:SS
        latitude=row['latitude'],   # valor padrão se não existir
        longitude=row['longitude']  # valor padrão se não existir
    )'''


'''df_recovery = pd.read_csv('com.samsung.shealth.exercise.recovery_heart_rate.csv', delimiter=";", skiprows=2,header=0,index_col=False)

df_recovery_data = df_recovery[['start_time', 'end_time', 'heart_rate']]

if 'hours_adjusted' not in df_recovery_data.columns or not df_recovery_data['hours_adjusted'].all():
    df_recovery_data['start_time'] = pd.to_datetime(df_recovery_data['start_time'])
    df_recovery_data['end_time'] = pd.to_datetime(df_recovery_data['end_time'])
    
    df_recovery_data['start_time'] -= timedelta(hours=3)
    df_recovery_data['end_time'] -= timedelta(hours=3)
    
    df_recovery_data['hours_adjusted'] = True
    

df_merged['com.samsung.health.exercise.end_time'] = df_merged['com.samsung.health.exercise.end_time'].dt.floor('S')
df_recovery_data['start_time'] = df_recovery_data['start_time'].dt.floor('S')

df_recovery_data = df_recovery_data[df_recovery_data['start_time'].isin(df_merged['com.samsung.health.exercise.end_time'])]
print(df_recovery_data.head(10))
#print(len(df_recovery_data))'''

for index, row in df_final.iterrows():
    #if index == 87 or index == 51:
    searchHistoricWeather(
        index, df_final['data'].loc[index],
        df_final['hora'].loc[index],
        df_final['latitude'].loc[index],
        df_final['longitude'].loc[index])
    
# Remove vírgulas e converte para float
df_final['com.samsung.health.exercise.distance_2'] = (
    df_final['com.samsung.health.exercise.distance']
    .astype(str)                      # garante que é string
    .str.replace('.', '', regex=False)  # remove pontos de milhar
    .str.replace(',', '.', regex=False) # substitui vírgula por ponto decimal (se existir)
    .astype(float)                     # converte para float
)


df_final['data'] = pd.to_datetime(df_final['data'])
df_final = df_final.sort_values('data')
df_final['volume_7d'] = (
    df_final
    .rolling('7d', on='data')['com.samsung.health.exercise.distance_2']
    .sum()
)
# ============================================================
#                    CÁLCULO DO PACE
# ============================================================
# Converte a duração de segundos para minutos, se necessário
df_final['duration_min'] = df_final['com.samsung.health.exercise.duration'] / 60
# Calcula pace em min/km
df_final['pace'] = df_final['duration_min'] / df_final['com.samsung.health.exercise.distance']
# Formata pace em minutos e segundos (opcional)
df_final['pace_min_sec'] = df_final['pace'].apply(lambda x: f"{int(x)}:{int((x-int(x))*60):02d} min/km")

# Caminho do diretório do final.py
current_dir = os.path.dirname(os.path.abspath(__file__))

# Sobe duas pastas
#output_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Caminho completo do CSV
output_path = os.path.join(current_dir, 'novosDadosTeste.csv')

# Salva o CSV
df_final.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"✅ Dados salvos em {output_path}")

#df_final.to_csv('novosDadosTeste.csv', index=False,encoding="utf-8-sig")

#print("Dados salvos com sucesso no banco de dados!") 
print('✅ Dados salvos com sucesso no arquivo novosDadosTeste.csv!')
    

import openmeteo_requests
#from sqlalchemy import create_engine
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import timedelta, datetime

def get_closest_hourly_row(index, hourly_dataframe, hour_column):  
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

	df_running_metrics['temperature_2m'].loc[index] = filtered_row['temperature_2m']
	df_running_metrics['relative_humidity_2m'].loc[index] = filtered_row['relative_humidity_2m']
	df_running_metrics['wind_speed_10m'].loc[index] = filtered_row['wind_speed_10m'] 

	#print(df_running_metrics[['temperature_2m', 'relative_humidity_2m', 'wind_speed_10m']].head(7))


def searchHistoricWeather(index, data_column, hour_column):
	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
	retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
	openmeteo = openmeteo_requests.Client(session=retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
	params = {
		"latitude": -21.7642,
		"longitude": -43.3503,
		"start_date": data_column,
		"end_date": data_column,
		"hourly": ["temperature_2m", "relative_humidity_2m", "weather_code", "wind_speed_10m"],
		"timezone": "America/Sao_Paulo"
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	#print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	#print(f"Elevation {response.Elevation()} m asl")
	#print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
	#print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

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
	get_closest_hourly_row(index, hourly_dataframe, hour_column)
	#print(hourly_dataframe)
	
         
	
df = pd.read_csv('com.samsung.shealth.exercise.victor.csv', delimiter=';', skiprows=2)

df = df[df['com.samsung.health.exercise.exercise_type'] == 1002]

if 'hours_adjusted' not in df.columns or not df['hours_adjusted'].all():
    df['com.samsung.health.exercise.start_time'] = pd.to_datetime(df['com.samsung.health.exercise.start_time'])
    df['com.samsung.health.exercise.end_time'] = pd.to_datetime(df['com.samsung.health.exercise.end_time'])
    
    df['com.samsung.health.exercise.start_time'] -= timedelta(hours=3)
    df['com.samsung.health.exercise.end_time'] -= timedelta(hours=3)
    
    df['hours_adjusted'] = True


print(df[['com.samsung.health.exercise.start_time', 'com.samsung.health.exercise.end_time']].head())

df_running_metrics = df[['total_calorie','heart_rate_sample_count', 
                         'location_data_internal', 'com.samsung.health.exercise.duration', 'com.samsung.health.exercise.start_time',
						 'com.samsung.health.exercise.mean_heart_rate', 'com.samsung.health.exercise.max_heart_rate',
                         'com.samsung.health.exercise.max_speed', 'com.samsung.health.exercise.mean_cadence',
                         'com.samsung.health.exercise.min_heart_rate', 'com.samsung.health.exercise.distance', 'com.samsung.health.exercise.calorie',
                         'com.samsung.health.exercise.max_cadence', 'com.samsung.health.exercise.vo2_max',
                         'com.samsung.health.exercise.mean_speed', 'com.samsung.health.exercise.end_time',
                         'com.samsung.health.exercise.sweat_loss']]
#print(df_running_metrics.head(10))

df_running_metrics['temperature_2m'] = None
df_running_metrics['relative_humidity_2m'] = None
df_running_metrics['wind_speed_10m'] = None 

#Coleta a data e a hora
df_running_metrics['com.samsung.health.exercise.start_time'] = df_running_metrics['com.samsung.health.exercise.start_time'].astype(str)
df_running_metrics['data'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[0]
df_running_metrics['hora'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[1].str[:8]

df_running_metrics['dataToJoin'] = df_running_metrics['com.samsung.health.exercise.start_time'].str.split(' ').str[0]




#for i in range(0, len(df_running_metrics)):
    #searchHistoricWeather(df_running_metrics['data'].iloc[i], df_running_metrics['hora'].iloc[i])

#for i in range(0, 1):
    #searchHistoricWeather(df_running_metrics['data'].iloc[4], df_running_metrics['hora'].iloc[4])

for index, row in df_running_metrics.iterrows():
	#if index == 87 or index == 51:
	searchHistoricWeather(index, df_running_metrics['data'].loc[index], df_running_metrics['hora'].loc[index])



df_sleep = pd.read_csv('com.samsung.shealth.sleep.victor.csv', delimiter=';', skiprows=2)

df_sleep_data = df_sleep[['mental_recovery','factor_01', 'factor_02', 'factor_03', 
						  'factor_04','factor_05','factor_06', 'factor_07', 'factor_08', 
						  'factor_09', 'factor_10', 'physical_recovery', 'movement_awakening',
						  'sleep_cycle', 'efficiency', 'sleep_score', 'sleep_duration',
						  'com.samsung.health.sleep.start_time', 'com.samsung.health.sleep.end_time']]

df_sleep_data['dataToJoin'] = df_sleep_data['com.samsung.health.sleep.start_time'].str.split(' ').str[0]

# Converte sleep_duration para timedelta (para permitir somar horas)
df_sleep_data['sleep_duration'] = pd.to_timedelta(df_sleep_data['sleep_duration'], errors='coerce')

# Agrupa por data e soma a duração total de sono
''' df_sleep_data = (
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
mask = ~df_merged['temperature_2m'].isnull()



# Aplicando a máscara e imprimindo os primeiros 5 valores
#print(df_merged[mask][['temperature_2m', 'com.samsung.health.exercise.start_time', 'com.samsung.health.exercise.end_time']].head())


df_recovery = pd.read_csv('com.samsung.shealth.exercise.recovery_heart_rate.csv', delimiter=";", skiprows=2)

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


#engine = create_engine('mysql+pymysql://root:toca12345@localhost:3306/teste')
#df_merged.to_sql('novosDadosTeste', engine, if_exists='replace', index=False)
df_merged.to_csv('novosDadosTeste.csv', index=False)

#print("Dados salvos com sucesso no banco de dados!") 
print('✅ Dados salvos com sucesso no arquivo novosDadosTeste.csv!')

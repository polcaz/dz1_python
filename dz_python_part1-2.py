import pandas as pd
import time
from joblib import Parallel, delayed
import requests
from datetime import datetime
import aiohttp
import asyncio

"""
ЧАСТЬ 1. Получение данных
"""
# Читаем данные
data = pd.read_csv('temperature_data.csv')

# Получение скользящего среднего по городам
data['rolling_mean'] = data.groupby('city')['temperature'].transform(lambda x: x.rolling(window = 30).mean())

# Группировка по городу и сезону для него и вычисление среднего и стандартного отклонения
city_season_stats = data.groupby(['city', 'season'])['temperature'].agg(['mean' , 'std']).reset_index()

# Поиск аномалий
def find_anomalies(row, statistics):
    data_statistics = statistics[(statistics['city'] == row['city']) & (statistics['season'] == row['season'])]
    if data_statistics.empty:
        return False
    mean_temp = data_statistics['mean'].values[0]
    std_temp = data_statistics['std'].values[0] 

    low_level = mean_temp - 2 * std_temp
    up_level = mean_temp + 2 * std_temp

    return not (low_level <= row['temperature'] <= up_level)

# Поиск аномалий с распараллеливанием
def parallel_find_anomalies(data , statistics , n_jobs = -1):
    def get_frame(frame):
        frame['anomaly'] = frame.apply(find_anomalies , axis=1, statistics=statistics)
        return frame
    
    # Разбивка исходного датасета на куски по каждому городу
    frames = [data[data['city'] == city] for city in data['city'].unique()]

    # Запуск параллельной функции
    processed_frames = Parallel(n_jobs=n_jobs)(delayed(get_frame)(frame) for frame in frames)

    result = pd.concat(processed_frames, axis = 0)
    return result

# Время обработки в случае без распараллеливания
print('Время обработки данных в обоих случаях: \n')
start_time = time.time()
data['anomaly'] = data.apply(find_anomalies, axis=1, statistics=city_season_stats)
print(f'Без распараллеливания:{time.time() - start_time:.2f} сек')

# Время обработки в случае с распараллеливанием
start_time = time.time()
data_parallel = parallel_find_anomalies(data , statistics=city_season_stats)
print(f'С распараллеливанием:{time.time() - start_time:.2f} сек \n')


"""
ЧАСТЬ 2. Работа с API
"""

# Получение данных по API
api_key = '2bbf71791159863c390f044fa06313b0'

"""
Синхронный подход
----------------------------------------------
"""
# Функция получения координат по названию города
def get_lat_lon(city, api_key):

    url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}'

    try:
        response = requests.get(url)
        data = response.json()
        if not data:
            raise Exception(f'Город {city} не найден')
                # Извлечение широты и долготы из первой записи
        lat = data[0]['lat']
        lon = data[0]['lon']
        return lat, lon
    
    except Exception as e:
        print(f"Ошибка при получении координат: {e}")
        return None, None
    
# Функция получения температуры по названию города
def get_temperature(city,  api_key):
    lat, lon = get_lat_lon(city, api_key)
    if lat is None or lon is None:
        print(f"Не удалось получить координаты для города {city}.")
        return None

    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        data = response.json()
        
        if response.status_code != 200:
            raise Exception(f"Ошибка API: {data.get('message', 'Неизвестная ошибка')}")
        
        # Извлечение текущей температуры
        current_temp = data['main']['temp']
        return current_temp
    
    except Exception as e:
        print(f"Ошибка при получении температуры: {e}")
        return None
    
# Функция получения сезона 
def season_today():
    month = {12: "winter", 1: "winter", 2: "winter", 3: "spring", 4: "spring", 5: "spring",
        6: "summer", 7: "summer", 8: "summer", 9: "autumn", 10: "autumn", 11: "autumn"}
    month_today = datetime.now().month
    return month[month_today]

# Функция проверки аномальности температуры для определенного города
def is_tempereture_normal(city, current_temp, statistics):
    season_current = season_today()
    data_statistics = statistics[(statistics['city'] == city) & (statistics['season'] == season_current)]
    if data_statistics.empty:
        return False
    mean_temp = data_statistics['mean'].values[0]
    std_temp = data_statistics['std'].values[0] 

    low_level = mean_temp - 2 * std_temp
    up_level = mean_temp + 2 * std_temp

    return not (low_level <= current_temp <= up_level)

"""
Асинхронный подход
----------------------------------------------
"""
# Асинхронная функция получения координат по названию города
async def get_lat_lon_async(city, api_key, session):

    url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}'

    try:
        async with session.get(url) as response:
            data = await response.json()
        if not data:
            raise Exception(f'Город {city} не найден')
        # Извлечение широты и долготы из первой записи
        lat = data[0]['lat']
        lon = data[0]['lon']
        return lat, lon
        
    except Exception as e:
        print(f"Ошибка при получении координат: {e}")
        return None, None

# Асинхронное получение температуры
async def get_temperature_async(city,  api_key, session):

    lat, lon = await get_lat_lon_async(city, api_key, session)
    if lat is None or lon is None:
        print(f"Не удалось получить координаты для города {city}.")
        return None

    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"    
    try:
        async with session.get(url) as response:
            data = await response.json()
        if response.status != 200:
            raise Exception(f"Ошибка API: {data.get('message', 'Неизвестная ошибка')}")
            
        # Извлечение текущей температуры
        current_temp = data['main']['temp']
        return current_temp
        
    except Exception as e:
        print(f"Ошибка при получении температуры: {e}")
        return None
        
# Функция тестирования асинхронного подхода

async def test_async_func(cities, api_key, city_season_stats):
    start_time_async = time.time()
    print('АСИНХРОННЫЙ подход')
    print('------------------ \n')
    print(f"Получение температур для выбранных городов: \n")
    async with aiohttp.ClientSession() as session:
        async def process_city(city):
            current_temp = await get_temperature_async(city, api_key, session)
            if current_temp is not None:
                print(f"Текущая температура в городе {city}: {current_temp}°C")
            else:
                print(f"Не удалось получить данные о температуре для города {city}.")
            print(f'Является ли температура аномальной?:{is_tempereture_normal(city, current_temp, statistics = city_season_stats)} \n')

        # Запускаем все запросы параллельно
        await asyncio.gather(*(process_city(city) for city in cities))

        print(f'Выполнено за: {time.time() - start_time_async:.2f} секунд')


if __name__ == "__main__":

    # Название города для тестирования
    cities = ["Berlin", "Cairo", "Dubai", "Beijing", "Moscow"]

    """
    Тестирование синхронного подхода
    """    
    print('Тестирование получения текущей температуры и выявление аномалий с помощью 2ух подходов: \n')
    print('СИНХРОННЫЙ подход')
    print('------------------ \n')
    start_time = time.time()
    for city in cities:
        print(f"Получение текущей температуры для города {city}")
    
        # Синхронное получение текущей температуры
        current_temp = get_temperature(city, api_key)
        if current_temp is not None:
            print(f"Текущая температура в городе {city}: {current_temp}°C")
        else:
            print(f"Не удалось получить данные о температуре для города {city}.")
        print(f'Является ли температура аномальной?:{is_tempereture_normal(city, current_temp, statistics = city_season_stats)} \n')
    print(f'Выполнено за: {time.time() - start_time:.2f} секунд \n')
    """
    Тестирование асинхронного подхода
    """  
    asyncio.run(test_async_func(cities, api_key, city_season_stats))
    # await test_async_func(cities, api_key, city_season_stats)




    
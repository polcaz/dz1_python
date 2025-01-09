import pandas as pd
import requests
from datetime import datetime
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# Загрузка данных
def load_data(file):
    data = pd.read_csv(file)
    return data

# Функция получения координат по названию города
def get_lat_lon(city, api_key):
    url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}'

    try:
        response = requests.get(url)
        data = response.json()

        if response.status_code == 200 and data: 
            lat = data[0]['lat']
            lon = data[0]['lon']
            return (lat, lon), None
        elif not data:
            return None, f'Город {city} не найден'
        else:
            error_message = data.get('message', 'Неизвестная ошибка')
            return None, error_message
    except Exception as e:
        return None, str(e)


# Функция получения температуры по названию города
def get_temperature(city, api_key):
    coords, error = get_lat_lon(city, api_key)
    if error:
        return None, error

    lat, lon = coords
    url = f'http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric'
    try:
        response = requests.get(url)
        data = response.json()

        if response.status_code == 200:
            return data['main']['temp'], None

        error_message = data.get('message', 'Неизвестная ошибка')
        return None, error_message

    except requests.exceptions.RequestException as e:
        return None, f'Ошибка соединения: {str(e)}'

    except Exception as e:
        return None, f'Неизвестная ошибка: {str(e)}'

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

# Функция получения сезона
def season_today():
    month = {12: "winter", 1: "winter", 2: "winter", 3: "spring", 4: "spring", 5: "spring",
             6: "summer", 7: "summer", 8: "summer", 9: "autumn", 10: "autumn", 11: "autumn"}
    month_today = datetime.now().month
    return month[month_today]

# Функция проверки аномальности температуры для определенного города
def is_temperature_normal(city, current_temp, statistics):
    season_current = season_today()
    data_statistics = statistics[(statistics['city'] == city) & (statistics['season'] == season_current)]
    if data_statistics.empty:
        return False
    mean_temp = data_statistics['mean'].values[0]
    std_temp = data_statistics['std'].values[0]

    low_level = mean_temp - 2 * std_temp
    up_level = mean_temp + 2 * std_temp

    return low_level <= current_temp <= up_level


st.title('Анализ температуры и мониторинг погоды')

# Загрузка данных
st.header('1. Загрузите файл с историческими данными о погоде')
uploaded_file = st.file_uploader('Загрузите файл с историческими данными (.csv)', type='csv')
if uploaded_file:
    data = load_data(uploaded_file)
    st.success('Данные загружены')
    st.write(data.head())

    # Получение скользящего среднего по городам
    data['rolling_mean'] = data.groupby('city')['temperature'].transform(lambda x: x.rolling(window=30).mean())

    # Группировка по городу и сезону для него и вычисление среднего и стандартного отклонения
    city_season_stats = data.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()

    # Описание данных
    st.header('2. Описательные характеристики данных')
    pick_city = st.selectbox('Выберите интересующий город:', data['city'].unique())
    pick_city_data = data[data['city'] == pick_city]
    st.write(pick_city_data.describe())

    # Визуализация распределения температур
    st.subheader('Гистограмма распределения температур')
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(pick_city_data['temperature'], bins=80, color='blue', alpha=0.7, edgecolor='black')
    ax.set_title(f'Распределение температур в {pick_city}')
    ax.set_xlabel('Температура (°C)')
    ax.set_ylabel('Частота')
    st.pyplot(fig)

    # Визуализация боксплота (boxplot)
    st.subheader('Боксплот температур')
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.boxplot(pick_city_data['temperature'], vert=False, patch_artist=True,
            boxprops=dict(facecolor='orange', color='black'),
            whiskerprops=dict(color='black'), capprops=dict(color='black'),
            medianprops=dict(color='red'))
    ax.set_title(f'Боксплот температур в {pick_city}')
    ax.set_xlabel('Температура (°C)')
    st.pyplot(fig)

    # Визуализация временного ряда
    st.header('3. Временной ряд температур')
    pick_city_data['timestamp'] = pd.to_datetime(pick_city_data['timestamp'], errors='coerce')

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(pick_city_data['timestamp'], pick_city_data['temperature'], label="Температура", color='blue', alpha=0.7)
    ax.plot(pick_city_data['timestamp'], pick_city_data['rolling_mean'], label="Скользящее среднее (30 дней)", color='orange', linewidth=2)
    ax.grid(visible=True, linestyle='--', alpha=0.5)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(fontsize=10)
    ax.set_title(f'Температура в {pick_city}')
    ax.set_xlabel('Дата')
    ax.set_ylabel('Температура (°C)')
    ax.legend()
    st.pyplot(fig)

    # Визуализация аномалий
    st.header('4. Выделение аномалий')
    pick_city_data['anomaly'] = pick_city_data.apply(find_anomalies, axis=1, statistics=city_season_stats)

    fig, ax = plt.subplots(figsize=(12, 6))
    anomalies = pick_city_data[pick_city_data['anomaly']]
    ax.plot(pick_city_data['timestamp'], pick_city_data['temperature'], label='Температура', color='blue', alpha=0.7)
    ax.scatter(anomalies['timestamp'], anomalies['temperature'], color="red", label='Аномалии')
    ax.grid(visible=True, linestyle="--", alpha=0.5)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(fontsize=10)
    ax.set_title(f'Аномалии температуры в {pick_city}')
    ax.set_xlabel('Дата')
    ax.set_ylabel('Температура (°C)')
    ax.legend()
    st.pyplot(fig)

    # API для текущей температуры
    st.header('5. Текущая температура')
    api_key = st.text_input('Введите API-ключ OpenWeatherMap:')

    if not api_key:
        st.warning("Введите API-ключ, чтобы получить данные о текущей погоде.")
    else:
        if st.button('Получить данные'):
            current_temp, error = get_temperature(pick_city, api_key)
            if error: 
                st.error(f'Ошибка: {error}')
            else:
                st.success(f'Текущая температура в городе {pick_city}: {current_temp}°C')
                if is_temperature_normal(pick_city, current_temp, city_season_stats):
                    st.info('Температура в пределах нормы для текущего сезона.')
                else:
                    st.warning('Температура является аномальной для текущего сезона.')
    
    st.header('6. Просмотр характеристик за конкретный временной диапазон')

    # Временной фильтр
    start_date = st.date_input('Начальная дата', value=pick_city_data['timestamp'].min())
    end_date = st.date_input('Конечная дата', value=pick_city_data['timestamp'].max())

    # Фильтрация данных по диапазону дат
    filtered_data = pick_city_data[(pick_city_data['timestamp'] >= pd.to_datetime(start_date)) &
                                (pick_city_data['timestamp'] <= pd.to_datetime(end_date))]
    if filtered_data.empty:
        st.warning('Нет данных для выбранного диапазона дат.')
    else:
        st.write(f'Данные для периода: {start_date} - {end_date}')
        st.write(filtered_data)

    # Сравнение температур между городами
    st.subheader('7. Сравнение истории температур между городами')
    selected_cities = st.multiselect('Выберите города для сравнения:', data['city'].unique(), default=[pick_city])

    if selected_cities:
        comparison_data = data[data['city'].isin(selected_cities)]

        fig, ax = plt.subplots(figsize=(12, 6))
        for city in selected_cities:
            city_data = comparison_data[comparison_data['city'] == city]
            ax.plot(city_data['timestamp'], city_data['temperature'], label=city, alpha=0.7)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.set_title('Сравнение температур между городами')
        ax.set_xlabel('Дата')
        ax.set_ylabel('Температура (°C)')
        ax.legend()
        st.pyplot(fig)


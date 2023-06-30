# import logging
# import threading
# import subprocess
# import multiprocessing

from external.client import YandexWeatherAPI
from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import get_url_by_city_name


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    city_name = "MOSCOW"
    url_with_data = get_url_by_city_name(city_name)
    resp = YandexWeatherAPI.get_forecasting(url_with_data)
    return resp


if __name__ == "__main__":
    data = DataFetchingTask.get_weather_data()
    data = DataCalculationTask.calculate_data(data)
    DataAggregationTask.aggregate_data(data)
    optimal_city_data = DataAnalyzingTask.get_optimal_city()
    print(
        f'The best city is {optimal_city_data["city_name"]} with '
        f'{optimal_city_data["avg_temp"]} average temperature and '
        f'{optimal_city_data["avg_relevant_cond_hours"]} hours of relevant conditions',
    )

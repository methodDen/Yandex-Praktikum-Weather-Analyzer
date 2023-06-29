# import logging
# import threading
# import subprocess
# import multiprocessing
from pprint import pprint

from external.analyzer import analyze_json
from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, get_url_by_city_name


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    city_name = "MOSCOW"
    url_with_data = get_url_by_city_name(city_name)
    resp = YandexWeatherAPI.get_forecasting(url_with_data)
    # print(resp)
    # pass
    return resp


if __name__ == "__main__":
    # resp = forecast_weather()
    data = DataFetchingTask.get_weather_data()
    # data = analyze_json(resp)
    pprint(data)
    DataCalculationTask.calculate_data(data)

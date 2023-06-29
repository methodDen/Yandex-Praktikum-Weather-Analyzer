import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from typing import List

from external.client import YandexWeatherAPI
from external.analyzer import analyze_json, deep_getitem
from utils import CITIES, get_url_by_city_name


class DataFetchingTask:

    @staticmethod
    def get_weather_data_by_city(city_name: str) -> dict:
        data = {"city_name": city_name, "data": {}}
        try:
            url_with_data = get_url_by_city_name(city_name)
            data["data"] = YandexWeatherAPI.get_forecasting(url_with_data)
            return data
        except Exception as ex:
            print(f"Error during fetching data for {city_name}: {ex}")
            data["data"] = {}
            return data

    @staticmethod
    def reformat_data(data: dict) -> dict:
        return {
            "city_name": data["city_name"],
            **analyze_json(data["data"])
        }

    @staticmethod
    def get_weather_data():
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = pool.map(DataFetchingTask.get_weather_data_by_city, CITIES)
        with multiprocessing.Pool() as pool:
            results = pool.map(DataFetchingTask.reformat_data, results)
        return results


class DataCalculationTask:
    @staticmethod
    def calculate_average(data: dict) -> dict:
        pass
    @staticmethod
    def calculate_data(data: List[dict]) -> dict:
        for d in data:
            print(type(deep_getitem(d, 'days')))
            print()
            print()


class DataAggregationTask:
    pass


class DataAnalyzingTask:

    # use multiprocessing
    pass

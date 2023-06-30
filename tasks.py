import json
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List
import csv
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
    def calculate_chunk_sum_for_metrics(data: List[dict]) -> dict:
        try:
            return {
                "temp_avg_sum": sum([deep_getitem(d, "temp_avg") for d in data if deep_getitem(d, "temp_avg") is not None]),
                "relevant_cond_hours_sum": sum([deep_getitem(d, "relevant_cond_hours") for d in data]),
                "number_of_days": sum(1 for d in data if deep_getitem(d, "hours_count") > 0),
            }
        except Exception as ex:
            print(f"Error during calculating data: {ex}")
            return {
                "temp_avg_sum": 0,
                "relevant_cond_hours_sum": 0,
                "number_of_days": 0,
            }

    @staticmethod
    def sort_calculated_data(data: List[dict]) -> List[dict]:
        data = sorted(data, key=lambda x: (x["avg_temp"] + x['avg_relevant_cond_hours']), reverse=True)
        for i in range(len(data), 0, -1):
            if data[i - 1]["avg_temp"] == 0:
                data[i - 1]["rating"] = 0
            else:
                data[i - 1]["rating"] = i
        return data



    # general method for all data (finding average data for all cities)
    @staticmethod
    def calculate_data(data_list: List[dict]) -> List[dict]:
        for data in data_list:
            days_data = deep_getitem(data, "days")
            with multiprocessing.Pool(3) as pool:
                if days_data:
                    chunk_size = len(days_data) // 3
                    chunks = [days_data[i:i + chunk_size] for i in range(0, len(days_data), chunk_size)]
                    chunk_metric_sum_results = pool.map(
                        DataCalculationTask.calculate_chunk_sum_for_metrics,
                        chunks,
                    )
                    overall_metric_sum_results = {
                        "temp_avg_sum": sum(
                            [deep_getitem(d, "temp_avg_sum") for d in chunk_metric_sum_results]
                        ),
                        "relevant_cond_hours_sum": sum(
                            [deep_getitem(d, "relevant_cond_hours_sum") for d in chunk_metric_sum_results]
                        ),
                        "number_of_days": sum(
                            [deep_getitem(d, "number_of_days") for d in chunk_metric_sum_results]
                        ),
                    }

                    data["avg_temp"] = round(
                        overall_metric_sum_results["temp_avg_sum"] / overall_metric_sum_results["number_of_days"], 3
                    )
                    data["avg_relevant_cond_hours"] = round(
                        overall_metric_sum_results["relevant_cond_hours_sum"] /
                        overall_metric_sum_results["number_of_days"], 3
                    )
                else:
                    # TODO: add logging
                    data["avg_temp"] = 0
                    data["avg_relevant_cond_hours"] = 0
        data_list = DataCalculationTask.sort_calculated_data(data_list)

        return data_list


class DataAggregationTask:

    @staticmethod
    def write_data(data: dict, file, lock: multiprocessing.Lock):
        with lock:
            json.dump(data, file)

    @staticmethod
    def aggregate_data(data: List[dict]):
        lock = threading.Lock()
        filepath = "data.json"
        file = open(filepath, "a", newline='')
        threads = [threading.Thread(target=DataAggregationTask.write_data, args=(d, file, lock)) for d in data]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        file.close()


class DataAnalyzingTask:

    # use multiprocessing
    pass

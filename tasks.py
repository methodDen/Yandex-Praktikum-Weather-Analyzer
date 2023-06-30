import json
import logging
import multiprocessing
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List

from external.analyzer import analyze_json, deep_getitem
from external.client import YandexWeatherAPI
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
            logging.error(f"Error during fetching data for {city_name}: {ex}")
            data["data"] = {}
            return data

    @staticmethod
    def reformat_data(data: dict) -> dict:
        return {"city_name": data["city_name"], **analyze_json(data["data"])}

    @staticmethod
    def get_weather_data() -> List[dict]:
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = pool.map(
                DataFetchingTask.get_weather_data_by_city, CITIES,
            )
        with multiprocessing.Pool() as pool:
            results = pool.map(DataFetchingTask.reformat_data, results)
        return results


class DataCalculationTask:
    @staticmethod
    def calculate_chunk_sum_for_metrics(data: List[dict]) -> dict:
        try:
            return {
                "temp_avg_sum": sum(
                    [
                        deep_getitem(d, "temp_avg")
                        for d in data
                        if deep_getitem(d, "temp_avg") is not None
                    ],
                ),
                "relevant_cond_hours_sum": sum(
                    [deep_getitem(d, "relevant_cond_hours") for d in data],
                ),
                "number_of_days": sum(
                    1 for d in data if deep_getitem(d, "hours_count") > 0
                ),
            }
        except Exception as ex:
            logging.error(f"Error during calculating data: {ex}")
            return {
                "temp_avg_sum": 0,
                "relevant_cond_hours_sum": 0,
                "number_of_days": 0,
            }

    @staticmethod
    def sort_calculated_data(data: List[dict]) -> List[dict]:
        data = sorted(
            data,
            key=lambda x: (x["avg_temp"] + x["avg_relevant_cond_hours"]),
            reverse=True,
        )
        for i in range(len(data), 0, -1):
            if data[i - 1]["avg_temp"] == 0:
                data[i - 1]["rating"] = 0
            else:
                data[i - 1]["rating"] = i
        return data

    @staticmethod
    def calculate_data(data_list: List[dict]) -> List[dict]:
        for data in data_list:
            days_data = deep_getitem(data, "days")
            with multiprocessing.Pool(3) as pool:
                if days_data:
                    chunk_size = len(days_data) // 3
                    chunks = [
                        days_data[i: i + chunk_size]
                        for i in range(0, len(days_data), chunk_size)
                    ]
                    chunk_metric_sum_results = pool.map(
                        DataCalculationTask.calculate_chunk_sum_for_metrics,
                        chunks,
                    )
                    overall_metric_sum_results = {
                        "temp_avg_sum": sum(
                            [
                                deep_getitem(d, "temp_avg_sum")
                                for d in chunk_metric_sum_results
                            ],
                        ),
                        "relevant_cond_hours_sum": sum(
                            [
                                deep_getitem(d, "relevant_cond_hours_sum")
                                for d in chunk_metric_sum_results
                            ],
                        ),
                        "number_of_days": sum(
                            [
                                deep_getitem(d, "number_of_days")
                                for d in chunk_metric_sum_results
                            ],
                        ),
                    }

                    data["avg_temp"] = round(
                        overall_metric_sum_results["temp_avg_sum"]
                        / overall_metric_sum_results["number_of_days"],
                        3,
                    )
                    data["avg_relevant_cond_hours"] = round(
                        overall_metric_sum_results["relevant_cond_hours_sum"]
                        / overall_metric_sum_results["number_of_days"],
                        3,
                    )
                else:
                    logging.warning(f"No data for {data['city_name']}")
                    data["avg_temp"] = 0
                    data["avg_relevant_cond_hours"] = 0
        data_list = DataCalculationTask.sort_calculated_data(data_list)

        return data_list


class DataAggregationTask:
    @staticmethod
    def write_data(data: dict, file, lock: multiprocessing.Lock) -> None:
        with lock:
            json.dump(data, file, indent=2)
            file.write(",\n")

    @staticmethod
    def aggregate_data(data: List[dict]) -> None:
        lock = threading.Lock()
        file = open('data.json', "a")
        file.write("[\n")
        threads = [
            threading.Thread(
                target=DataAggregationTask.write_data, args=(d, file, lock),
            )
            for d in data
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        file.close()

        with open('data.json', 'rb+') as filehandle:
            filehandle.seek(-2, os.SEEK_END)
            filehandle.truncate()

        with open('data.json', 'a') as filehandle:
            filehandle.write("\n]")


class DataAnalyzingTask:
    @staticmethod
    def read_data(filepath: str) -> List[dict]:
        with open(filepath, "r") as file:
            json_data = file.read()
        data = json.loads(json_data)
        return data

    @staticmethod
    def get_optimal_city() -> dict:
        data = DataAnalyzingTask.read_data("data.json")
        data = {
            "city_name": data[0]["city_name"],
            "avg_temp": data[0]["avg_temp"],
            "avg_relevant_cond_hours": data[0]["avg_relevant_cond_hours"],
        }
        return data

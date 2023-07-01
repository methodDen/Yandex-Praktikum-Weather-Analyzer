from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)

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

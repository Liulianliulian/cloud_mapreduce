import os
from src.mapreduce import map_function, reduce_function, mapreduce


def read_data(file_path):
    with open(file_path, 'r') as file:
        data = file.readlines()
    return data


def main():
    # Set data file path
    data_file_path = os.path.join(os.path.dirname(__file__), '../data/AComp_Passenger_data_no_error.csv')

    # Read data
    input_data = read_data(data_file_path)

    # MapReduce
    result = mapreduce(input_data, map_function, reduce_function)

    # result
    max_flights = max(result.items(), key=lambda x: x[1])
    print(f"The passenger with the highest number of flights is {max_flights[0]} with {max_flights[1]} flights.")


if __name__ == "__main__":
    main()

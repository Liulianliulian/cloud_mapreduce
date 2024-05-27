
from collections import defaultdict
from multiprocessing import Pool


def map_function(record):
    # Analyze each record and return the passenger ID and number of flights (initially 1)
    passenger_id = record.split(',')[0]
    return (passenger_id, 1)


def reduce_function(mapped_data):
    # Summarize the number of flights with passenger IDs
    flight_counts = defaultdict(int)
    for passenger_id, count in mapped_data:
        flight_counts[passenger_id] += count
    return flight_counts


def mapreduce(input_data, map_func, reduce_func):
    # Using multiple processes for the Map phase
    with Pool() as pool:
        mapped_data = pool.map(map_func, input_data)

    # Reduce
    reduced_data = reduce_func(mapped_data)
    return reduced_data

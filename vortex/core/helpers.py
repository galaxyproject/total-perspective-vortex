import random
from functools import reduce


def get_dataset_size(dataset_association):
    return dataset_association.dataset.file_size


def sum_total(prev, current):
    return prev + current


def calculate_dataset_total(datasets):
    if datasets:
        return reduce(sum_total,
                      map(get_dataset_size, datasets))
    else:
        return 0


def input_size(job):
    return calculate_dataset_total(job.input_datasets)/1024.0/1024.0


def weighted_random_sampling(destinations):
    rankings = [(d.params.get('weight', 1) if d.params else 1) for d in destinations]
    return random.choices(destinations, weights=rankings, k=len(destinations))

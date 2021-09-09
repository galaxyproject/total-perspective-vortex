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

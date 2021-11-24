import random
from functools import reduce

GIGABYTES = 1024.0**3


def get_dataset_size(input_dataset):
    return input_dataset.dataset.dataset.file_size


def sum_total(prev, current):
    return prev + current


def calculate_dataset_total(datasets):
    if datasets:
        return reduce(sum_total,
                      map(get_dataset_size, datasets))
    else:
        return 0


def input_size(job):
    return float(calculate_dataset_total(job.input_datasets))/GIGABYTES


def weighted_random_sampling(destinations):
    if not destinations:
        return []
    rankings = [(d.params.get('weight', 1) if d.params else 1) for d in destinations]
    return random.choices(destinations, weights=rankings, k=len(destinations))


def __get_keys_from_dict(dl, keys_list):
    # This function builds a list using the keys from nest dictionaries
    # (copied from galaxyproject/galaxy lib/galaxy/jobs/dynamic_tool_destination.py)
    if isinstance(dl, dict):
        keys_list.extend(dl.keys())
        for x in dl.values():
            __get_keys_from_dict(x, keys_list)
    elif isinstance(dl, list):
        for x in dl:
            __get_keys_from_dict(x, keys_list)


def job_args_match(job, app, args):
    # Check whether a dictionary of arguments matches a job's parameters.  This code is
    # from galaxyproject/galaxy lib/galaxy/jobs/dynamic_tool_destination.py
    if not args or not isinstance(args, dict):
        return False
    options = job.get_param_values(app)
    matched = True
    # check if the args in the config file are available
    for arg in args:
        arg_dict = {arg: args[arg]}
        arg_keys_list = []
        __get_keys_from_dict(arg_dict, arg_keys_list)
        try:
            options_value = reduce(dict.__getitem__, arg_keys_list, options)
            arg_value = reduce(dict.__getitem__, arg_keys_list, arg_dict)
            if (arg_value != options_value):
                matched = False
        except KeyError:
            matched = False
    return matched

try:
    from galaxy.tool_util.version import parse_version
except ImportError:
    # Fallback to an older `packaging` version when Galaxy < 23.1.
    # If Galaxy is < 23.1 you need to have `packaging` in <= 21.3
    from packaging.version import parse as parse_version

import random
from functools import reduce
from galaxy import model

GIGABYTES = 1024.0**3


def get_dataset_size(dataset):
    return float(dataset.file_size)


def sum_total(prev, current):
    return prev + current


def calculate_dataset_total(datasets):
    if datasets:
        unique_datasets = {inp_ds.dataset.dataset.id: inp_ds.dataset.dataset for inp_ds in datasets if inp_ds.dataset}
        return reduce(sum_total, map(get_dataset_size, unique_datasets.values()), 0.0)
    else:
        return 0.0


def input_size(job):
    return calculate_dataset_total(job.input_datasets)/GIGABYTES


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


def concurrent_job_count_for_tool(app, tool, user=None):  # requires galaxy version >= 21.09
    # Match all tools, regardless of version. For example, a tool id such as "fastqc/0.1.0+galaxy1" is
    # turned into "fastqc/.*"
    tool_id_regex = '/'.join(tool.id.split('/')[:-1]) + '/.*' if '/' in tool.id else tool.id
    query = app.model.context.query(model.Job)
    if user:
        query = query.filter(model.Job.table.c.user_id == user.id)
    query = query.filter(model.Job.table.c.state.in_(['queued', 'running']))
    query = query.filter(model.Job.table.c.tool_id.regexp_match(tool_id_regex))
    return query.count()


def tag_values_match(entity, match_tag_values=[], exclude_tag_values=[]):
    # Return true if an entity has require/prefer/accept tags in the match_tags_values list
    # and no require/prefer/accept tags in the exclude_tag_values list
    return (
        all([any(entity.tpv_tags.filter(tag_value=tag_value)) for tag_value in match_tag_values])
        and not any([any(entity.tpv_tags.filter(tag_value=tag_value)) for tag_value in exclude_tag_values])
    )


def tool_version_eq(tool, version):
    return parse_version(tool.version) == parse_version(version)


def tool_version_lte(tool, version):
    return parse_version(tool.version) <= parse_version(version)


def tool_version_lt(tool, version):
    return parse_version(tool.version) < parse_version(version)


def tool_version_gte(tool, version):
    return parse_version(tool.version) >= parse_version(version)


def tool_version_gt(tool, version):
    return parse_version(tool.version) > parse_version(version)

from __future__ import annotations
import logging

from . import util

log = logging.getLogger(__name__)


class TPVConfigFormatter(object):

    def __init__(self, yaml_dict):
        self.yaml_dict = yaml_dict or {}

    @staticmethod
    def generic_key_sorter(keys_to_place_first):
        def sort_criteria(key):
            try:
                index = list(keys_to_place_first).index(key)
            except ValueError:
                index = len(keys_to_place_first)
            # sort by keys to place first, then potential toolshed tools, and finally alphabetically
            return (index, "/" not in key, key)
        return sort_criteria

    @staticmethod
    def multi_level_dict_sorter(dict_to_sort, sort_order):
        """
        Sorts a dict by given criteria, placing the given keys first.
        For example:
        TPVConfigFormatter.multi_level_dict_sorter({'b': 'again', 'a': 'hello', 'c': 'world'}, {'a': {}, 'b': {}})
        would return {'a': 'hello', 'b': 'again', 'c': 'world'}

        Also handles nested dictionaries.
        For example:
        TPVConfigFormatter.multi_level_dict_sorter(
            dict_to_sort = {
                'b': 'again',
                'a': 'hello',
                'c': {
                    'x': {
                        'x.1': 1,
                        'x.2': 2
                    }
                }
            },
            sort_order = {
                 'a': {},
                 'b': {},
                 'c': {
                    '*': {
                        'x.2': {}
                    }
                 }
            }
        )
        would return {'a': 'hello', 'b': 'again', 'c': {'x': {'x.2': 2, 'x.1': 1}}}

        The * denotes a special value which is used as a generic place holder when the key is not known,
        but nested keys still need to be sorted.

        :param dict_to_sort:
        :param sort_order_dict:
        :return:
        """
        if not sort_order:
            return dict_to_sort
        if isinstance(dict_to_sort, dict):
            sorted_keys = sorted(dict_to_sort or [], key=TPVConfigFormatter.generic_key_sorter(sort_order.keys()))
            return {key: TPVConfigFormatter.multi_level_dict_sorter(dict_to_sort.get(key),
                                                                    sort_order.get(key, {}) or sort_order.get('*', {}))
                    for key in sorted_keys}
        elif isinstance(dict_to_sort, list):
            return [TPVConfigFormatter.multi_level_dict_sorter(item, sort_order.get('*', []))
                    for item in dict_to_sort]
        else:
            return dict_to_sort

    def format(self):
        default_inherits = self.yaml_dict.get('global', {}).get('default_inherits') or 'default'

        basic_entity_sort_order = {
            'id': {},
            'if': {},
            'context': {},
            'gpus': {},
            'cores': {},
            'mem': {},
            'env': {
                '*': {}
            },
            'params': {
                '*': {}
            },
            'scheduling': {
                'require': {},
                'prefer': {},
                'accept': {},
                'reject': {},
            }
        }

        entity_with_rules_sort_order = {
            default_inherits: {},
            '*': {
                **basic_entity_sort_order,
                'rules': {
                    '*': basic_entity_sort_order
                }
            }
        }

        global_field_sort_order = {
            'global': {},
            'tools': entity_with_rules_sort_order,
            'roles': entity_with_rules_sort_order,
            'users': entity_with_rules_sort_order,
            'destinations': entity_with_rules_sort_order,
        }
        return self.multi_level_dict_sorter(self.yaml_dict, global_field_sort_order)

    @staticmethod
    def from_url_or_path(url_or_path: str):
        tpv_config = util.load_yaml_from_url_or_path(url_or_path)
        return TPVConfigFormatter(tpv_config)

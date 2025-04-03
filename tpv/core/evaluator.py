import abc
from typing import Any, Dict


class TPVCodeEvaluator(abc.ABC):

    @abc.abstractmethod
    def compile_code_block(self, code: str, as_f_string=False, exec_only=False):
        pass

    @abc.abstractmethod
    def eval_code_block(
        self, code: str, context: Dict[str, Any], as_f_string=False, exec_only=False
    ):
        pass

    def process_complex_property(
        self, prop_name: str, prop_val: Any, context: Dict[str, Any], func
    ):
        if isinstance(prop_val, str):
            return func(prop_name, prop_val, context)
        elif isinstance(prop_val, dict):
            evaluated_props = {
                key: self.process_complex_property(
                    f"{prop_name}_{key}", childprop, context, func
                )
                for key, childprop in prop_val.items()
            }
            return evaluated_props
        elif isinstance(prop_val, list):
            evaluated_props = [
                self.process_complex_property(
                    f"{prop_name}_{idx}", childprop, context, func
                )
                for idx, childprop in enumerate(prop_val)
            ]
            return evaluated_props
        else:
            return prop_val

    def compile_complex_property(self, prop):
        return self.process_complex_property(
            None,
            prop,
            None,
            lambda n, v, c: self.compile_code_block(v, as_f_string=True),
        )

    def evaluate_complex_property(self, prop, context: Dict[str, Any]):
        return self.process_complex_property(
            None,
            prop,
            context,
            lambda n, v, c: self.eval_code_block(v, c, as_f_string=True),
        )

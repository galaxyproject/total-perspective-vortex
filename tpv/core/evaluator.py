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

    def process_complex_property(self, prop: Any, context: Dict[str, Any], func):
        if isinstance(prop, str):
            return func(prop, context)
        elif isinstance(prop, dict):
            evaluated_props = {
                key: self.process_complex_property(childprop, context, func)
                for key, childprop in prop.items()
            }
            return evaluated_props
        elif isinstance(prop, list):
            evaluated_props = [
                self.process_complex_property(childprop, context, func)
                for childprop in prop
            ]
            return evaluated_props
        else:
            return prop

    def compile_complex_property(self, prop):
        return self.process_complex_property(
            prop, None, lambda p, c: self.compile_code_block(p, as_f_string=True)
        )

    def evaluate_complex_property(self, prop, context: Dict[str, Any]):
        return self.process_complex_property(
            prop,
            context,
            lambda p, c: self.eval_code_block(p, c, as_f_string=True),
        )

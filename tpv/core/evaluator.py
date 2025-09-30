import abc
from types import CodeType
from typing import Any, Callable, Dict, Tuple, Union


class TPVCodeEvaluator(abc.ABC):

    @abc.abstractmethod
    def compile_code_block(
        self, code: str, as_f_string: bool = False, exec_only: bool = False
    ) -> tuple[CodeType, Union[CodeType, None]]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def eval_code_block(
        self,
        code: str,
        context: Dict[str, Any],
        as_f_string: bool = False,
        exec_only: bool = False,
    ) -> Any:
        pass  # pragma: no cover

    def process_complex_property(
        self,
        prop_name: str,
        prop_val: Any,
        context: Dict[str, Any],
        func: Callable[[str, Any, Dict[str, Any]], Any],
    ) -> Any:
        if isinstance(prop_val, str):
            return func(prop_name, prop_val, context)
        elif isinstance(prop_val, dict):
            evaluated_props_dict = {
                key: self.process_complex_property(f"{prop_name}_{key}", childprop, context, func)
                for key, childprop in prop_val.items()
            }
            return evaluated_props_dict
        elif isinstance(prop_val, list):
            evaluated_props_list = [
                self.process_complex_property(f"{prop_name}_{idx}", childprop, context, func)
                for idx, childprop in enumerate(prop_val)
            ]
            return evaluated_props_list
        else:
            return prop_val

    def compile_complex_property(self, prop: Any) -> Any:
        return self.process_complex_property(
            "",
            prop,
            {},
            lambda n, v, c: self.compile_code_block(v, as_f_string=True),
        )

    def evaluate_complex_property(self, prop: Any, context: Dict[str, Any]) -> Any:
        return self.process_complex_property(
            "",
            prop,
            context,
            lambda n, v, c: self.eval_code_block(v, c, as_f_string=True),
        )

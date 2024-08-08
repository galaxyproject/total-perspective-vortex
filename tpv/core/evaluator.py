import abc


class TPVCodeBlockInterface(abc.ABC):

    @abc.abstractmethod
    def compile_code_block(self, code, as_f_string=False, exec_only=False):
        pass

    @abc.abstractmethod
    def eval_code_block(self, code, context, as_f_string=False, exec_only=False):
        pass

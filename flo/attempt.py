from dataclasses import dataclass
from functools import partial
from logging import Logger

from flo.lamb import FunctionOrLambda, as_fcn, Function, R, UNASSIGNED
from typing import *

R1 = TypeVar('R1')


@dataclass(init=False)
class Attempt(Generic[R]):
    _result: R
    _exception: Exception

    def __init__(self, *, result: R = UNASSIGNED, exception: Exception = UNASSIGNED):
        self._result = result
        self._exception = exception

    def result(self) -> R:
        if self._result is UNASSIGNED:
            raise self._exception
        return self._result

    def exception(self) -> Optional[Exception]:
        if self._exception is UNASSIGNED:
            return None
        return self._exception

    def succeeded(self) -> bool:
        return self._result is not UNASSIGNED

    def failed(self) -> bool:
        return self._exception is not UNASSIGNED

    def and_then(self, fcn: FunctionOrLambda, **kwargs) -> 'Attempt[R1]':
        if self.succeeded():
            return try_(fcn, self.result(), **kwargs)
        else:
            return self

    def or_else(self, default: R = None, log: Logger = None) -> R:
        if self.succeeded():
            return self.result()
        if log is not None:
            log.exception(str(self._exception), exc_info=self._exception)
        return default


def try_(fcn: FunctionOrLambda, *args, **kwargs) -> Attempt[R]:
    f = as_fcn(fcn)
    try:
        return Attempt(result=f(*args, **kwargs))
    except Exception as e:
        return Attempt(exception=e)


def as_try(fcn: FunctionOrLambda, *args, **kwargs) -> Function:
    return partial(try_, fcn, *args, **kwargs)

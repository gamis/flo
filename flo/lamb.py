"""
Cleaner syntax for lambda-like expressions

Usage:

>>> from flo import start as _, for_each
>>> filenames_starting_with_i = filter(_.startswith('i').f,['egor','ivan','elanor'])
or
>>> filenames_starting_with_i = for_each('egor','ivan','elanor').where(_.startswith('i')).to(list)
instead of
>>> filenames_starting_with_i = filter(lambda i: i.startswith('i'),['egor','ivan','elanor'])

"""
import inspect
import re
from functools import partial
from operator import attrgetter, itemgetter
from typing import TypeVar, Callable, Union, Any, Mapping, Container, Tuple

I = TypeVar('I')
R = TypeVar('R')
R1 = TypeVar('R1')
UNASSIGNED = object()

Function = Callable[[I], R]
FunctionOrLambda = Union[Function, 'Lambda']


class Lambda(object):
    _parent: 'Lambda'
    _accessor: Function
    name_: str

    def __init__(self, parent: 'Lambda', accessor: Function, name_: str):
        self._parent = parent
        self._accessor = accessor
        self.name_ = name_

    def __getattr__(self, item: str) -> 'Lambda':
        return Lambda(self, attrgetter(item), f'.{item}')

    def __getitem__(self, item) -> 'Lambda':
        return Lambda(self, itemgetter(item), f'[{item}]')

    def __call__(self, *args, **kwargs) -> 'Lambda':
        argstr = ','.join(map(repr, args))
        kwargstr = ','.join((f'{k}={repr(v)}' for k, v in kwargs.items()))
        name = f'({argstr}{", " if argstr and kwargstr else ""}{kwargstr})'
        return Lambda(self, partial(_call, args=args, kwargs=kwargs), name)

    def __eq__(self, other) -> Function:
        return Lambda(self, lambda e: e == other, '==' + str(other)).f

    def __ne__(self, other) -> Function:
        return Lambda(self, lambda e: e != other, '!=' + str(other)).f

    def __lt__(self, other) -> Function:
        return Lambda(self, lambda e: e < other, '<' + str(other)).f

    def __le__(self, other) -> Function:
        return Lambda(self, lambda e: e <= other, '<=' + str(other)).f

    def __gt__(self, other) -> Function:
        return Lambda(self, lambda e: e > other, '>' + str(other)).f

    def __ge__(self, other) -> Function:
        return Lambda(self, lambda e: e >= other, '>=' + str(other)).f

    def __add__(self, other) -> Function:
        return Lambda(self, lambda e: e + other, '+' + str(other)).f

    def __sub__(self, other) -> Function:
        return Lambda(self, lambda e: e - other, '-' + str(other)).f

    def __truediv__(self, other) -> Function:
        return Lambda(self, lambda e: e / other, '/' + str(other)).f

    def __mod__(self, other) -> Function:
        return Lambda(self, lambda e: e % other, '%' + str(other)).f

    def __invert__(self) -> 'Lambda':
        return self.negate()

    def negate(self) -> 'Lambda':
        return Lambda(self, lambda e: not e, ' not')

    def apply(self, fcn: Function, **kwargs) -> 'Lambda':
        return Lambda(self, partial(fcn, **kwargs), f".apply({fcn.__name__}{kwarg_str(kwargs)})")

    def apply_(self, fcn: Function) -> Function:
        return self.apply(fcn).f

    def has(self, item) -> 'Lambda':
        return Lambda(self, lambda e: (e is not None) and (item in e), f' contains {repr(item)}')

    def is_none(self) -> 'Lambda':
        return Lambda(self, lambda e: e is None, f' is None')

    def not_none(self) -> 'Lambda':
        return Lambda(self, lambda e: e is not None, f' is not None')

    def between(self, left: Union[I, str, tuple, list], right: I = UNASSIGNED, *,
                left_inclusive: bool = True, right_inclusive: bool = False) -> 'Lambda':
        """
        Default is left <= e < right, which can be modified with left_inclusive and right_inclusive.
        Alternatively, can specify a string in mathematical interval form,
        e.g., "(1,3]" is equiv to 1 < e <= 3.
        Alternatively, passing a two-element list is like left_inclusive=right_inclusive=True
        and a two-element tuple is like left_inclusive=right_inclusive=False
        Examples:
            e_.between(2,10) is equivalent to 2 <= e <10 and e_.between("[2,10)")
            e_.between(2,10,left_inclusive=False) is equiv to 2 < e < 10
            e_.between(2,10,right_inclusive=True) is equiv to 2 <= 3 <= 10
            e_.between("[2,10]") is the same as e_.between([2,10])
                is the same as e_.between(2,10,left_inclusive=True, right_inclusive=True)
                is the same as 2 <= e <= 10
            e_.between("(2,10)") is the same as e_.between((2,10))
                is the same as e_.between(2,10,left_inclusive=False, right_inclusive=False)
                is the same as 2 < e < 10
            e_.between("(2,10]") is the same as e_.between(2,10,left_inclusive=False, right_inclusive=True)
                is the same as 2 < e <= 10
        """
        left, right, left_inclusive, right_inclusive = interpret_between(left, right, left_inclusive, right_inclusive)

        if left_inclusive:
            if right_inclusive:
                return Lambda(self, lambda e: left <= e <= right, f' in [{left},{right}]')
            else:
                return Lambda(self, lambda e: left <= e < right, f' in [{left},{right})')
        else:
            if right_inclusive:
                return Lambda(self, lambda e: left < e <= right, f' in ({left},{right}]')
            else:
                return Lambda(self, lambda e: left < e < right, f' in ({left},{right})')

    def truthy(self) -> 'Lambda':
        return self.apply(bool)

    def falsey(self) -> 'Lambda':
        return self.apply(bool).negate()

    def matches(self, regex, case: bool = False) -> 'Lambda':
        if isinstance(regex, str):
            regex = re.compile(regex, re.IGNORECASE if not case else 0)

        def fcn(e: str) -> bool:
            return regex.match(e) is not None

        return Lambda(self, fcn, str(regex))

    def sub_(self, regex, rpl: str) -> 'Lambda':
        return Lambda(self, lambda s: re.sub(regex, rpl, s), f'{{{regex}->{rpl}}}')

    def is_in(self, container: Container) -> 'Lambda':
        return Lambda(self, lambda e: e in container, ' in ' + str(container))

    def not_in(self, container: Container) -> 'Lambda':
        return Lambda(self, lambda e: e not in container, ' not in ' + str(container))

    def instanceof(self, *types: type) -> 'Lambda':
        return Lambda(self, lambda e: isinstance(e, types), f'.instanceof({", ".join(t.__name__ for t in types)})')

    def invoke(self, instance: I, **kwargs) -> R:
        if self._parent is not None and self._parent._accessor is not None:
            p = self._parent.invoke(instance, **kwargs)
            return self._accessor(p)
        if self._accessor:
            return self._accessor(instance)
        return instance

    def as_try(self) -> Function:
        """Returns this Lambda as a function that will attempt execution
        and return an Attempt object with either a result or an exception"""
        from flo.attempt import try_
        return partial(try_, self.f)

    @property
    def f(self):
        return self.invoke

    def __repr__(self) -> str:
        if self._parent is None:
            return self.name_ or 'unknown'
        r = repr(self._parent) + self.name_
        return r

    def __str__(self) -> str:
        return repr(self)

    def __getstate__(self) -> Mapping[str, Any]:
        return self.__dict__

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self._parent = state['_parent']
        self._accessor = state['_accessor']
        self.name_ = state['name_']


start = Lambda(name_='_', parent=None, accessor=None)


def interpret_between(left: I, right: I, left_inclusive: bool, right_inclusive: bool) -> Tuple[I, I, bool, bool]:
    if right is UNASSIGNED:
        # interpret left
        if isinstance(left, str):
            m = re.fullmatch(r"\s*([[(])\s*([0-9._]+)\s*,\s*([0-9._]+)\s*([])])\s*", left)
            if not m:
                raise ValueError(f"Unrecognized between spec '{left}'")
            left_brace, left_str, right_str, right_brace = m.groups()
            left_inclusive = left_brace == '['
            right_inclusive = right_brace == ']'
            left = float(left_str)
            right = float(right_str)
        elif isinstance(left, tuple):
            left_inclusive = False
            right_inclusive = False
            left, right = left
        elif isinstance(left, list):
            left_inclusive = True
            right_inclusive = True
            left, right = left
        else:
            raise ValueError("right not specified but first argument isn't str, list, or tuple")
    return left, right, left_inclusive, right_inclusive


def as_fcn(function_or_lambda: FunctionOrLambda) -> Function:
    if isinstance(function_or_lambda, Lambda):
        return function_or_lambda.f
    if callable(function_or_lambda):
        return function_or_lambda
    return constant(function_or_lambda).f


def as_name_function(f: FunctionOrLambda) -> Tuple[str, Callable]:
    if isinstance(f, Lambda):
        return str(f), f.f
    return name_for(f), f


def name_for(function_or_lambda: FunctionOrLambda) -> str:
    if isinstance(function_or_lambda, Lambda):
        return str(function_or_lambda)
    if callable(function_or_lambda):
        name = getattr(function_or_lambda, '__name__')
        if name == '<lambda>':
            name = inspect.getsource(function_or_lambda)[:-1]
        elif name == 'invoke':
            name = str(getattr(function_or_lambda, '__self__'))
        return name
    return str(function_or_lambda)


def constant(value) -> Lambda:
    return Lambda(parent=None, accessor=lambda e: value, name_=str(value))


def star(fcn: Callable[..., R]) -> Function:
    """
    Takes a function that accepts multiple arguments
    and returns a function that accepts those arguments as a single tuple
    :param fcn: that takes *args and **kwargs
    :return: A function that takes argtuple and **kwargs and calls fcn(*argtuple,**kwargs)
    """

    return partial(_call_star, fcn)


def _call_star(fcn: Callable[..., R], argtuple, **kwargs) -> Function:
    return fcn(*argtuple, **kwargs)


def _call(f, args, kwargs):
    return f(*args, **kwargs)


e_ = start


def kwarg_str(kwargs: Mapping[str, Any]) -> str:
    if kwargs:
        return '(_, ' + ', '.join(f"{k}={repr(v)}" for k, v in kwargs.items()) + ')'
    else:
        return ""

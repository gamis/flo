"""
Code related to reusable pipelines-- sequences of map/filter/reduce methods to apply to iterables
"""
from typing import *
from itertools import chain, dropwhile, takewhile
from .lamb import Lambda, as_name_function, e_, as_fcn, kwarg_str

_E = TypeVar('_E')  # Iterator element type
_R = TypeVar('_R')  # return type
_R1 = TypeVar('_R1')  # return type
_C = TypeVar('_C')  # return type

Function = Callable[[_E], _R]

Mapper = Union[Lambda, Callable[[_E], _R1]]
Filter = Union[Lambda, Callable[[_E], bool]]
Reducer = Callable[[_C, _E], _C]
Collector = Union[Lambda, Callable[[Iterable[_E]], _C]]
Transform = Callable[[Iterable[_E]], Iterable[_R]]
TerminalTranform = Callable[[Iterable[_E]], _C]


def pipeline(label: str = '') -> 'Pipeline[_E,_R]':
    return Pipeline(label, ())


class Pipeline(Generic[_E, _R]):
    label: str
    steps: Tuple[Transform]

    def __init__(self, label: str, steps: Tuple[Transform]):
        self.label = label
        self.steps = steps

    def _with(self, additional_label: str, additional_step: Transform) -> 'Pipeline[_E,_R1]':
        return Pipeline(f"{self.label} {additional_label}", tuple((*self.steps, additional_step)))

    def map(self, mapper: Mapper, **kwargs) -> 'Pipeline[_E,_R1]':
        """Apply mapper to each element of the iterable.
        Optional kwargs are passed onto mapper after the target element
        :param: mapper Any Lambda or function that takes an element and returns something new of type _R
        :returns A new pipeine that adds this mapper
        """
        label, f = as_name_function(mapper)
        return self._with(f"* {label}{kwarg_str(kwargs)}", lambda it: (f(e, **kwargs) for e in it))

    def filter(self, true_condition: Filter, **kwargs) -> 'Pipeline[_E,_E]':
        """Filter elements of the iterable to only those that pass this true_condition test.
        Optional kwargs are passed onto mapper after the target element
        :param: true_condition Any Lambda or function that takes an element and returns something boolean-like
        :returns A new pipeline that adds this filter
        """
        label, f = as_name_function(true_condition)
        return self._with(f"/ {label.lstrip()}{kwarg_str(kwargs)}", lambda it: (e for e in it if f(e, **kwargs)))

    def exclude(self, excluded_condition: Filter, **kwargs) -> 'Pipeline[_E,_E]':
        """Filter out elements of the iterable that pass this condition.
        Optional kwargs are passed onto mapper after the target element
        :param: excluded_condition Any Lambda or function that takes an element and returns something boolean-like
        :returns A new pipeline that adds this filter
        """
        label, f = as_name_function(excluded_condition)
        return self._with(f"/ not {label.lstrip()}{kwarg_str(kwargs)}", lambda it: (e for e in it if not f(e, **kwargs)))

    def flatten(self) -> 'Pipeline[Iterable[_E],_E]':
        """
        Flattens an iterator of iterators into an iterator of elements
        """
        return self._with("* flatten", lambda it: (e for ee in it for e in ee))

    def flatmap(self, mapper: Mapper, **kwargs) -> 'Pipeline[_E,_R]':
        """Shorthand for self.map(mapper,**kwargs).flatten()"""
        return self.map(mapper, **kwargs).flatten()

    def zip_with(self, it: Iterable[_R]) -> 'Pipeline[_E,Tuple[_E,_R]]':
        return self._with(f"zip({type(it)}[{len(it) if isinstance(it, Sized) else '?'}]", lambda e: zip(e, it))

    def chain(self, it: Iterable[_E]) -> 'Pipeline[_E,_E]':
        return self._with(f"chain({type(it)}[{len(it) if isinstance(it, Sized) else '?'}]", lambda e: chain(e, it))

    def dropwhile(self, condition: Filter, **kwargs) -> 'Pipeline[_E,_E]':
        label, f = as_name_function(condition)
        return self._with(f"dropwhile({label.lstrip()}{kwarg_str(kwargs)}", lambda it: dropwhile(f, it))

    def takewhile(self, condition: Filter, **kwargs) -> 'Pipeline[_E,_E]':
        label, f = as_name_function(condition)
        return self._with(f"takewhile({label.lstrip()}{kwarg_str(kwargs)}", lambda it: takewhile(f, it))

    def collect(self, collector: Collector, *collectors: Collector, **kwargs) -> 'TerminatedPipeline[_E,_R]':
        """Drain the elements from the iterable into the collector function(s).
        Optional kwargs are passed onto collector after the target element
        :param collector A lambda or function that takes the entire iterable as an argument and returns a single _R
            Multiple transformers can be passed. collector[n+1] will just take the output of collector[n] as an input.
            E.g., you may want it.collect(list,pd.concat) to first drain the iterable into a list and then
            pass that list into pandas concat function.
            kwargs are only passed to the first reducer
        :return TerminatedPipeline with this collector at the end
        """
        label, f = as_name_function(collector)
        if kwargs:
            label += kwarg_str(kwargs)
        fcn = e_.apply(f, **kwargs)
        for c in collectors:
            fcn = fcn.apply(as_fcn(c))
        return TerminatedPipeline(self, f"> {label}", fcn.f)

    def apply(self, it: Iterable[_E]) -> Iterator[_R]:
        n = -1
        try:
            transformed = it
            for step in self.steps:
                transformed = step(transformed)
            for n, e in enumerate(transformed):
                yield e
        except Exception as ee:
            raise type(ee)(f"Failed to iterate to item {n + 1} in flo.Pipeline({self.label})", ee)

    def __call__(self, it: Iterable[_E]) -> Iterator[_R]:
        return self.apply(it)

    where = filter
    without = exclude
    filter_in = filter
    filter_out = exclude
    to = collect

    def __gt__(self, collector: Collector) -> '_R':
        return self.collect(collector)

    def __truediv__(self, true_condition: Filter) -> 'Pipeline[_E]':
        return self.filter(true_condition)

    def __mul__(self, mapper: Mapper) -> 'Pipeline[_R]':
        return self.map(mapper)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.label})"

    def __str__(self) -> str:
        return self.label


class TerminatedPipeline(Generic[_E, _C]):
    label: str
    pipeline: Pipeline[_E, _R]
    transform: TerminalTranform

    def __init__(self, pipeline: Pipeline[_E, _R], additional_label: str, transform: TerminalTranform):
        self.label = f"{pipeline.label} {additional_label}"
        self.pipeline = pipeline
        self.transform = transform

    def __call__(self, it: Iterable[_E]) -> _C:
        piped = self.pipeline(it)
        return self.transform(piped)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.label})"

    def __str__(self) -> str:
        return self.label


def index(it: Iterator[_E], key: Mapper, **kwargs) -> Mapping[_R, Sequence[_E]]:
    label, f = as_name_function(key)
    kwargstr = kwarg_str(kwargs)
    d = {}
    e = None
    try:
        for e in it:
            k = f(e, **kwargs)
            v = d.get(k)
            if v is None:
                v = []
                d[k] = v
            v.append(e)
    except Exception as ee:
        raise type(ee)(f"Failed to index {it} with {label}({e}{kwargstr})", ee)

    return d


def unique_index(it: Iterator[_E], key: Mapper, **kwargs) -> Mapping[_R, _E]:
    label, f = as_name_function(key)
    kwargstr = kwarg_str(kwargs)
    d = {}
    e = None
    try:
        for e in it:
            k = f(e, **kwargs)
            if k in d:
                raise KeyError(f"Non-unique keys for {label}({e}{kwargstr}) = {k}")
            d[k] = e
    except Exception as ee:
        raise type(ee)(f"Failed to index {it} with {label}({e}{kwargstr})", ee)

    return d

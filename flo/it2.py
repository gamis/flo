from typing import *

from .lamb import as_name_function, kwarg_str
from .pipeline import Pipeline, Mapper, Filter, Collector, pipeline

_E = TypeVar('_E')  # Iterator element type
_E1 = TypeVar('_E1')
_R = TypeVar('_R')  # return type
_R1 = TypeVar('_R1')  # return type
_C = TypeVar('_C')  # return type


def from_(it: Iterable[_E], label: str = None) -> 'It[_E]':
    if not label:
        lenstr = f"[{len(it)}]" if isinstance(it, Sized) else ""
        label = f"{type(it).__name__}{lenstr}"
    return It(it, pipeline(label))


def for_each(*elements: _E) -> 'It[_E]':
    return from_(elements, label=str(elements) if len(elements) < 10 else None)


class It(Generic[_R]):
    _src: Iterable[_E]
    _pipeline: Pipeline[_E, _R]

    def __init__(self, src: Iterable[_E], pipeline: Pipeline[_E, _R]):
        self._src = src
        self._pipeline = pipeline

    def _with(self, pipeline: Pipeline[_E, _R1]) -> 'It[_R1]':
        return It(self._src, pipeline)

    def map(self, mapper: Mapper, **kwargs) -> 'It[_R]':
        """Apply mapper to each element of the iterable.
        Optional kwargs are passed onto mapper after the target element
        :param: mapper Any Lambda or function that takes an element and returns something new of type _R
        :returns A new iterable of element type _R
        """
        return self._with(self._pipeline.map(mapper, **kwargs))

    def filter(self, true_condition: Filter, **kwargs) -> 'It[_E]':
        """Filter elements of the iterable to only those that pass this true_condition test.
        Optional kwargs are passed onto mapper after the target element
        :param: true_condition Any Lambda or function that takes an element and returns something boolean-like
        :returns A new iterable of element type _E that contains only elements that passed the test
        """
        return self._with(self._pipeline.filter(true_condition, **kwargs))

    def __iter__(self) -> Iterator[_E]:
        return self._pipeline.apply(self._src)

    def exclude(self, excluded_condition: Filter, **kwargs) -> 'It[_E]':
        """Filter out elements of the iterable that pass this condition.
        Optional kwargs are passed onto mapper after the target element
        :param: excluded_condition Any Lambda or function that takes an element and returns something boolean-like
        :returns A new iterable that adds this filter
        """
        return self._with(self._pipeline.exclude(excluded_condition, **kwargs))

    def flatten(self) -> 'It[_R]':
        """
        Flattens an iterator of iterators into an iterator of elements
        """
        return self._with(self._pipeline.flatten())

    def flatmap(self, mapper: Mapper, **kwargs) -> 'It[_R]':
        """Shorthand for self.map(mapper,**kwargs).flatten()"""
        return self._with(self._pipeline.flatmap(mapper, **kwargs))

    def zip_with(self, it: Iterable[_E1]) -> 'It[Tuple[_E,_E1]]':
        return self._with(self._pipeline.zip_with(it))

    def chain(self, it: Iterable[_E]) -> 'It[_E]':
        return self._with(self._pipeline.chain(it))

    def dropwhile(self, condition: Filter, **kwargs) -> 'It[_E]':
        return self._with(self._pipeline.dropwhile(condition, **kwargs))

    def takewhile(self, condition: Filter, **kwargs) -> 'It[_E]':
        return self._with(self._pipeline.takewhile(condition, **kwargs))

    def cache(self) -> 'It[_E]':
        """
        Caches the results of the iterator at this point, in memory,
        such that the iterator can be rerun without recomputing steps.
        """
        return It(CachingIt(self), pipeline(str(self)))

    def collect(self, collector: Collector, *collectors: Collector, **kwargs) -> '_C':
        """Drain the elements from the iterable into the collector function(s).
        Optional kwargs are passed onto collector after the target element
        :param collector A lambda or function that takes the entire iterable as an argument and returns a single _R
            Multiple transformers can be passed. collector[n+1] will just take the output of collector[n] as an input.
            E.g., you may want it.collect(list,pd.concat) to first drain the iterable into a list and then
            pass that list into pandas concat function.
            kwargs are only passed to the first reducer
        """
        pipeline = self._pipeline.collect(collector, *collectors, **kwargs)
        return pipeline(self._src)

    where = filter
    to = collect

    def __gt__(self, collector: Collector) -> '_C':
        return self.collect(collector)

    def __truediv__(self, true_condition: Filter) -> 'It[_E]':
        return self.filter(true_condition)

    def __mul__(self, mapper: Mapper) -> 'It[_R]':
        return self.map(mapper)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._pipeline.label})"

    def __str__(self) -> str:
        return self._pipeline.label


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


class CachingIt(Iterable[_E]):
    _src: Iterable[_E]
    _cache: List[_E]

    def __init__(self, src: It[_E]):
        self._src = src
        self._cache = []

    def __iter__(self) -> Iterator[_E]:
        if self._cache:
            for e in self._cache:
                yield e
            return
        try:
            for e in self._src:
                self._cache.append(e)
                yield e
        except:
            self._cache.clear()

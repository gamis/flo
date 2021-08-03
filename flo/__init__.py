"""
flo - A python package for fluent programming styles

`flo` let's you embrace some of the benefits of functional programming in a way that flows more naturally.

Quick motivating example:

Say I have a list
>>> mylist = ['pretty','cool','items', 'kiddo']
If I want to get only the items with contaiing an 'e' and then index them by their first letter, you can do:
>>>{v[0]:v for v in mylist if 'e' in v}
That's not too bad. But what if you want to apply some mapping first, e.g., Make it all upper case.
Then it starts getting ugly:
>>>{v[0]:v for v in (vv.upper() for vv in mylist) if 'E' in v}
Of course, you could do this instead:
>>>{v[0]:v for v in map(str.upper,mylist) if 'E' in v}
Not terrible, but for me, the flow is weird. We start with the end result, then describe the beginning,
and then finish with the middle!

What if, instead, the code flowed according to the actual steps of execution:
>>> from flo import from_, for_each,  e_, unique_index
>>> from_(mylist).map(e_.upper()).filter(e_.has('E')).reduce(unique_index,key=e_[0])
Or alternatively, a more sql-like syntax
>>> from_(mylist).apply(e_.upper()).where(e_.has('E')).to(unique_index,key=e_[0])
Or even more terse
>>> for_each('pretty','cool','items', 'kiddo') * e_.upper() / e_.has('E') > list

This library is motivated by the API for Apache Spark.

Main entry points are:
flo.from_, which wraps your iterable into a flo.It with methods like map (aka, apply), filter (aka, where),
and reduce (aka, to and select).
flo.for_each, which takes your elements as individual arguments.
flo.Lambda, often abbreviated as `e_`, which allows for nearly everything you can do with a Python lambda
  but with cleaner syntax.

"""

from .lamb import Lambda, start, as_fcn, e_
from .check import each, that
from .it2 import from_, unique_index, for_each
from .attempt import try_, as_try, Attempt
from .parallel import pmap, tmap

try:
    from .pds import FloSeries as Series, FloDataFrame as DataFrame
except ImportError:
    pass

# flo

Utilities and APIs for fluent python coding.

`flo` let's you embrace some of the benefits of functional programming in a way that flows more naturally.

Quick motivating example:

Say I have a list:

```python
mylist = ['pretty','cool','items', 'kiddo']
```

If I want to get only the items containing an 'e' and then index them by their first letter, you could do:
```python
{v[0]: v for v in mylist if 'e' in v} 
```
That's not too bad. But what if you want to apply some mapping first, e. g., Make it all upper case. Then it starts getting ugly:
```python
{v[0]: v for v in (vv.upper() for vv in mylist) if 'E' in v} 
```
Of course, you could do this instead:
```python
{v[0]: v for v in map(str.upper, mylist) if 'E' in v} 
```
Not terrible, but for me, the flow is weird. We start with the end result, then describe the beginning, and then finish with the middle!

What if, instead, the code flowed according to the actual steps of execution:
```python
from flo import from_, for_each, e_, unique_index
(from_(mylist)
     .map(e_.upper())
     .filter(e_.has('E'))
     .collect(unique_index, key=e_[0]))
```
Or alternatively, a more sql-like syntax:
```python
(from_(mylist)
     .apply(e_.upper())
     .where(e_.has('E'))
     .to(unique_index, key=e_[0]))
```
Or even more terse
```python
for_each('pretty', 'cool', 'items', 'kiddo') * e_.upper() / e_.has('E') > list
```

This library was motivated in part by the API for Apache Spark.

Main entry points are:
* `flo.from_`, which wraps any `Iterable` into a `flo.It` with methods like `map` (aka, `apply`), `filter` (aka, `where`), and `collect` (aka, `to`). 
* Alternatively, `flo.for_each`, which takes your elements as individual arguments. 
* `flo.Lambda`, often abbreviated as `e_`, which allows for nearly everything you can do with a Python lambda but with cleaner syntax.
* `flo.DataFrame` and `flo.Series`, which provide easier ways to chain commands on `pandas` collections.

`flo` comes with some nice utilities to complement this api.

* `flo.parallel` provides a common, flo-like API for parallel processing and implementations backed by standard python `multiprocessing` and `threading` libraries as well as `joblib`.

  * `flo.pmap` and `flo.tmap`provide even more convenient entry points for quickly applying a function to an iterable in parallel.

* `flo.try_`, which calls a function and returns an `Attempt` containing either a successful result or a failure `Exception`.

* `flo.check` for quickly checking pre- and post-conditions.

  
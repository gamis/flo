import pandas as pd
import pytest
from pandas.testing import assert_series_equal, assert_frame_equal

from flo import e_
from flo.pds import monkey_patch_pandas

monkey_patch_pandas()


@pytest.mark.parametrize('series,value,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), 3, pd.Series([3, 3], index=[2, 4])),
                          (pd.Series([1, 2, 3, float('nan'), 3]), 3, pd.Series([3, 3], index=[2, 4], dtype=float)),
                          (pd.Series([1, 2, 3, float('nan'), 3]), None, pd.Series([float('nan')], index=[3])),
                          (pd.Series(["foo", "bar", None, "baz"]), "bar", pd.Series(["bar"], index=[1])),
                          (pd.Series(["foo", "bar", None, "baz"]), None, pd.Series([None], index=[2])),
                          (pd.Series(["foo"]), "you", pd.Series([], dtype=object))
                          ])
def test_only(series: pd.Series, value, expected: pd.Series):
    assert_series_equal(series.only(value), expected)
    assert_frame_equal(pd.DataFrame(dict(foo=series)).only(foo=value), pd.DataFrame(dict(foo=expected)))


@pytest.mark.parametrize('series,value,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), 3, pd.Series([1, 2, 4], index=[0, 1, 3])),
                          (pd.Series([1, 2, 3, float('nan'), 3]), 3,
                           pd.Series([1, 2, float('nan')], index=[0, 1, 3], dtype=float)),
                          (pd.Series([1, 2, 3, float('nan'), 3]), None,
                           pd.Series([1, 2, 3, 3], index=[0, 1, 2, 4], dtype=float)),
                          (pd.Series(["foo", "bar", None, "baz"]), "bar",
                           pd.Series(["foo", None, "baz"], index=[0, 2, 3])),
                          (pd.Series(["foo", "bar", None, "baz"]), None,
                           pd.Series(["foo", "bar", "baz"], index=[0, 1, 3])),
                          (pd.Series(["foo"]), "you", pd.Series(["foo"]))
                          ])
def test_ds_without(series: pd.Series, value, expected: pd.Series):
    assert_series_equal(series.without(value), expected)
    assert_frame_equal(pd.DataFrame(dict(foo=series)).without(foo=value), pd.DataFrame(dict(foo=expected)))


@pytest.mark.parametrize('series,condition,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), lambda e: e % 2 == 0, pd.Series([2, 4], index=[1, 3])),
                          (pd.Series(["foo", "bar", "baz"]), e_.startswith('b'),
                           pd.Series(["bar", "baz"], index=[1, 2]))
                          ])
def test_ds_only_if(series: pd.Series, condition, expected: pd.Series):
    assert_series_equal(series.only_if(condition), expected)
    assert_frame_equal(pd.DataFrame(dict(foo=series)).only_if(foo=condition), pd.DataFrame(dict(foo=expected)))


@pytest.mark.parametrize('series,value,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), {3}, pd.Series([3, 3], index=[2, 4])),
                          (pd.Series([1, 2, 3, 4, 3]), {7, 3, 18}, pd.Series([3, 3], index=[2, 4])),
                          (pd.Series([1, 2, 3, 4, 3]), {3, 4}, pd.Series([3, 4, 3], index=[2, 3, 4])),
                          (pd.Series([1, 2, 3, float('nan'), 3]), {3}, pd.Series([3, 3], index=[2, 4], dtype=float)),
                          (pd.Series([1, 2, 3, float('nan'), 3]), {float('nan')}, pd.Series([float('nan')], index=[3])),
                          (pd.Series(["foo", "bar", None, "baz"]), {"bar"}, pd.Series(["bar"], index=[1])),
                          (pd.Series(["foo", "bar", None, "baz"]), {None}, pd.Series([None], index=[2])),
                          (pd.Series(["foo"]), {"you"}, pd.Series([], dtype=object)),
                          (pd.Series(["foo"]), set(), pd.Series([], dtype=object))
                          ])
def test_ds_only_in(series: pd.Series, value, expected: pd.Series):
    assert_series_equal(series.only_in(value), expected)
    assert_frame_equal(pd.DataFrame(dict(foo=series)).only_in(foo=value), pd.DataFrame(dict(foo=expected)))


@pytest.mark.parametrize('series,value,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), {3}, pd.Series([1, 2, 4], index=[0, 1, 3])),
                          (pd.Series([1, 2, 3, float('nan'), 3]), {3},
                           pd.Series([1, 2, float('nan')], index=[0, 1, 3], dtype=float)),
                          (pd.Series([1, 2, 3, float('nan'), 3]), {float('nan')},
                           pd.Series([1, 2, 3, 3], index=[0, 1, 2, 4], dtype=float)),
                          (pd.Series(["foo", "bar", None, "baz"]), {"bar"},
                           pd.Series(["foo", None, "baz"], index=[0, 2, 3])),
                          (pd.Series(["foo", "bar", None, "baz"]), {None},
                           pd.Series(["foo", "bar", "baz"], index=[0, 1, 3])),
                          (pd.Series(["foo"]), {"you"}, pd.Series(["foo"]))
                          ])
def test_ds_not_in(series: pd.Series, value, expected: pd.Series):
    assert_series_equal(series.not_in(value), expected)
    assert_frame_equal(pd.DataFrame(dict(foo=series)).not_in(foo=value), pd.DataFrame(dict(foo=expected)))


@pytest.mark.parametrize('series,method,value,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), "gt_", 3, pd.Series([4], index=[3])),
                          (pd.Series([1, 2, 3, 4, 3]), "geq_", 3, pd.Series([3, 4, 3], index=[2, 3, 4])),
                          (pd.Series([1, 2, 3, 4, 3]), "lt_", 3, pd.Series([1, 2], index=[0, 1])),
                          (pd.Series([1, 2, 3, 4, 3]), "leq_", 3,
                           pd.Series([1, 2, 3, 3], index=[0, 1, 2, 4])),
                          ])
def test_ds_ops(series: pd.Series, method: str, value, expected: pd.Series):
    assert_series_equal(getattr(pd.Series, method)(series, value), expected)
    assert_frame_equal(getattr(pd.DataFrame, method)(pd.DataFrame(dict(foo=series)), foo=value), pd.DataFrame(dict(foo=expected)))


@pytest.mark.parametrize('series,kwargs,expected',
                         [(pd.Series([1, 2, 3, 4, 3]), {}, pd.Series([2, 1, 1, 1], index=[3, 1, 2, 4])),
                          ])
def test_ds_ops(series: pd.Series, kwargs, expected: pd.Series):
    assert_series_equal(series.count_distinct(**kwargs), expected)
    expected.index.rename('foo', inplace=True)
    assert_series_equal(pd.DataFrame(dict(foo=series)).count_by('foo', **kwargs), expected)


@pytest.mark.parametrize('left,right,left_inclusive,right_inclusive,element,expected',
                         [(2, 10, True, False, 3, [3]),
                          (2, 10, True, False, 2, [2]),
                          (2, 10, True, False, 10, []),
                          (2, 10, True, True, 10, [10]),
                          (2, 10, False, False, 2, [])
                          ])
def test_between(left, right, left_inclusive, right_inclusive, element, expected) -> None:
    assert_series_equal(pd.Series([element]).only_between(left, right, left_inclusive=left_inclusive, right_inclusive=right_inclusive), pd.Series(expected, dtype='int64'))
    if left_inclusive:
        if right_inclusive:
            interval = [left, right]
        else:
            interval = f"[{left},{right})"
    else:
        if right_inclusive:
            interval = f"({left},{right}]"
        else:
            interval = (left, right)
    assert_frame_equal(pd.DataFrame(dict(foo=[element])).only_between(foo=interval), pd.DataFrame(dict(foo=expected), dtype='int64'))

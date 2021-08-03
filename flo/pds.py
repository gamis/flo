""" Fluent api for pandas"""
from operator import eq, ne, gt, ge, lt, le
from typing import Set, Any, Callable, Literal, TypeVar

import pandas as pd

from flo.it2 import Filter
from flo.lamb import as_fcn, UNASSIGNED, interpret_between

SeriesOperator = Callable[[pd.Series, Any], pd.Series]
T = TypeVar('T')
op_by_str = {'==': eq, '<=': le, '!=': ne, '<': lt, '>': gt, '>=': ge}


def ds_op(self: pd.Series, op: SeriesOperator, value) -> pd.Series:
    return self[op(self, value)]


def ds_only(self: pd.Series, value) -> pd.Series:
    if value is None:
        return self[self.isnull()]
    return ds_op(self, eq, value)


def ds_without(self: pd.Series, value) -> pd.Series:
    if value is None:
        return self.dropna()
    return ds_op(self, ne, value)


# noinspection PyTypeChecker
def ds_only_if(self: pd.Series, condition: Filter) -> pd.Series:
    return self[self.apply(as_fcn(condition))]


def ds_only_in(self: pd.Series, included: Set) -> pd.Series:
    return self[self.isin(included)]


def ds_not_in(self: pd.Series, excluded: Set) -> pd.Series:
    return self[~self.isin(excluded)]


def ds_gt(self: pd.Series, value) -> pd.Series:
    return ds_op(self, gt, value)


def ds_geq(self: pd.Series, value) -> pd.Series:
    return ds_op(self, ge, value)


def ds_leq(self: pd.Series, value) -> pd.Series:
    return ds_op(self, le, value)


def ds_lt(self: pd.Series, value) -> pd.Series:
    return ds_op(self, lt, value)


def ds_between(self: pd.Series, left: T, right: T = UNASSIGNED, *,
               left_inclusive: bool = True, right_inclusive: bool = True) -> pd.Series:
    """
    Default is left <= e < right, which can be modified with left_inclusive and right_inclusive.
    Alternatively, can specify a string in mathematical interval form,
    e.g., "(1,3]" is equiv to 1 < e <= 3.
    Alternatively, passing a two-element list is like left_inclusive=right_inclusive=True
    and a two-element tuple is like left_inclusive=right_inclusive=False
    Examples:
        ds_between(2,10) is equivalent to 2 <= e <10 and ds_between("[2,10)")
        ds_between(2,10,left_inclusive=False) is equiv to 2 < e < 10
        ds_between(2,10,right_inclusive=True) is equiv to 2 <= 3 <= 10
        ds_between("[2,10]") is the same as ds_between([2,10])
            is the same as ds_between(2,10,left_inclusive=True, right_inclusive=True)
            is the same as 2 <= e <= 10
        ds_between("(2,10)") is the same as ds_between((2,10))
            is the same as ds_between(2,10,left_inclusive=False, right_inclusive=False)
            is the same as 2 < e < 10
        ds_between("(2,10]") is the same as ds_between(2,10,left_inclusive=False, right_inclusive=True)
            is the same as 2 < e <= 10
    """
    left, right, left_inclusive, right_inclusive = interpret_between(left, right, left_inclusive, right_inclusive)
    if left_inclusive != right_inclusive:
        # Series.between only supports fully open or fully closed intervals
        # so if half open, we need to just split it into two filters
        f = (self >= left) if left_inclusive else (self > left)
        f &= (self <= right) if right_inclusive else (self < right)
    else:
        f = self.between(left, right, inclusive=left_inclusive)
    return self[f]


def ds_count_distinct(self: pd.Series,
                      normalize: bool = False,
                      sort: bool = True,
                      ascending: bool = False,
                      bins=None,
                      dropna: bool = False) -> pd.Series:
    return self.value_counts(normalize=normalize, sort=sort, ascending=ascending, bins=bins, dropna=dropna)


def ds_print_lines(self: pd.Series) -> None:
    for v in self.values:
        print(v)


class FloSeries(pd.Series):
    only = ds_only
    without = ds_without
    only_if = ds_only_if
    only_in = ds_only_in
    not_in = ds_not_in
    gt_ = ds_gt
    geq_ = ds_geq
    leq_ = ds_leq
    lt_ = ds_lt
    only_between = ds_between
    count_distinct = ds_count_distinct
    print_lines = ds_print_lines


def monkey_patch_ds(series_type: type = pd.Series):
    if hasattr(series_type, 'flo_patched'):
        return
    series_type.flo_patched = True
    series_type.only = ds_only
    series_type.without = ds_without
    series_type.only_if = ds_only_if
    series_type.only_in = ds_only_in
    series_type.not_in = ds_not_in
    series_type.gt_ = ds_gt
    series_type.geq_ = ds_geq
    series_type.leq_ = ds_leq
    series_type.lt_ = ds_lt
    series_type.only_between = ds_between
    series_type.count_distinct = ds_count_distinct
    series_type.print_lines = ds_print_lines


def df_op(self: pd.DataFrame, op: Literal['==', '<=', '!=', '<', '>', '>='], **kwargs) -> pd.DataFrame:
    if all(isinstance(v, (str, float, int)) for v in kwargs.values()):
        # Oddly, a string query is faster than a series of filters
        # because pandas doesn't have to expose a python object at each step
        query = ' & '.join(f"{key}{op}{repr(val)}" for key, val in kwargs.items())
        return self.query(query)
    # else fall back to series of filters
    f = True
    opf = op_by_str[op]
    for key, val in kwargs.items():
        if val is None and op in {'==', '!='}:
            f &= self[key].isnull() if op == '==' else ~self[key].isnull()
        else:
            f &= opf(self[key], val)
    return self[f]


def df_only(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df_op(self, '==', **kwargs)


def df_without(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df_op(self, '!=', **kwargs)


def df_only_if(self: pd.DataFrame, **kwargs: Filter) -> pd.DataFrame:
    df = self
    for key, val in kwargs.items():
        df = df[df[key].apply(as_fcn(val))]
    return df


def df_only_in(self: pd.DataFrame, **kwargs: Set) -> pd.DataFrame:
    f = True
    for key, val in kwargs.items():
        f &= self[key].isin(val)
    return self[f]


def df_not_in(self: pd.DataFrame, **kwargs: Set) -> pd.DataFrame:
    f = True
    for key, val in kwargs.items():
        f &= ~self[key].isin(val)
    return self[f]


def df_gt(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df_op(self, '>', **kwargs)


def df_geq(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df_op(self, '>=', **kwargs)


def df_leq(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df_op(self, '<=', **kwargs)


def df_lt(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df_op(self, '<', **kwargs)


def df_between(self: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Returns df only including rows where df[col].between(val[0],val[1])
    for each col:val in kwargs.
    if val is a list, we presume a closed interval
    if val is a tuple, we presume an open interval
    if val is a str, we will parse, eg, "[2,5)" as >=2 and <5.
    E.g., df_between(df,col1=(1,8)) <==> df[df[col1].between(1,8,inclusive=False)]
          df_between(df,col1=[1,8]) <==> df[df[col1].between(1,8,inclusive=True)]
          df_between(df,col1=(1,8),col2=[20,100]) <==> df[df[col1].between(1,8,inclusive=False) &
                                                          df[col2].between(20,100,inclusive=True)]

    """
    f = True
    for col, interval in kwargs.items():
        if isinstance(interval, str):
            left, right, left_incl, right_incl = interpret_between(interval, UNASSIGNED, False, False)
            if left_incl:
                f &= self[col].ge(left)
            else:
                f &= self[col].gt(left)
            if right_incl:
                f &= self[col].le(right)
            else:
                f &= self[col].lt(right)
        else:
            left, right = interval
            inclusive = isinstance(interval, list)
            f &= self[col].between(left, right, inclusive=inclusive)
    return self[f]


def df_count_by(self: pd.DataFrame, *args: str,
                normalize: bool = False,
                sort: bool = True,
                ascending: bool = False,
                dropna: bool = False) -> pd.DataFrame:
    g = self.groupby(list(args))
    c = g.count() if dropna else g.size()
    if normalize:
        # noinspection PyArgumentList
        c /= c.sum()
    if sort:
        c = c.sort_values(ascending=ascending)
    return c


class FloDataFrame(pd.DataFrame):
    only = df_only
    without = df_without
    only_if = df_only_if
    only_in = df_only_in
    not_in = df_not_in
    only_between = df_between
    gt_ = df_gt
    geq_ = df_geq
    leq_ = df_leq
    lt_ = df_lt
    count_by = df_count_by


def monkey_patch_df(frame_type: type = pd.DataFrame):
    if hasattr(frame_type, 'flo_patched'):
        return
    frame_type.flo_patched = True
    frame_type.only = df_only
    frame_type.without = df_without
    frame_type.only_if = df_only_if
    frame_type.only_in = df_only_in
    frame_type.not_in = df_not_in
    frame_type.only_between = df_between
    frame_type.gt_ = df_gt
    frame_type.geq_ = df_geq
    frame_type.leq_ = df_leq
    frame_type.lt_ = df_lt
    frame_type.count_by = df_count_by


def monkey_patch_pandas(df_type=pd.DataFrame, ds_type=pd.Series) -> None:
    monkey_patch_df(df_type)
    monkey_patch_ds(ds_type)

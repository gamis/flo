import random

import pytest

from flo.it2 import from_, for_each, unique_index, index

from flo.lamb import e_


def test_usage_examples() -> None:
    mylist = ['pretty', 'cool', 'items', 'kiddo']
    expected = {'P': 'PRETTY', 'I': 'ITEMS'}
    expected_repr = "It(list[4] * _.upper() / _ contains 'E')"

    it = from_(mylist).map(e_.upper()).filter(e_.has('E'))

    assert repr(it) == expected_repr
    assert it.collect(unique_index, key=e_[0]) == expected

    it = from_(mylist).map(e_.upper()).where(e_.has('E'))
    assert it.to(unique_index, key=e_[0]) == expected

    assert (for_each('pretty', 'cool', 'items', 'kiddo') * e_.upper() / e_.has('E') > list) == list(expected.values())


@pytest.mark.parametrize('src,mapper,rep,expected',
                         [([3, 4], str, "It(list[2] * str)", ["3", "4"]),
                          ([3, 4], e_ + 1, "It(list[2] * _+1)", [4, 5])])
def test_map(src, mapper, rep, expected):
    it = from_(src).map(mapper)
    assert repr(it) == rep
    assert it.to(list) == expected


@pytest.mark.parametrize('src,mapper,rep,expected',
                         [(["3 1", "a", "b c d"], str.split, "It(list[3] * split * flatten)", ["3", "1", "a", "b", "c", "d"]),
                          (["3,1", "a", "b,c,d"], e_.split(','), "It(list[3] * _.split(',') * flatten)", ["3", "1", "a", "b", "c", "d"]),
                          (["3,1", "a", "b,c,d"], (str.split, {'sep': ','}), "It(list[3] * split(_, sep=',') * flatten)", ["3", "1", "a", "b", "c", "d"])
                          ])
def test_flatmap(src, mapper, rep, expected):
    if isinstance(mapper, tuple):
        mapper, kwargs = mapper
    else:
        kwargs = {}
    it = from_(src).flatmap(mapper, **kwargs)
    assert repr(it) == rep
    assert it.to(list) == expected


@pytest.mark.parametrize('src,f,rep,expected',
                         [([3, 4], e_ > 3, "It(list[2] / _>3)", [4]),
                          ([3, 4], e_.apply(str).not_in({'2'}), "It(list[2] / _.apply(str) not in {'2'})", [3, 4])])
def test_filter(src, f, rep, expected):
    it = from_(src).filter(f)
    assert repr(it) == rep
    assert it.to(list) == expected


@pytest.mark.parametrize('src,f,rep,expected',
                         [([3, 4], e_ > 3, "It(list[2] / not _>3)", [3]),
                          ([3, 4], e_.apply(str).not_in({'2'}), "It(list[2] / not _.apply(str) not in {'2'})", [])])
def test_exclude(src, f, rep, expected):
    it = from_(src).exclude(f)
    assert repr(it) == rep
    assert it.to(list) == expected


@pytest.mark.parametrize('src,f,expected',
                         [([3, 4], (set,), {3, 4}),
                          ([3, 4], (list, str), '[3, 4]')
                          ])
def test_collect(src, f, expected):
    assert from_(src).collect(*f) == expected


@pytest.mark.parametrize('src,expected',
                         [([[1, 2, 3], [4]], [1, 2, 3, 4]),
                          ([[1, 2, 3], [], [4]], [1, 2, 3, 4]),
                          ([[1, 2, 3], None, [4]], TypeError),
                          ([[1, 2, 3], 15, [4]], TypeError),
                          ])
def test_flatten(src: list, expected: list):
    try:
        assert from_(src).flatten().to(list) == expected
    except TypeError:
        assert expected == TypeError


def test_zip_with():
    assert for_each(1, 2, 3).zip_with('abc').to(list) == [(1, 'a'), (2, 'b'), (3, 'c')]


def test_chain():
    assert for_each(1, 2, 3).chain('abc').to(list) == [1, 2, 3, 'a', 'b', 'c']


def test_dropwhile():
    assert from_(range(10)).dropwhile(e_ < 3).to(list) == [3, 4, 5, 6, 7, 8, 9]


def test_takewhile():
    assert from_(range(10)).takewhile(e_ < 3).to(list) == [0, 1, 2]


def test_cache():
    rng = random.Random()
    original = for_each(1, 2, 3).map(lambda e: e + rng.random())
    assert original.to(list) != original.to(list)
    cached = original.cache()
    first = cached.to(list)
    second = cached.to(list)
    assert first == second


def test_index():
    src = ['apples', 'bananas', 'apricots', 'grapes', 'grapefruit']
    actual = from_(src).to(index, key=e_[0])
    expected = dict(a=['apples', 'apricots'], b=['bananas'], g=['grapes', 'grapefruit'])
    assert actual == expected

    try:
        from_(src).to(unique_index, key=e_[0])
        pytest.fail('Expected exception')
    except KeyError as e:
        pass

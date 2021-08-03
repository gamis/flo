from flo import start as e_
import pytest
from operator import *

from flo.attempt import Attempt
from flo.lamb import interpret_between, UNASSIGNED, as_fcn


def test_example_usage() -> None:
    assert list(filter(lambda i: i.startswith('i'), ['egor', 'ivan', 'elanor'])) == ['ivan']


def test_getattr_property() -> None:
    slicer = slice(3, 4)
    assert e_.start.invoke(slicer) == 3
    assert str(e_.start) == '_.start'


def test_getitem() -> None:
    s = [3, 55, 7]
    assert e_[1].invoke(s) == 55
    assert str(e_[1]) == '_[1]'


def test_method_no_args() -> None:
    s = "mystr"
    assert e_.upper().invoke(s) == "MYSTR"
    assert str(e_.upper()) == '_.upper()'


def test_method_args() -> None:
    s = "mystr"
    assert e_.startswith('my').invoke(s)
    assert str(e_.startswith('my')) == "_.startswith('my')"


def test_method_args_kwargs() -> None:
    s = "mystr"
    assert e_.split('s', maxsplit=1).invoke(s) == ['my', 'tr']
    assert str(e_.split('s', maxsplit=1)) == "_.split('s', maxsplit=1)"


@pytest.mark.parametrize('operator,left,right,expected',
                         [(eq, 3, 4, False),
                          (eq, 'foo', 'foo', True),
                          (ne, 3, 4, True),
                          (ne, 'foo', 'foo', False),
                          (lt, 3, 4, True),
                          (lt, 3, 3, False),
                          (le, 3, 3, True),
                          (gt, 3, 4, False),
                          (gt, 3, 3, False),
                          (ge, 3, 3, True),
                          (add, 3, 1, 4),
                          (sub, 3, 1, 2),
                          (truediv, 6, 3, 2)])
def test_binary_operators(operator, left, right, expected) -> None:
    assert operator(e_, right)(left) == expected


@pytest.mark.parametrize('left,right,left_inclusive,right_inclusive,element,expected',
                         [(2, 10, True, False, 3, True),
                          (2, 10, True, False, 2, True),
                          (2, 10, True, False, 10, False),
                          (2, 10, True, True, 10, True),
                          (2, 10, False, False, 2, False),
                          (2, 10, False, True, 2, False),
                          (2, 10, False, True, 10, True),
                          (2, 10, False, True, 3, True)
                          ])
def test_between(left, right, left_inclusive, right_inclusive, element, expected) -> None:
    assert e_.between(left, right, left_inclusive=left_inclusive, right_inclusive=right_inclusive)(element) == expected


def test_as_try():
    t = e_.startswith('foo').as_try()

    t1 = t('some string')
    assert t1 == Attempt(result=False)
    assert t1.succeeded()
    assert not t1.failed()

    t2 = t(32)
    assert t2.failed()
    assert not t2.succeeded()
    with pytest.raises(AttributeError) as exec_info:
        t2.result()
    assert str(exec_info.value) == "'int' object has no attribute 'startswith'"

    assert type(t2.exception()) == AttributeError
    assert str(t2.exception()) == "'int' object has no attribute 'startswith'"


@pytest.mark.parametrize('left,expected',
                         [((2, 10), dict(left=2, right=10, left_inclusive=False, right_inclusive=False)),
                          ([2, 10], dict(left=2, right=10, left_inclusive=True, right_inclusive=True)),
                          ("(2, 10)", dict(left=2, right=10, left_inclusive=False, right_inclusive=False)),
                          ("(2, 10]", dict(left=2, right=10, left_inclusive=False, right_inclusive=True)),
                          ("[2, 10)", dict(left=2, right=10, left_inclusive=True, right_inclusive=False)),
                          ("[2, 10]", dict(left=2, right=10, left_inclusive=True, right_inclusive=True)),
                          ("(2.3, 10.0)", dict(left=2.3, right=10.0, left_inclusive=False, right_inclusive=False)),
                          ("(.2, 10)", dict(left=0.2, right=10, left_inclusive=False, right_inclusive=False)),
                          ])
def test_interpret_between(left, expected) -> None:
    actual = dict(zip(('left', 'right', 'left_inclusive', 'right_inclusive'),
                      interpret_between(left, UNASSIGNED, True, False)))

    assert actual == expected


def test_interpret_between_bad_pattern() -> None:
    try:
        interpret_between("[23535]", UNASSIGNED, True, False)
        pytest.fail('expected exception')
    except ValueError:
        pass


@pytest.mark.parametrize('f,args,expected',
                         [(str, (3,), "3"),
                          (e_ + 1, (3,), 4),
                          (e_, (3,), 3),
                          (3, (14,), 3)])
def test_as_fcn(f, args, expected) -> None:
    assert as_fcn(f)(*args) == expected


@pytest.mark.parametrize('operator,target,expected',
                         [(invert, True, False)])
def test_unary_operators(operator, target, expected) -> None:
    assert operator(e_)(target) == expected


def test_has() -> None:
    assert e_.has('foo')('yep foo you')
    assert e_.has(3)({7, 18, 88}) == False
    assert e_.has('foo')(None) == False


def test_is_none() -> None:
    assert e_.is_none()(None)
    assert e_.is_none()('blarg') == False


@pytest.mark.parametrize('element,expected',
                         [(True, True),
                          (False, False),
                          ([23434], True),
                          (0, False),
                          ('', False),
                          ('blah', True),
                          ([], False)])
def test_truthy(element, expected) -> None:
    assert e_.truthy()(element) == expected


@pytest.mark.parametrize('element,expected',
                         [(True, False),
                          (False, True),
                          ([23434], False),
                          ([], True),
                          ('', True),
                          (0, True)])
def test_falsy(element, expected) -> None:
    assert e_.truthy()(element) == expected


@pytest.mark.parametrize('element,regex,expected',
                         [("foo", "foo", True),
                          ("you", "foo", False),
                          ("foo", ".*o{2}$", True)])
def test_matches(element, regex, expected) -> None:
    assert e_.matches(regex)(element) == expected


def test_is_instance() -> None:
    assert e_.instanceof(str)('adsf') == True
    assert e_.instanceof(str, int)('adsf') == True
    assert e_.instanceof(str)(3) == False
    assert e_.instanceof(str, int)(3) == True
    assert e_.instanceof(str, int)(3.0) == False


def test_invoke_root() -> None:
    assert e_.invoke(32343) == 32343


def test_sub() -> None:
    assert e_.sub_('foo', 'you')('toofoobar') == 'tooyoubar'


def test_is_in() -> None:
    assert e_.is_in({4, 3})(3)
    assert e_.is_in({4, 3})(2) == False


def test_mod() -> None:
    assert (e_ % 3)(3) == 0
    assert (e_ % 3)(1) == 1


def test_not_none() -> None:
    assert e_.not_none()(None) == False
    assert e_.not_none()(32) == True
    assert e_.not_none()(0) == True

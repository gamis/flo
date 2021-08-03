import pytest

from flo import e_, check


@pytest.mark.parametrize('kwargs,expected_result,expected_exception',
                         [(dict(val=3, test=e_ >= 4), None, ValueError("Value (3) failed test _>=4. ")),
                          (dict(val=3, test=e_.not_none()), 3, None),  # will just return 3
                          (dict(val=(3, 2), test=e_ != (None, None), msg="You need to either specify myvar or yourvar."), (3, 2), None),
                          (dict(val=(3, 2), test=e_.has(None), msg="You can't specify both myvar and yourvar."), None, ValueError("You can't specify both myvar and yourvar. Value ((3, 2)) failed test _ contains None. "))
                          ])
def test_check_that(kwargs, expected_result, expected_exception):
    try:
        r = check.that(**kwargs)
        assert expected_exception is None
        assert r == expected_result
    except ValueError as e:
        assert expected_result is None
        assert e.args == expected_exception.args


@pytest.mark.parametrize('test,args,kwargs,expected_result,expected_exception',
                         [(e_ >= 3, (3, 2), {}, None, ValueError("Value #2 = 2 failed test _>=3. ")),
                          (e_ > 3, (3, 2), {}, None, ValueError("Value #1 = 3 failed test _>3. Value #2 = 2 failed test _>3. ")),
                          (e_.not_none(), (), dict(myvar=3), (3,), None),  # will just return (3,)
                          (e_.not_none(), (3,), {}, (3,), None),  # will just return (3,)
                          (e_.instanceof(str), (), dict(myvar=3), None, ValueError("myvar = 3 failed test _.instanceof(str). ")),
                          (e_ != (None, None), (3, 2), dict(msg="You need to either specify myvar or yourvar."), (3, 2), None),
                          (e_.has(None), ((3, 2),), dict(msg="You can't specify both myvar and yourvar."), None, ValueError("You can't specify both myvar and yourvar. Value #1 = (3, 2) failed test _ contains None. "))])
def test_check_each(test, args, kwargs, expected_result, expected_exception):
    try:
        r = check.each(test, *args, **kwargs)
        assert expected_exception is None
        assert r == expected_result
    except ValueError as e:
        assert expected_result is None
        assert e.args == expected_exception.args

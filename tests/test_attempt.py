import logging
from io import StringIO
from typing import Tuple, Any, Type

import pytest
from flo.lamb import e_

from flo.attempt import try_, Attempt


@pytest.mark.parametrize('initial,params,expected_result,expected_exception',
                         [(try_(e_ / 0, 1), (e_ + 2,), None, ZeroDivisionError),  # initial failure
                          (try_(e_ / 1, 1), (e_ + 2,), 3, None),  # initial success
                          (try_(e_ / 1, 1), (e_ / 0,), None, ZeroDivisionError),  # secondary failure
                          ])
def test_and_then(initial: Attempt, params: Tuple[Any, ...], expected_result: Any, expected_exception: Type):
    a = initial.and_then(*params)
    if a.failed():
        assert expected_result is None
        assert type(a.exception()) == expected_exception
    else:
        assert a.result() == expected_result


def test_or_else():
    assert try_(e_ / 0, 1).or_else(3) == 3
    assert try_(e_ / 1, 1).or_else(3) == 1
    logger = logging.Logger('test_or_else')
    sio = StringIO()
    logger.addHandler(logging.StreamHandler(sio))
    assert try_(e_ / 0, 1).or_else(3, log=logger) == 3
    assert sio.getvalue().startswith('division by zero')

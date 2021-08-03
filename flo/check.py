from typing import Union, Callable, Any, TypeVar, Tuple

from flo.lamb import Lambda, start, as_name_function

i_ = start

T = TypeVar('T')
Expression = Union[Lambda, Callable[[Any], bool]]
_unassigned = object()


def that(val: T, test: Expression, msg: str = None) -> T:
    """
    Checks that val returns true for the test expression,
    returns the val if test passes.
    E.g.,
    >>> from flo import start as _, check
    >>> myvar, yourvar = 3, 2
    >>> check.that( myvar, _ >= 4) #will raise ValueError("Value (3) failed test _>=4.")
    >>> check.that( myvar, _.not_none()) #will just return 3
    >>> check.that( (myvar,yourvar), _!=(None,None), msg="You need to either specify myvar or yourvar.") #Just returns (myvar,yourvar)
    >>> check.that( (myvar,yourvar), _.has(None), msg="You can't specify both myvar and yourvar.") #raises ValueError("You can't specify both myvar and yourvar.  Value #1 = (3, 2) failed test _ contains None. ")
    """
    fname, fcn = as_name_function(test)

    msg = msg or ""

    result = fcn(val)
    if not isinstance(result, bool):
        raise ValueError(f'Test {fname} for {val} does not evaluate to a bool.')
    if not result:
        if msg and not msg[-1].isspace():
            msg += " "
        msg += f"Value ({val}) failed test {fname}. "
        raise ValueError(msg)

    return val


def each(test: Expression, *args: T, msg: str = None, **kwargs: T) -> Tuple[T, ...]:
    """
    Checks that test expression returns true for each value
    Returns
    E.g.,
    >>> from flo import start as _, check
    >>> myvar, yourvar = 3, 2  # typically some input arguments
    >>> check.each(_ >= 3, myvar, yourvar)  # will raise ValueError("Value #2 = 2 failed test _>=3.")
    >>> check.each(_ > 3, myvar, yourvar)  # will raise ValueError("Values #1 (3) and #2 (2) failed test _>3.")
    >>> check.each(_.not_none(), myvar=myvar)  # will just return (3,)
    >>> check.each(_.instanceof(str), myvar=myvar)  # will raise ValueError("myvar=3 does not meet test _.isinstance(str)
    >>> check.each(_ != (None, None), (myvar, yourvar), msg="You need to either specify myvar or yourvar.")  # Just returns ((myvar,yourvar),)
    >>> check.each(_.has(None), (myvar, yourvar), msg="You can't specify both myvar and yourvar.")  # raises ValueError("You can't specify both myvar and yourvar. Value #1 (3,2) does not meet test _.has(None). ")

    :param test: Expression to test, e.g., lambda e: isinstance(e, str) or _ > 0.
    :param args: Unlabeled values to test
    :param msg: Optional additional message to prepend to the standard one
    :param kwargs: labeled values to test
    :return: Tuple of the values themselves
    :raises ValueError if test is false for any values
    """
    fname, fcn = as_name_function(test)

    msg = msg or ""

    instances = (*((f"Value #{n + 1}", a) for n, a in enumerate(args)), *kwargs.items())
    failed = False

    for label, instance in instances:
        ret = fcn(instance)
        if not isinstance(ret, bool):
            raise ValueError(f'Test {fname} for {label}={instance} does not evaluate to a bool.')
        if not ret:
            if msg and not msg[-1].isspace():
                msg += " "
            msg += f"{label} = {instance} failed test {fname}. "
            failed = True

    if failed:
        raise ValueError(msg)

    return tuple((*args, *kwargs.values()))

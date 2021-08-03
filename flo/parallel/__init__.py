from typing import Iterable, Dict, Sequence, Optional

from ._fascade import PFascade, R, I, Done, Future, Mapper
from ..lamb import FunctionOrLambda
from ._threads import Threads
from ._mp import Processes

try:
    from ._joblib import JobLibParallel
except ImportError:
    pass

singletons: Dict[str, PFascade] = {}


def register_pool(key: str, pool: PFascade):
    singletons[key] = pool


def get_pool(key: str) -> Optional[PFascade]:
    return singletons.get(key)


def pmap(func: FunctionOrLambda, it: Iterable[I], timeout: float = None, chunksize: int = 1, **kwargs) -> Sequence[R]:
    """
    Apply func to each element of it using a shared parallel-processing backend.
    Backend will be JobLibParallel if available, else multiprocessing-backed Processes
    """
    if 'pmap' not in singletons:
        try:
            register_pool('pmap', JobLibParallel(max_workers=-1))
        except Exception:
            register_pool('pmap', Processes(max_workers=None))

    return singletons['pmap'].map_ordered(func, it, timeout=timeout, chunksize=chunksize, **kwargs)


def tmap(func: FunctionOrLambda, it: Iterable[I], timeout: float = None, chunksize: int = 1, **kwargs) -> Sequence[R]:
    """
    Apply func to each element of it using a shared threaded backend.
    """

    if 'tmap' not in singletons:
        register_pool('tmap', Threads(max_workers=None))

    return singletons['tmap'].map_ordered(func, it, timeout=timeout, chunksize=chunksize, **kwargs)

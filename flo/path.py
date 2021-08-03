"""
Functions related to file paths
"""
import abc
import stat
from datetime import datetime
from hashlib import md5
from io import BufferedReader, DEFAULT_BUFFER_SIZE
from pathlib import Path, PurePath
from typing import *

from flo.hashing import file_hash
from flo.it2 import It, from_
from flo.lamb import as_fcn, FunctionOrLambda

PathLike = Union[str, Path, 'FloPath']

# 1. Files: .ls(),

PathType = TypeVar('PathType', bound='FloPath')


def path_for(path: PathLike) -> 'FloPath':
    if isinstance(path, Path):
        return MountedPath(path)
    if isinstance(path, FloPath):
        return path
    if not isinstance(path, str):
        raise ValueError(f'Unrecognized path type {type(path)} for {path}')
    if '://' in path:
        raise NotImplementedError("Can't yet handle URLs")
    return MountedPath(Path(path))


class FloPath(abc.ABC):
    """Abstract path that can be implemented with just a normal Path or something remote,
    like a reference to blob storage, an sftp path, etc."""
    path: PurePath

    def __init__(self, path: PurePath):
        self.path = path

    def with_path(self: PathType, path: PurePath) -> PathType:
        return type(self)(path)

    def joinpath(self: PathType, *parts) -> PathType:
        return self.with_path(self.path.joinpath(*parts))

    def with_suffix(self: PathType, suffix: str) -> PathType:
        return self.with_path(self.path.with_suffix(suffix))

    def relative_to(self: PathType, parent: 'FloPath') -> PurePath:
        return self.path.relative_to(parent.path)

    @abc.abstractmethod
    def iterdir(self: PathType) -> Iterable[PathType]:
        pass

    @abc.abstractmethod
    def glob(self: PathType, glob: str) -> Iterable[PathType]:
        pass

    @abc.abstractmethod
    def rglob(self: PathType, glob: str) -> Iterable[PathType]:
        pass

    @abc.abstractmethod
    def size(self) -> int:
        pass

    @abc.abstractmethod
    def last_updated(self) -> datetime:
        pass

    @abc.abstractmethod
    def mode(self) -> int:
        pass

    def ls(self: PathType) -> Sequence[PathType]:
        return ls(self)

    @abc.abstractmethod
    def open(self, mode: str = 'rb', **kwargs) -> IO:
        pass

    def copy_to(self, dest: PathLike) -> int:
        dest = path_for(dest)
        total = 0
        with self.open('rb') as f, dest.open('wb') as d:
            while True:
                line = f.read(DEFAULT_BUFFER_SIZE)
                if not line:
                    return total
                d.write(line)
                total += len(line)

    def from_lines(self) -> It[str]:
        return from_lines(self)

    def content_md5(self) -> str:
        with self.open('rb') as f:
            return file_hash(f, md5)

    @abc.abstractmethod
    def is_file(self) -> bool:
        pass

    @abc.abstractmethod
    def is_dir(self) -> bool:
        pass


class MountedPath(FloPath):
    path: Path

    def with_path(self, path: PathLike) -> PathType:
        return MountedPath(Path(path))

    def iterdir(self) -> Iterable[PathType]:
        return map(MountedPath, self.path.iterdir())

    def glob(self, glob: str) -> Iterable[PathType]:
        return map(MountedPath, self.path.glob(glob))

    def rglob(self, glob: str) -> Iterable[PathType]:
        return map(MountedPath, self.path.rglob(glob))

    def size(self) -> int:
        return self.path.stat().st_size

    def last_updated(self) -> datetime:
        return datetime.fromtimestamp(self.path.stat().st_mtime)

    def mode(self) -> int:
        return self.path.stat().st_mode

    def open(self, mode: str = 'rb', **kwargs) -> IO:
        return self.path.open(mode, **kwargs)

    def is_file(self) -> bool:
        return self.path.is_file()

    def is_dir(self) -> bool:
        return self.path.is_dir()


UNITS = ((1000 ** 0, 'B'), (1000 ** 1, 'KB'), (1000 ** 2, 'MB'), (1000 ** 3, 'GB'), (1000 ** 4, 'TB'), (1000 ** 5, 'PB'))[::-1]


def sizestr(size: int) -> str:
    for factor, suffix in UNITS:
        if size >= factor:
            if factor == 1:
                return f"{size} {suffix}".rjust(7)
            else:
                return f"{size / factor:.2f} {suffix}".rjust(7)
    return str(size).rjust(7)


def ls(directory: PathLike, recursive: bool = False, glob: str = '*', sort_key: FunctionOrLambda = None, print_hidden: bool = True, print_mode: bool = True, print_updated: bool = True, print_size: bool = True) -> Sequence[PathLike]:
    """Prints the contents of a directory and returns it as a sequence
    :param directory Directory to the list the contents of
    :param recursive Whether to print the contents of subdirectories or just direct children.
    :param glob: Optional glob pattern. If provided, will only print and return children matching glob.
    :param sort_key Function by which to sort results. E.g., e_.size()*-1 will give you largest files first
    :param print_hidden Whether to print (or suppress) files starting with .
    :param print_mode Whether to print the mode
    :param print_updated Whether to print the datetime the file was updated
    :param print_size Whether to print the file size
    :return A sequence of the contents of the directory. Only arguments recursive, glob, and sort_key impact the return value.
    """
    directory = path_for(directory)
    src = directory.rglob(glob) if recursive else directory.glob(glob)
    if not print_hidden:
        src = filter(lambda p: not p.name.startswith('.'), src)
    contents = sorted(src, key=as_fcn(sort_key)) if sort_key else list(src)

    columns = []
    if print_mode:
        columns.append(['Mode', *(stat.filemode(p.mode()) for p in contents)])
    if print_size:
        sizes = ['Size', *(sizestr(p.size()) if p.is_file() else '----' for p in contents)]
        columns.append(sizes)
    if print_updated:
        columns.append(['Updated', *map(lambda p: f"{p.last_updated():%Y-%m-%d %H:%M:%S}", contents)])
    columns.append(['Path', *(p.relative_to(directory).as_posix() for p in contents)])

    col_size = [min(max(map(len, col)), 120 // len(columns)) for col in columns]
    for n, row in enumerate(zip(*columns)):
        print(*(c.ljust(s) for c, s in zip(row, col_size)))
        if n == 0:
            print(*("-" * s for c, s in zip(row, col_size)))
        elif (n % 80) == 0:
            input('---More---')


def from_lines(path: PathLike) -> It[str]:
    def src() -> Iterator[str]:
        with path_for(path).open('rt') as f:
            if not hasattr(f, 'readline'):
                f = BufferedReader(f)
            while True:
                line = f.readline()
                if not line:
                    return
                yield line

    return from_(src())


def content_md5(path: PathLike) -> str:
    with path_for(path).open('rb') as f:
        return file_hash(f, md5)

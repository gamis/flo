import base64
from _hashlib import HASH
from dataclasses import dataclass
from hashlib import md5
from io import DEFAULT_BUFFER_SIZE
from typing import IO, Callable


@dataclass()
class Hash(object):
    _h: HASH

    def digest(self) -> bytes:
        return self._h.digest()

    def hexdigest(self) -> str:
        return self._h.hexdigest()

    def base64(self) -> str:
        return base64.b32encode(self.digest())


def file_hash(io: IO[bytes], hash_fcn: Callable[[], HASH] = md5) -> Hash:
    h = hash_fcn()
    while chunk := io.read(DEFAULT_BUFFER_SIZE):
        h.update(chunk)
    return Hash(h)

import hashlib
from io import BytesIO
from pathlib import Path

import pytest

from flo import hashing


@pytest.mark.parametrize('io,hash_fcn,expected_hex,expected_base64',
                         [(b'happy birthday', hashlib.md5, 'fa47bb06f6ea83e49cc70cd2a130f1dc', b'7JD3WBXW5KB6JHGHBTJKCMHR3Q======'),
                          (b'happy birthday', hashlib.blake2b, '5ea2f983ffa47513a706e1a7eb586a6fd8497968d403fa282818350de0ba4a0fdd182edc299126886e4b8df5dcc61fee7388a105f62fce020e21a079e89f297a',
                           b'L2RPTA77UR2RHJYG4GT6WWDKN7MES6LI2QB7UKBIDA2Q3YF2JIH52GBO3QUZCJUINZFY35O4YYP6444IUEC7ML6OAIHCDIDZ5CPSS6Q='),
                          (b'', hashlib.md5, 'd41d8cd98f00b204e9800998ecf8427e', b'2QOYZWMPACZAJ2MABGMOZ6CCPY======')])
def test_file_hash(io, hash_fcn, expected_hex, expected_base64) -> None:
    if isinstance(io, bytes):
        io = BytesIO(io)
    h = hashing.file_hash(io, hash_fcn)
    assert h.hexdigest() == expected_hex
    assert h.base64() == expected_base64


def test_big_file_hash() -> None:
    test_file = Path(__file__).parent / 'rand16k.file'
    with test_file.open('rb') as f:
        h = hashing.file_hash(f)
    hh = hashlib.md5()
    hh.update(test_file.read_bytes())
    assert h.hexdigest() == hh.hexdigest()

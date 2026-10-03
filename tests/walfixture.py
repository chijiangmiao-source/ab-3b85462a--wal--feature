"""Shared fixtures for WAL recovery tests.

Two ways of obtaining WAL bytes are used:

* ``make_sqlite_wal`` drives the real sqlite3 library with auto-checkpoint
  disabled and snapshots the -wal file while the connection is still open,
  giving genuine multi-transaction WALs (including frames spilled by an
  uncommitted transaction).
* ``build_wal`` hand-assembles WAL bytes so tests can inject illegal page
  numbers, salt changes and checksum corruption at precise offsets.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import struct
import tempfile

from wal_recover import (
    FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
    WAL_MAGIC_BE,
    WAL_MAGIC_LE,
    _checksum,
)


def make_sqlite_wal(page_size: int = 4096, transactions=((1,),), *,
                    spill_uncommitted: int = 0, cache_size: int = 2000):
    """Create a real database/WAL pair.

    ``transactions`` is a sequence; each element is the number of marker rows
    inserted and committed in that transaction. When ``spill_uncommitted`` is
    non-zero, that many rows are inserted in a final transaction which is
    left open (frames spill into the WAL with db-size 0 when the cache is
    small) when the snapshot is taken.

    Returns (db_bytes, wal_bytes).
    """
    tmp = tempfile.mkdtemp(prefix="waltest-")
    path = os.path.join(tmp, "app.db")
    con = sqlite3.connect(path)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA page_size=%d" % page_size)
        con.execute("PRAGMA wal_autocheckpoint=0")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA cache_size=%d" % cache_size)
        con.execute("CREATE TABLE telemetry(id INTEGER PRIMARY KEY, marker TEXT)")
        con.commit()
        for n in transactions:
            if n:
                con.executemany(
                    "INSERT INTO telemetry(marker) VALUES(?)",
                    [("committed-%06d" % i,) for i in range(n)],
                )
            con.commit()
        if spill_uncommitted:
            # Large uncommitted insert forces page frames with db-size 0.
            con.executemany(
                "INSERT INTO telemetry(marker) VALUES(?)",
                [("UNCOMMITTED-%06d" % i,) for i in range(spill_uncommitted)],
            )
        with open(path, "rb") as fh:
            db_bytes = fh.read()
        wal_path = path + "-wal"
        if os.path.exists(wal_path):
            with open(wal_path, "rb") as fh:
                wal_bytes = fh.read()
        else:
            wal_bytes = b""
    finally:
        con.close()
        shutil.rmtree(tmp, ignore_errors=True)
    return db_bytes, wal_bytes


def list_frames(wal: bytes):
    page_size = struct.unpack(">I", wal[8:12])[0]
    frame_size = FRAME_HEADER_SIZE + page_size
    out = []
    for i in range((len(wal) - WAL_HEADER_SIZE) // frame_size):
        off = WAL_HEADER_SIZE + i * frame_size
        page_no, db_size, s1, s2, c1, c2 = struct.unpack(">IIIIII", wal[off : off + 24])
        out.append(
            {
                "number": i + 1,
                "offset": off,
                "page_no": page_no,
                "db_size": db_size,
                "salt": (s1, s2),
                "checksum": (c1, c2),
            }
        )
    return out, page_size


def build_wal(
    page_size: int,
    frames,
    *,
    salt=(0x11111111, 0x22222222),
    big_endian_words: bool = False,
    version: int = 3007000,
    checkpoint_seq: int = 0,
    corrupt_header_checksum: bool = False,
    initial_checksum=None,
):
    """Assemble a WAL.

    Each frame is a dict with keys: page_no, db_size, page (bytes), and
    optional salt=(s1, s2), corrupt_checksum=False. Pass
    ``initial_checksum=(s1,s2)`` (and slice off the header afterwards) to
    append frames that continue another WAL's cumulative checksum chain.
    """
    magic = WAL_MAGIC_BE if big_endian_words else WAL_MAGIC_LE
    header_prefix = struct.pack(
        ">IIIIII",
        magic,
        version,
        page_size,
        checkpoint_seq,
        salt[0],
        salt[1],
    )
    hs1, hs2 = _checksum(header_prefix, 0, 0, big_endian_words)
    if corrupt_header_checksum:
        hs1 ^= 0xFFFFFFFF
    header = header_prefix + struct.pack(">II", hs1, hs2)
    blob = bytearray(header)

    s1, s2 = initial_checksum if initial_checksum is not None else (hs1, hs2)

    for fr in frames:
        page = fr["page"]
        assert len(page) == page_size
        fs1, fs2 = fr.get("salt", salt)
        s1, s2 = _checksum(
            struct.pack(">II", fr["page_no"], fr["db_size"]), s1, s2, big_endian_words
        )
        s1, s2 = _checksum(page, s1, s2, big_endian_words)
        if fr.get("corrupt_checksum"):
            s1 ^= 0x01
        blob += struct.pack(">IIIIII", fr["page_no"], fr["db_size"], fs1, fs2, s1, s2)
        blob += page
    return bytes(blob)


def fake_page(page_size: int, tag: bytes, fill: int = 0x55) -> bytes:
    """Build page bytes with an identifiable 8-byte tag at the start."""
    p = bytearray((fill for _ in range(page_size)))
    p[: len(tag)] = tag
    return bytes(p)


def minimal_main_db(page_size: int = 4096, pages: int = 2) -> bytes:
    """A structurally valid (header-only) main database for synthetic WALs."""
    db = bytearray(page_size * pages)
    db[:16] = b"SQLite format 3\x00"
    db[16:18] = struct.pack(">H", page_size)
    return bytes(db)

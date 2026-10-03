"""End-to-end HTTP smoke checks against a running recovery service.

Used both locally and by the one-shot ``verify`` Compose service. The target
URL comes from TARGET_URL (default http://127.0.0.1:${PORT:-8080}).

Scenarios:
  1. health endpoint
  2. valid multi-transaction WAL produced by real SQLite -> recovered image
     is itself a readable database containing exactly the committed rows
  3. last frame truncated -> earlier complete commit still returned, the
     invalid offset is reported, and uncommitted bytes never enter the image
  4. last frame checksum-corrupt -> same recovery, precise checksum offset
  5. WAL without any commit frame -> 409 and no image in the response
  6. malformed base64 -> 400
  7. page_order=frame orders sources by last-appearance frame
"""

from __future__ import annotations

import base64
import json
import os
import struct
import sys
import tempfile
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "app"))

import walfixture  # noqa: E402
from wal_recover import FRAME_HEADER_SIZE, WAL_HEADER_SIZE, recover  # noqa: E402


class SmokeFailure(Exception):
    pass


def _target() -> str:
    return os.environ.get(
        "TARGET_URL", "http://127.0.0.1:%s" % os.environ.get("PORT", "8080")
    ).rstrip("/")


def _request(method: str, path: str, payload: dict | None = None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        _target() + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _post_recover(db: bytes, wal: bytes, page_order: str = "numeric"):
    return _request(
        "POST",
        "/recover",
        {
            "database_base64": base64.b64encode(db).decode("ascii"),
            "wal_base64": base64.b64encode(wal).decode("ascii"),
            "page_order": page_order,
        },
    )


def _open_count(image_b64: str) -> int:
    import sqlite3

    data = base64.b64decode(image_b64)
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with open(path, "wb") as fh:
            fh.write(data)
        con = sqlite3.connect("file:%s?immutable=1" % path, uri=True)
        try:
            return con.execute("SELECT count(*) FROM telemetry").fetchone()[0]
        finally:
            con.close()
    finally:
        os.unlink(path)


def check_health():
    status, body = _request("GET", "/health")
    if status != 200 or body.get("status") != "ok":
        raise SmokeFailure("health check failed: %r %r" % (status, body))
    return "GET /health -> 200 ok"


def check_valid_multi_transaction():
    db, wal = walfixture.make_sqlite_wal(4096, (3, 40, 400))
    expected = recover(db, wal)
    status, body = _post_recover(db, wal)
    if status != 200 or body.get("status") != "recovered":
        raise SmokeFailure("expected 200/recovered, got %r %r" % (status, body))
    if body["commit_frame"] != expected.commit_frame:
        raise SmokeFailure("commit frame mismatch: %s != %s"
                          % (body["commit_frame"], expected.commit_frame))
    if body["digest"] != expected.digest:
        raise SmokeFailure("digest does not match engine-computed image")
    if body["wal_complete"] is not True:
        raise SmokeFailure("complete WAL flagged incomplete")
    if _open_count(body["image_base64"]) != 3 + 40 + 400:
        raise SmokeFailure("rebuilt image does not contain all committed rows")
    return "valid multi-transaction WAL -> %d-page image, %d committed rows" % (
        body["recovered_pages"], 3 + 40 + 400,
    )


def check_truncated_last_frame():
    db, wal = walfixture.make_sqlite_wal(4096, (3, 40))
    broken = wal + b"\x00" * 100  # incomplete trailing frame
    status, body = _post_recover(db, broken)
    if status != 200 or body.get("status") != "recovered_with_invalid_tail":
        raise SmokeFailure("expected invalid-tail recovery, got %r %r"
                           % (status, body))
    if body["first_invalid_offset"] != len(wal):
        raise SmokeFailure("truncation offset %r != %d"
                           % (body["first_invalid_offset"], len(wal)))
    if _open_count(body["image_base64"]) != 3 + 40:
        raise SmokeFailure("image after truncation lost committed rows")
    return "truncated trailing frame -> commit frame %d kept, offset %d" % (
        body["commit_frame"], body["first_invalid_offset"],
    )


def check_corrupt_last_frame_checksum():
    db, wal = walfixture.make_sqlite_wal(4096, (3, 40))
    page_size = struct.unpack(">I", wal[8:12])[0]
    salt = struct.unpack(">II", wal[16:24])
    fsz = FRAME_HEADER_SIZE + page_size
    n_frames = (len(wal) - WAL_HEADER_SIZE) // fsz
    last_hdr_off = WAL_HEADER_SIZE + (n_frames - 1) * fsz
    running = struct.unpack(">II", wal[last_hdr_off + 16: last_hdr_off + 24])
    rogue = walfixture.build_wal(
        page_size,
        [{"page_no": 1, "db_size": 2, "page": walfixture.fake_page(page_size, b"X")}],
        salt=salt,
        initial_checksum=running,
    )[WAL_HEADER_SIZE:]
    broken = bytearray(wal + rogue)
    broken[-1] ^= 0xFF  # corrupt page payload -> cumulative checksum fails
    status, body = _post_recover(db, bytes(broken))
    if status != 200:
        raise SmokeFailure("expected recovery of earlier commit, got %r %r"
                           % (status, body))
    expected_offset = WAL_HEADER_SIZE + (len(wal) - WAL_HEADER_SIZE) // fsz * fsz + 16
    if body["first_invalid_offset"] != expected_offset:
        raise SmokeFailure("checksum offset %r != %d"
                           % (body["first_invalid_offset"], expected_offset))
    if _open_count(body["image_base64"]) != 3 + 40:
        raise SmokeFailure("corrupt transaction leaked into image")
    return "corrupt last-frame checksum ignored, first invalid offset %d" % (
        body["first_invalid_offset"],
    )


def check_no_commit_wal():
    page_size = 4096
    db = walfixture.minimal_main_db(page_size, pages=1)
    wal = walfixture.build_wal(
        page_size,
        [
            {"page_no": 1, "db_size": 0, "page": walfixture.fake_page(page_size, b"A")},
            {"page_no": 1, "db_size": 0, "page": walfixture.fake_page(page_size, b"B")},
        ],
    )
    status, body = _post_recover(db, wal)
    if status != 409 or body.get("status") != "unrecoverable":
        raise SmokeFailure("expected 409/unrecoverable, got %r %r" % (status, body))
    if "image_base64" in body:
        raise SmokeFailure("failure response must not contain a partial image")
    if "no complete recoverable commit" not in body.get("error", ""):
        raise SmokeFailure("unexpected error text: %r" % body.get("error"))
    return "commit-less WAL -> 409, no image returned"


def check_bad_base64():
    payload = {
        "database_base64": "!!!not-base64!!!",
        "wal_base64": "AA==",
        "page_order": "numeric",
    }
    status, body = _request("POST", "/recover", payload)
    if status != 400:
        raise SmokeFailure("expected 400 for bad base64, got %r" % status)
    return "invalid base64 -> 400"


def check_page_ordering():
    page_size = 4096
    db = walfixture.minimal_main_db(page_size, pages=2)
    frames = [
        {"page_no": 1, "db_size": 0, "page": walfixture.fake_page(page_size, b"1")},
        {"page_no": 2, "db_size": 2, "page": walfixture.fake_page(page_size, b"2")},
        {"page_no": 1, "db_size": 2, "page": walfixture.fake_page(page_size, b"3")},
    ]
    wal = walfixture.build_wal(page_size, frames)
    _, numeric = _post_recover(db, wal, "numeric")
    _, by_frame = _post_recover(db, wal, "frame")
    if [e["page"] for e in numeric["page_sources"]] != [1, 2]:
        raise SmokeFailure("numeric ordering wrong: %r" % numeric["page_sources"])
    if [e["frame"] for e in by_frame["page_sources"]] != [2, 3]:
        raise SmokeFailure("frame ordering wrong: %r" % by_frame["page_sources"])
    return "page_order numeric/frame both honoured"


CHECKS = [
    check_health,
    check_valid_multi_transaction,
    check_truncated_last_frame,
    check_corrupt_last_frame_checksum,
    check_no_commit_wal,
    check_bad_base64,
    check_page_ordering,
]


def run() -> int:
    print("smoke target: %s" % _target())
    failures = 0
    for check in CHECKS:
        try:
            print("  - %s ... %s" % (check.__name__, check()))
        except Exception as exc:  # noqa: BLE001 - report every failure
            failures += 1
            print("  - %s ... FAIL: %s" % (check.__name__, exc))
    if failures:
        print("SMOKE RESULT: %d/%d checks failed" % (failures, len(CHECKS)))
    else:
        print("SMOKE RESULT: all %d checks passed" % len(CHECKS))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run())

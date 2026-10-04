"""Raw-byte SQLite 3 WAL recovery engine.

The main database and the WAL are parsed from their original bytes only (no
sqlite3 library is involved). Anything that deviates from the documented WAL
format aborts the whole recovery; a partial image is never returned.

SQLite WAL facts used here:
  * Magic 0x377f0682 -> checksums accumulate 32-bit words little-endian;
    magic 0x377f0683 -> big-endian words.
  * 32-byte WAL header; each frame is a 24-byte header followed by one page.
  * Integers in both headers are stored big-endian on disk.
  * Checksums are cumulative: the WAL header checksum seeds the first frame.
  * A frame is a commit frame when its db-size field is non-zero.
"""

from __future__ import annotations

import base64
import hashlib
import struct
from dataclasses import dataclass, field

WAL_MAGIC_LE = 0x377F0682
WAL_MAGIC_BE = 0x377F0683
WAL_HEADER_SIZE = 32
FRAME_HEADER_SIZE = 24
WAL_FORMAT_VERSION = 3007000
MIN_PAGE_SIZE = 512
MAX_PAGE_SIZE = 4096
MAX_WAL_SIZE = 2 * 1024 * 1024
SQLITE_HEADER = b"SQLite format 3\x00"


class RecoveryError(Exception):
    """Recovery cannot proceed.

    ``offset`` is the byte offset (within the WAL unless ``scope`` says
    otherwise) of the first invalid byte, or ``None`` when no single offset
    applies (e.g. empty WAL / no valid commit).
    """

    def __init__(self, message: str, *, offset: int | None = None, scope: str = "wal"):
        super().__init__(message)
        self.message = message
        self.offset = offset
        self.scope = scope


@dataclass
class FrameInfo:
    number: int          # 1-based ordinal in the WAL
    offset: int          # frame header offset in the WAL
    page_no: int
    db_size: int         # commit db size in pages (0 = not a commit frame)
    salt1: int
    salt2: int


@dataclass
class RecoveryResult:
    commit_frame: int
    db_size: int
    page_size: int
    # page number -> frame its image was taken from (0 = main database)
    page_sources: dict[int, int] = field(default_factory=dict)
    image: bytes = b""
    digest_alg: str = "sha256"
    digest: str = ""
    # WAL structural diagnostics: recovery stops at the first invalid frame;
    # the image still reflects a complete earlier commit, never a partial one.
    valid_frames: int = 0
    wal_complete: bool = True
    first_invalid_offset: int | None = None
    first_invalid_reason: str | None = None

    def to_report(self, *, page_order: str) -> dict:
        if page_order == "numeric":
            pages = sorted(self.page_sources)
        else:  # "frame": order of each page's last appearance before the commit
            pages = [
                p for p, _ in sorted(self.page_sources.items(), key=lambda kv: (kv[1], kv[0]))
            ]
        wal_pages = sum(1 for src in self.page_sources.values() if src > 0)
        return {
            "status": "recovered" if self.wal_complete else "recovered_with_invalid_tail",
            "commit_frame": self.commit_frame,
            "recovered_pages": len(self.image) // self.page_size,
            "wal_pages": wal_pages,
            "valid_frames": self.valid_frames,
            "wal_complete": self.wal_complete,
            "first_invalid_offset": self.first_invalid_offset,
            "first_invalid_reason": self.first_invalid_reason,
            "db_size_pages": self.db_size,
            "page_size": self.page_size,
            "page_order": page_order,
            "page_sources": [
                {"page": p, "frame": self.page_sources[p]} for p in pages
            ],
            "image_base64": base64.b64encode(self.image).decode("ascii"),
            "digest_alg": self.digest_alg,
            "digest": self.digest,
        }


def _checksum(data: bytes, s1: int, s2: int, big_endian_words: bool) -> tuple[int, int]:
    """One WAL checksum pass over 32-bit words, two words per iteration."""
    words = struct.unpack(
        (">" if big_endian_words else "<") + "%dI" % (len(data) // 4), data
    )
    for i in range(0, len(words), 2):
        s1 = (s1 + words[i] + s2) & 0xFFFFFFFF
        s2 = (s2 + words[i + 1] + s1) & 0xFFFFFFFF
    return s1, s2


def _check_page_size(page_size: int, where: str, offset: int, scope: str) -> None:
    if not (MIN_PAGE_SIZE <= page_size <= MAX_PAGE_SIZE) or (page_size & (page_size - 1)):
        raise RecoveryError(
            "%s declares unsupported page size %r (allowed: powers of two "
            "from %d to %d)" % (where, page_size, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
            offset=offset,
            scope=scope,
        )


def _validate_main_db(db: bytes) -> int:
    if len(db) < 100:
        raise RecoveryError(
            "main database is shorter than the 100-byte SQLite header",
            offset=0,
            scope="database",
        )
    if db[:16] != SQLITE_HEADER:
        raise RecoveryError(
            "main database missing 'SQLite format 3\\x00' magic header",
            offset=0,
            scope="database",
        )
    encoded = struct.unpack(">H", db[16:18])[0]
    if encoded == 1:  # SQLite encodes 65536 as 1; outside this service's range
        raise RecoveryError(
            "main database declares 65536-byte pages, exceeding the %d-byte "
            "limit" % MAX_PAGE_SIZE,
            offset=16,
            scope="database",
        )
    _check_page_size(encoded, "main database", 16, "database")
    return encoded


def _parse_wal_header(wal: bytes) -> dict:
    if len(wal) == 0:
        raise RecoveryError("WAL is empty: there is no recoverable commit")
    if len(wal) < WAL_HEADER_SIZE:
        raise RecoveryError(
            "truncated WAL header: %d of %d bytes present"
            % (len(wal), WAL_HEADER_SIZE),
            offset=len(wal),
        )
    magic = struct.unpack(">I", wal[0:4])[0]
    if magic == WAL_MAGIC_LE:
        big_endian_words = False
    elif magic == WAL_MAGIC_BE:
        raise RecoveryError(
            "WAL uses big-endian checksums (magic 0x%08x); this service "
            "supports little-endian SQLite 3 databases only (magic 0x%08x)"
            % (WAL_MAGIC_BE, WAL_MAGIC_LE),
            offset=0,
        )
    else:
        raise RecoveryError(
            "bad WAL magic 0x%08x (expected 0x%08x); only standard SQLite 3 "
            "WALs are supported" % (magic, WAL_MAGIC_LE),
            offset=0,
        )
    version = struct.unpack(">I", wal[4:8])[0]
    if version != WAL_FORMAT_VERSION:
        raise RecoveryError(
            "unsupported WAL format version %d (expected %d)"
            % (version, WAL_FORMAT_VERSION),
            offset=4,
        )
    page_size = struct.unpack(">I", wal[8:12])[0]
    if page_size == 1:
        raise RecoveryError(
            "WAL header declares 65536-byte pages, exceeding the %d-byte limit"
            % MAX_PAGE_SIZE,
            offset=8,
        )
    _check_page_size(page_size, "WAL header", 8, "wal")
    checkpoint_seq = struct.unpack(">I", wal[12:16])[0]
    salt1, salt2 = struct.unpack(">II", wal[16:24])
    calc1, calc2 = _checksum(wal[:24], 0, 0, big_endian_words)
    file1, file2 = struct.unpack(">II", wal[24:32])
    if (calc1, calc2) != (file1, file2):
        raise RecoveryError(
            "WAL header checksum mismatch: stored 0x%08x%08x vs computed "
            "0x%08x%08x" % (file1, file2, calc1, calc2),
            offset=24,
        )
    return {
        "page_size": page_size,
        "checkpoint_seq": checkpoint_seq,
        "salt1": salt1,
        "salt2": salt2,
        "big_endian_words": big_endian_words,
        "checksum": (file1, file2),
    }


def _parse_frames(wal: bytes, hdr: dict) -> tuple[list[FrameInfo], int | None, str | None]:
    """Validate frames until the first invalid one.

    Returns ``(frames, first_invalid_offset, reason)``. Frames before the
    failure are returned with their cumulative checksums proven intact.
    Per frame, in order: full-frame presence (truncation), salt match,
    non-zero page number, then the cumulative checksum over the first 8 bytes
    of the frame header and the page bytes.
    """
    page_size = hdr["page_size"]
    frame_size = FRAME_HEADER_SIZE + page_size
    body = len(wal) - WAL_HEADER_SIZE
    frame_count, tail = divmod(body, frame_size)
    truncation_offset = len(wal) - tail if tail else None
    truncation_reason = (
        "truncated trailing frame: %d of %d bytes present" % (tail, frame_size)
        if tail
        else None
    )

    big = hdr["big_endian_words"]
    s1, s2 = hdr["checksum"]
    frames: list[FrameInfo] = []
    for i in range(frame_count):
        off = WAL_HEADER_SIZE + i * frame_size
        fh = wal[off : off + FRAME_HEADER_SIZE]
        page_no, db_size = struct.unpack(">II", fh[0:8])
        fs1, fs2 = struct.unpack(">II", fh[8:16])
        stored1, stored2 = struct.unpack(">II", fh[16:24])

        if (fs1, fs2) != (hdr["salt1"], hdr["salt2"]):
            return (
                frames,
                off + 8,
                "frame %d salt 0x%08x%08x does not match WAL header salt "
                "0x%08x%08x" % (i + 1, fs1, fs2, hdr["salt1"], hdr["salt2"]),
            )

        page = wal[off + FRAME_HEADER_SIZE : off + frame_size]
        s1, s2 = _checksum(fh[0:8], s1, s2, big)
        s1, s2 = _checksum(page, s1, s2, big)
        if (s1, s2) != (stored1, stored2):
            return (
                frames,
                off + 16,
                "frame %d cumulative checksum mismatch: stored 0x%08x%08x vs "
                "computed 0x%08x%08x" % (i + 1, stored1, stored2, s1, s2),
            )

        # Only after the checksum proves the header intact is the page number
        # field trustworthy enough to validate semantically.
        if page_no == 0:
            return (
                frames,
                off,
                "frame %d carries illegal page number 0" % (i + 1),
            )

        frames.append(
            FrameInfo(
                number=i + 1,
                offset=off,
                page_no=page_no,
                db_size=db_size,
                salt1=fs1,
                salt2=fs2,
            )
        )
    return frames, truncation_offset, truncation_reason


def recover(db: bytes, wal: bytes, target_frame: int | None = None) -> RecoveryResult:
    """Rebuild the image of a recoverable commit.

    With ``target_frame`` unset, the image is rebuilt at the last checksum-
    valid frame that commits a non-zero database size (historical default).
    With ``target_frame`` set to a 1-based frame ordinal, the image is rebuilt
    exactly at that historical commit boundary: every recovered page takes
    only its last image at or before the target frame, so later commits and
    uncommitted transactions can never leak in. The target must itself be a
    fully validated commit frame; a missing, uncommitted or not-yet-validated
    (past the first invalid frame) target raises RecoveryError.

    Raises RecoveryError on any malformed input or when no complete commit
    exists; never returns a partial image.
    """
    if target_frame is not None and (
        not isinstance(target_frame, int)
        or isinstance(target_frame, bool)
        or target_frame < 1
    ):
        raise RecoveryError(
            "target_frame must be a positive integer, got %r" % (target_frame,),
            scope="request",
        )

    if len(wal) > MAX_WAL_SIZE:
        raise RecoveryError(
            "WAL exceeds %d-byte limit (%d bytes)" % (MAX_WAL_SIZE, len(wal)),
            offset=MAX_WAL_SIZE,
        )
    db_page_size = _validate_main_db(db)
    hdr = _parse_wal_header(wal)
    if hdr["page_size"] != db_page_size:
        raise RecoveryError(
            "page size mismatch: WAL header says %d, main database says %d"
            % (hdr["page_size"], db_page_size),
            offset=8,
        )

    frames, bad_offset, bad_reason = _parse_frames(wal, hdr)
    if not frames:
        # Truncated before even one complete frame: nothing validated.
        if bad_offset is not None:
            raise RecoveryError(
                "%s; no complete frame precedes it, so there is no "
                "recoverable commit" % bad_reason,
                offset=bad_offset,
            )
        raise RecoveryError(
            "WAL has a valid header but no frames: there is no recoverable "
            "commit"
        )

    # Commit boundaries proven by validated frames: frame number -> frame,
    # for every validated frame carrying a non-zero commit database size.
    # Their chronological order (and which transaction each frame belongs
    # to) lets recovery rebuild the image at any historical commit point.
    commit_frames: dict[int, FrameInfo] = {
        fr.number: fr for fr in frames if fr.db_size > 0
    }

    if target_frame is None:
        # Default: the last recoverable commit within the validated prefix.
        commit = next((fr for fr in reversed(frames) if fr.db_size > 0), None)
        if commit is None:
            raise RecoveryError(
                "no frame carries a non-zero commit database size among the "
                "%d validated frame(s)%s: the WAL holds no complete "
                "recoverable commit"
                % (
                    len(frames),
                    "; the WAL then becomes invalid at offset %d (%s)"
                    % (bad_offset, bad_reason)
                    if bad_offset is not None
                    else "",
                ),
                offset=bad_offset,
            )
    else:
        if target_frame > len(frames):
            if bad_offset is not None:
                why = (
                    "the WAL becomes invalid at offset %d (%s) before frame "
                    "%d is validated" % (bad_offset, bad_reason, target_frame)
                )
            else:
                why = "the WAL contains only %d validated frame(s)" % len(frames)
            raise RecoveryError(
                "target commit frame %d does not exist: %s" % (target_frame, why),
                offset=bad_offset,
            )
        commit = commit_frames.get(target_frame)
        if commit is None:
            target = frames[target_frame - 1]
            raise RecoveryError(
                "target frame %d is not a commit frame (commit database size "
                "is 0): it belongs to an uncommitted transaction and cannot "
                "be used as a recovery boundary" % target_frame,
                offset=target.offset,
            )

    # Recovery prefix: exactly the validated frames up to the chosen commit.
    prefix = frames[: commit.number]

    # Every frame in the prefix belongs to a transaction ending at a commit
    # frame; page numbers must not exceed the db size their transaction
    # commits. This is the "illegal page number" rule.
    tx_frames: list[FrameInfo] = []
    for fr in prefix:
        tx_frames.append(fr)
        if fr.db_size > 0:
            for member in tx_frames:
                if member.page_no > fr.db_size:
                    raise RecoveryError(
                        "frame %d writes page %d but its transaction commits "
                        "a database of only %d page(s)"
                        % (member.number, member.page_no, fr.db_size),
                        offset=member.offset,
                    )
            tx_frames = []

    # Keep the last image of each page within the prefix.
    page_size = hdr["page_size"]
    frame_size = FRAME_HEADER_SIZE + page_size
    wal_images: dict[int, bytes] = {}
    source_frame: dict[int, int] = {}
    for fr in prefix:
        start = WAL_HEADER_SIZE + (fr.number - 1) * frame_size + FRAME_HEADER_SIZE
        wal_images[fr.page_no] = wal[start : start + page_size]
        source_frame[fr.page_no] = fr.number

    if len(db) % page_size != 0:
        raise RecoveryError(
            "main database size %d is not a multiple of page size %d"
            % (len(db), page_size),
            offset=len(db) - (len(db) % page_size),
            scope="database",
        )

    images: dict[int, bytes] = {
        p: db[(p - 1) * page_size : p * page_size]
        for p in range(1, len(db) // page_size + 1)
    }
    images.update(wal_images)

    missing = [p for p in range(1, commit.db_size + 1) if p not in images]
    if missing:
        raise RecoveryError(
            "committed database needs %d pages but page(s) %s are in neither "
            "the main database nor the WAL prefix"
            % (commit.db_size, missing[:10])
        )

    image = b"".join(images[p] for p in range(1, commit.db_size + 1))
    return RecoveryResult(
        commit_frame=commit.number,
        db_size=commit.db_size,
        page_size=page_size,
        page_sources={
            p: source_frame.get(p, 0) for p in range(1, commit.db_size + 1)
        },
        image=image,
        digest=hashlib.sha256(image).hexdigest(),
        valid_frames=len(frames),
        wal_complete=bad_offset is None,
        first_invalid_offset=bad_offset,
        first_invalid_reason=bad_reason,
    )

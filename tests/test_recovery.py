"""Engine tests: real SQLite WALs, hand-built WALs and every failure path."""

import base64
import hashlib
import os
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from wal_recover import (  # noqa: E402
    FRAME_HEADER_SIZE,
    MAX_PAGE_SIZE,
    MAX_WAL_SIZE,
    MIN_PAGE_SIZE,
    WAL_HEADER_SIZE,
    RecoveryError,
    recover,
)
import walfixture  # noqa: E402

PAGE = 4096
FSZ = FRAME_HEADER_SIZE + PAGE


def tag_page(tag: int) -> bytes:
    return walfixture.fake_page(PAGE, b"P%06d" % tag, fill=(tag % 251) + 1)


def multi_commit_wal():
    """Three committed transactions over a database that grows to 3 pages.

    Only the last frame of each transaction carries a non-zero db size, as
    real SQLite writes it.
    """
    frames = [
        # tx1: page 1 + page 2, commit size 2
        {"page_no": 1, "db_size": 0, "page": tag_page(10)},
        {"page_no": 2, "db_size": 2, "page": tag_page(20)},
        # tx2: page 2 rewritten then page 3 grows the file, commit size 3
        {"page_no": 2, "db_size": 0, "page": tag_page(21)},
        {"page_no": 3, "db_size": 3, "page": tag_page(30)},
        # tx3: page 2 rewritten again, commit size 3
        {"page_no": 2, "db_size": 3, "page": tag_page(22)},
    ]
    db = walfixture.minimal_main_db(PAGE, pages=2)
    wal = walfixture.build_wal(PAGE, frames)
    return db, wal, frames


class EngineSyntheticTests(unittest.TestCase):
    def test_multi_commit_uses_last_complete_commit(self):
        db, wal, frames = multi_commit_wal()
        result = recover(db, wal)
        self.assertEqual(result.commit_frame, 5)
        self.assertEqual(result.db_size, 3)
        self.assertEqual(len(result.image), 3 * PAGE)
        self.assertTrue(result.wal_complete)
        self.assertIsNone(result.first_invalid_offset)

    def test_last_image_of_each_page_wins(self):
        db, wal, _ = multi_commit_wal()
        result = recover(db, wal)
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(22))  # page 2
        self.assertEqual(result.image[2 * PAGE:], tag_page(30))       # page 3
        self.assertEqual(result.image[:PAGE], tag_page(10))          # page 1

    def test_page_sources_point_at_last_occurrence(self):
        db, wal, _ = multi_commit_wal()
        result = recover(db, wal)
        self.assertEqual(result.page_sources, {1: 1, 2: 5, 3: 4})

    # ---- historical target commit selection -----------------------------

    def test_target_frame_first_commit_rebuilds_first_boundary(self):
        db, wal, _ = multi_commit_wal()  # commit frames: 2 (size 2), 4, 5
        result = recover(db, wal, target_frame=2)
        self.assertEqual(result.commit_frame, 2)
        self.assertEqual(result.db_size, 2)
        self.assertEqual(len(result.image), 2 * PAGE)
        self.assertEqual(result.image[:PAGE], tag_page(10))
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(20))
        self.assertEqual(result.page_sources, {1: 1, 2: 2})
        # Frames 3..5 (later commits) must not leak into the old boundary.
        self.assertNotIn(tag_page(21), result.image)
        self.assertNotIn(tag_page(22), result.image)
        self.assertNotIn(tag_page(30), result.image)

    def test_target_frame_middle_commit_uses_images_at_or_before_target(self):
        db, wal, _ = multi_commit_wal()
        result = recover(db, wal, target_frame=4)
        self.assertEqual(result.commit_frame, 4)
        self.assertEqual(result.db_size, 3)
        self.assertEqual(result.image[:PAGE], tag_page(10))
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(21))  # frame 3
        self.assertEqual(result.image[2 * PAGE:], tag_page(30))       # frame 4
        self.assertEqual(result.page_sources, {1: 1, 2: 3, 3: 4})
        self.assertNotIn(tag_page(22), result.image)  # frame 5 excluded

    def test_target_last_commit_matches_default_result(self):
        db, wal, _ = multi_commit_wal()
        defaulted = recover(db, wal)
        targeted = recover(db, wal, target_frame=5)
        self.assertEqual(targeted.commit_frame, defaulted.commit_frame)
        self.assertEqual(targeted.db_size, defaulted.db_size)
        self.assertEqual(targeted.image, defaulted.image)
        self.assertEqual(targeted.page_sources, defaulted.page_sources)
        self.assertEqual(targeted.digest, defaulted.digest)
        self.assertEqual(targeted.valid_frames, defaulted.valid_frames)
        self.assertEqual(
            targeted.to_report(page_order="numeric"),
            defaulted.to_report(page_order="numeric"),
        )

    def test_target_uncommitted_frame_is_refused_without_image(self):
        db, wal, _ = multi_commit_wal()  # frame 3 has db_size 0
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal, target_frame=3)
        self.assertIn("not a commit frame", ctx.exception.message)
        self.assertEqual(
            ctx.exception.offset, WAL_HEADER_SIZE + 2 * FSZ
        )  # frame 3 header

    def test_target_missing_frame_is_refused(self):
        db, wal, _ = multi_commit_wal()
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal, target_frame=99)
        self.assertIn("does not exist", ctx.exception.message)
        self.assertIsNone(ctx.exception.offset)

    def test_target_frame_validation(self):
        db, wal, _ = multi_commit_wal()
        for bad in (0, -1):
            with self.assertRaises(RecoveryError):
                recover(db, wal, target_frame=bad)
        with self.assertRaises(RecoveryError):
            recover(db, wal, target_frame=True)  # bool is not an ordinal

    def test_target_behind_corrupt_tail_still_recovers_historical_point(self):
        db, wal, _ = multi_commit_wal()
        bad = wal + walfixture.build_wal(
            PAGE,
            [{"page_no": 3, "db_size": 3, "page": tag_page(77)}],
        )[WAL_HEADER_SIZE:]
        bad = bytearray(bad)
        bad[-1] ^= 0xFF  # appended frame 6 fails its cumulative checksum
        # Target frame 5 (before the invalid tail) remains reachable.
        result = recover(db, bytes(bad), target_frame=5)
        self.assertEqual(result.commit_frame, 5)
        self.assertFalse(result.wal_complete)
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(22))
        self.assertNotIn(tag_page(77), result.image)
        # Target frame 6 is past the first invalid frame: explicit failure.
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, bytes(bad), target_frame=6)
        self.assertIn("does not exist", ctx.exception.message)
        self.assertEqual(
            ctx.exception.offset, WAL_HEADER_SIZE + 5 * FSZ + 16
        )

    def test_target_inside_truncated_tail_is_refused_but_commit_before_is_ok(self):
        db, wal, _ = multi_commit_wal()
        cut = wal[:-100]  # frame 5 incomplete; first invalid offset at frame 5
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, cut, target_frame=5)
        self.assertEqual(ctx.exception.offset, WAL_HEADER_SIZE + 4 * FSZ)
        result = recover(db, cut, target_frame=4)
        self.assertEqual(result.commit_frame, 4)
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(21))

    def test_target_ignores_uncommitted_frames_after_commit(self):
        frames = [
            {"page_no": 1, "db_size": 2, "page": tag_page(10)},
            {"page_no": 2, "db_size": 2, "page": tag_page(20)},
            {"page_no": 2, "db_size": 0, "page": tag_page(99)},
            {"page_no": 3, "db_size": 0, "page": tag_page(98)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=2)
        wal = walfixture.build_wal(PAGE, frames)
        result = recover(db, wal, target_frame=2)
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(20))
        with self.assertRaises(RecoveryError):
            recover(db, wal, target_frame=3)

    def test_report_orders_and_digest(self):
        db, wal, _ = multi_commit_wal()
        result = recover(db, wal)
        numeric = result.to_report(page_order="numeric")
        self.assertEqual([e["page"] for e in numeric["page_sources"]], [1, 2, 3])
        frame_order = result.to_report(page_order="frame")
        # page 1 (frame 1), page 3 (frame 4), page 2 (frame 5)
        self.assertEqual(
            [(e["page"], e["frame"]) for e in frame_order["page_sources"]],
            [(1, 1), (3, 4), (2, 5)],
        )
        self.assertEqual(numeric["digest"], hashlib.sha256(result.image).hexdigest())
        self.assertEqual(
            base64.b64decode(numeric["image_base64"]), result.image
        )
        self.assertEqual(numeric["recovered_pages"], 3)
        self.assertEqual(numeric["wal_pages"], 3)

    def test_uncommitted_trailing_frames_never_enter_image(self):
        frames = [
            {"page_no": 1, "db_size": 2, "page": tag_page(10)},
            {"page_no": 2, "db_size": 2, "page": tag_page(20)},
            # spilled frames of an open transaction: db size 0
            {"page_no": 2, "db_size": 0, "page": tag_page(99)},
            {"page_no": 3, "db_size": 0, "page": tag_page(98)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=2)
        wal = walfixture.build_wal(PAGE, frames)
        result = recover(db, wal)
        self.assertEqual(result.commit_frame, 2)
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(20))
        self.assertEqual(len(result.image), 2 * PAGE)
        self.assertNotIn(tag_page(99), result.image)
        self.assertNotIn(tag_page(98), result.image)

    def test_big_endian_checksum_magic_is_rejected(self):
        frames = [
            {"page_no": 1, "db_size": 1, "page": tag_page(1)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = walfixture.build_wal(PAGE, frames, big_endian_words=True)
        self.assertEqual(struct.unpack(">I", wal[:4])[0], 0x377F0683)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal)
        self.assertEqual(ctx.exception.offset, 0)
        self.assertIn("little-endian", ctx.exception.message)

    # ---- corruption / truncation ---------------------------------------

    def test_last_frame_checksum_corruption_recovers_previous_commit(self):
        db, wal, _ = multi_commit_wal()
        bad = wal + walfixture.build_wal(
            PAGE,
            [{"page_no": 3, "db_size": 3, "page": tag_page(77)}],
        )[WAL_HEADER_SIZE:]
        # Flip a payload byte of the appended frame so its checksum fails.
        bad = bytearray(bad)
        bad[-1] ^= 0xFF
        result = recover(db, bytes(bad))
        self.assertEqual(result.commit_frame, 5)
        self.assertFalse(result.wal_complete)
        self.assertEqual(
            result.first_invalid_offset,
            WAL_HEADER_SIZE + 5 * FSZ + 16,
        )
        self.assertIn("checksum", result.first_invalid_reason)
        self.assertNotIn(tag_page(77), result.image)

    def test_corrupt_frame_after_one_commit_still_returns_full_commit(self):
        frames = [
            {"page_no": 1, "db_size": 1, "page": tag_page(10)},
            {"page_no": 1, "db_size": 0, "page": tag_page(11)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = bytearray(walfixture.build_wal(PAGE, frames))
        wal[WAL_HEADER_SIZE + FSZ + 20] ^= 0x01  # break frame 2 checksum
        result = recover(db, bytes(wal))
        self.assertEqual(result.commit_frame, 1)
        self.assertEqual(result.image[:PAGE], tag_page(10))
        self.assertEqual(result.first_invalid_offset,
                         WAL_HEADER_SIZE + FSZ + 16)

    def test_truncated_trailing_frame_is_located(self):
        db, wal, _ = multi_commit_wal()
        cut = wal[:-100]
        result = recover(db, cut)
        # Frame 5 (tx3) is incomplete: recovery stops at the tx2 commit.
        self.assertEqual(result.commit_frame, 4)
        self.assertEqual(
            result.first_invalid_offset, WAL_HEADER_SIZE + 4 * FSZ
        )
        self.assertIn("truncated", result.first_invalid_reason)
        self.assertEqual(len(result.image), 3 * PAGE)
        self.assertEqual(result.image[PAGE:2 * PAGE], tag_page(21))
        self.assertNotIn(tag_page(22), result.image)

    def test_truncated_before_any_frame_is_unrecoverable(self):
        wal = walfixture.build_wal(PAGE, [])[:32] + b"\x00" * 10
        db = walfixture.minimal_main_db(PAGE, pages=1)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal)
        self.assertEqual(ctx.exception.offset, WAL_HEADER_SIZE)

    def test_salt_change_after_commit_is_reported_and_ignored(self):
        db, wal, _ = multi_commit_wal()
        rogue = walfixture.build_wal(
            PAGE,
            [{"page_no": 3, "db_size": 3, "page": tag_page(78)}],
            salt=(0xDEADBEEF, 0xCAFEBABE),
        )[WAL_HEADER_SIZE:]
        combined = wal + rogue
        result = recover(db, combined)
        self.assertEqual(result.commit_frame, 5)
        self.assertEqual(
            result.first_invalid_offset, WAL_HEADER_SIZE + 5 * FSZ + 8
        )
        self.assertNotIn(tag_page(78), result.image)

    def test_salt_change_before_first_commit_aborts_without_image(self):
        frames = [
            {"page_no": 1, "db_size": 0, "page": tag_page(1)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = walfixture.build_wal(
            PAGE, frames, salt=(0x11111111, 0x22222222)
        )
        wal = bytearray(wal)
        struct.pack_into(">I", wal, WAL_HEADER_SIZE + 8, 0x99999999)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, bytes(wal))
        self.assertEqual(ctx.exception.offset, WAL_HEADER_SIZE + 8)

    def test_illegal_page_number_zero_aborts(self):
        frames = [
            {"page_no": 0, "db_size": 1, "page": tag_page(1)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = walfixture.build_wal(PAGE, frames)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal)
        self.assertEqual(ctx.exception.offset, WAL_HEADER_SIZE)

    def test_page_beyond_committed_size_is_illegal(self):
        frames = [
            {"page_no": 1, "db_size": 2, "page": tag_page(1)},
            {"page_no": 9, "db_size": 2, "page": tag_page(9)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=2)
        wal = walfixture.build_wal(PAGE, frames)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal)
        self.assertEqual(ctx.exception.offset, WAL_HEADER_SIZE + FSZ)

    # ---- no commit / headers / limits ----------------------------------

    def test_no_commit_frame_is_unrecoverable(self):
        frames = [
            {"page_no": 1, "db_size": 0, "page": tag_page(1)},
            {"page_no": 2, "db_size": 0, "page": tag_page(2)},
        ]
        db = walfixture.minimal_main_db(PAGE, pages=2)
        wal = walfixture.build_wal(PAGE, frames)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal)
        self.assertIsNone(ctx.exception.offset)
        self.assertIn("no complete recoverable commit", ctx.exception.message)

    def test_empty_wal(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, b"")
        self.assertIn("empty", ctx.exception.message)

    def test_short_wal_header_locates_first_bad_byte(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, b"\x37\x7f\x06\x82")
        self.assertEqual(ctx.exception.offset, 4)

    def test_bad_magic(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = bytearray(walfixture.build_wal(PAGE, []))
        wal[0] = 0x00
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, bytes(wal))
        self.assertEqual(ctx.exception.offset, 0)

    def test_bad_version(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = bytearray(walfixture.build_wal(PAGE, []))
        struct.pack_into(">I", wal, 4, 3007001)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, bytes(wal))
        self.assertEqual(ctx.exception.offset, 4)

    def test_bad_header_checksum(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        wal = walfixture.build_wal(PAGE, [], corrupt_header_checksum=True)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, wal)
        self.assertEqual(ctx.exception.offset, 24)

    def test_page_size_limits(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        for ps in (MIN_PAGE_SIZE, MAX_PAGE_SIZE):
            small_db = walfixture.minimal_main_db(ps, pages=1)
            wal = walfixture.build_wal(
                ps, [{"page_no": 1, "db_size": 1,
                      "page": walfixture.fake_page(ps, b"X")}]
            )
            self.assertEqual(recover(small_db, wal).page_size, ps)
        for bad in (256, 8192, 1024 + 1):
            wal = bytearray(walfixture.build_wal(PAGE, []))
            struct.pack_into(">I", wal, 8, bad)
            with self.assertRaises(RecoveryError):
                recover(db, bytes(wal))

    def test_wal_size_limit(self):
        db = walfixture.minimal_main_db(PAGE, pages=1)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db, b"\x00" * (MAX_WAL_SIZE + 1))
        self.assertEqual(ctx.exception.offset, MAX_WAL_SIZE)

    def test_database_rejects_non_sqlite_and_page_size_mismatch(self):
        good_wal = walfixture.build_wal(PAGE, [])
        with self.assertRaises(RecoveryError) as ctx:
            recover(b"not a database" * 10, good_wal)
        self.assertEqual(ctx.exception.scope, "database")
        db512 = walfixture.minimal_main_db(512, pages=1)
        with self.assertRaises(RecoveryError) as ctx:
            recover(db512, good_wal)
        self.assertIn("page size mismatch", ctx.exception.message)


class RealSqliteTests(unittest.TestCase):
    """End-to-end checks against WAL bytes produced by real SQLite."""

    def setUp(self):
        try:
            import sqlite3  # noqa: F401
        except ImportError:
            self.skipTest("sqlite3 module unavailable")

    def _open_immutable(self, image: bytes):
        import sqlite3
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        with open(path, "wb") as fh:
            fh.write(image)
        con = sqlite3.connect("file:%s?immutable=1" % path, uri=True)
        self.addCleanup(con.close)
        self.addCleanup(lambda: os.unlink(path))
        return con

    def test_real_multi_transaction_wal_roundtrip(self):
        db, wal = walfixture.make_sqlite_wal(4096, (3, 40, 400))
        self.assertGreater(len(wal), WAL_HEADER_SIZE)
        result = recover(db, wal)
        con = self._open_immutable(result.image)
        (count,) = con.execute("SELECT count(*) FROM telemetry").fetchone()
        self.assertEqual(count, 3 + 40 + 400)
        # commit frame must be the last frame of a real commit (nonzero dbsize)
        frames, _ = walfixture.list_frames(wal)
        last_commit = max(f["number"] for f in frames if f["db_size"])
        self.assertEqual(result.commit_frame, last_commit)
        # every source frame agrees with the last frame touching that page
        last_seen = {}
        for f in frames[:last_commit]:
            last_seen[f["page_no"]] = f["number"]
        for page, src in result.page_sources.items():
            if src:
                self.assertEqual(src, last_seen[page])

    def test_real_wal_targets_early_commit_exactly(self):
        db, wal = walfixture.make_sqlite_wal(4096, (3, 40, 400))
        frames, _ = walfixture.list_frames(wal)
        commits = [f["number"] for f in frames if f["db_size"]]
        # Frame sequence also contains the initial schema commit (0 rows);
        # the three data transactions commit 3, 3+40 and 3+40+400 rows.
        first_data, second_data = commits[1], commits[2]
        at_first = recover(db, wal, target_frame=first_data)
        at_second = recover(db, wal, target_frame=second_data)
        con1 = self._open_immutable(at_first.image)
        con2 = self._open_immutable(at_second.image)
        try:
            self.assertEqual(
                con1.execute("SELECT count(*) FROM telemetry").fetchone()[0], 3
            )
            self.assertEqual(
                con2.execute("SELECT count(*) FROM telemetry").fetchone()[0], 3 + 40
            )
        finally:
            con1.close()
            con2.close()
        self.assertNotEqual(at_first.digest, at_second.digest)


        db, wal = walfixture.make_sqlite_wal(
            4096, (10,), spill_uncommitted=4000, cache_size=8
        )
        frames, _ = walfixture.list_frames(wal)
        if not any(f["db_size"] == 0 for f in frames):
            self.skipTest("SQLite did not spill uncommitted frames")
        result = recover(db, wal)
        con = self._open_immutable(result.image)
        committed = con.execute(
            "SELECT count(*) FROM telemetry WHERE marker LIKE 'committed-%'"
        ).fetchone()[0]
        uncommitted = con.execute(
            "SELECT count(*) FROM telemetry WHERE marker LIKE 'UNCOMMITTED-%'"
        ).fetchone()[0]
        self.assertEqual(committed, 10)
        self.assertEqual(uncommitted, 0)
        self.assertLess(result.commit_frame, len(frames))


if __name__ == "__main__":
    unittest.main(verbosity=2)

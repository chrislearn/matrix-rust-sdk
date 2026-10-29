import tempfile
from pathlib import Path
import unittest

from check_benchmark_profiles import validate

# Reduced actual combined-profile shape: metadata and termination are empty;
# the measured part has summary: 0 but positive totals and instruction costs.
PROFILE = """# callgrind format
version: 1
creator: callgrind-3.26.0
part: 1
desc: Trigger: Client Request: Metadata: codspeed-rust 4.7.0
positions: line
events: Ir Dr
summary: 0
totals: 0
part: 2
desc: Trigger: Client Request: benchmarks/benches/linked_chunk.rs::writing::sample
positions: line
events: Ir Dr
summary: 0
ob=/usr/lib/libc.so
fl=memmove.S
fn=memcpy
255 29 2
+1 4 1
totals: 33 3
part: 3
desc: Trigger: Program termination
positions: line
events: Ir Dr
summary: 0
totals: 0
"""


class OfflineProfileTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.folder = self.root / "profile.actual.out"
        self.folder.mkdir()
        self.file = self.folder / "3540.out"

    def check(self, text):
        self.file.write_text(text)
        return validate(self.root, "linked_chunk")

    def test_actual_combined_summary_zero_with_nonzero_totals_qualifies(self):
        result = self.check(PROFILE)
        self.assertEqual(result["measurements"], 1)
        self.assertTrue(result["names"][0].startswith("benchmarks/benches/linked_chunk.rs::"))

    def test_missing_empty_log_only_metadata_and_wrapper_do_not_qualify(self):
        for text in ["", "valgrind process exited successfully", PROFILE.split("part: 2")[0],
                     "part: 1\ndesc: Trigger: Program termination\nevents: Ir\ntotals: 99\n"]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.check(text)
        with self.assertRaises(ValueError):
            validate(self.root / "missing", "linked_chunk")
        self.file.unlink()
        (self.folder / "valgrind.3540.log").write_text("Measured: a log line alone is insufficient")
        with self.assertRaises(ValueError):
            validate(self.root, "linked_chunk")

    def test_truncated_or_missing_function_cost_or_totals_fails(self):
        for text in [PROFILE.split("totals: 33")[0], PROFILE.replace("totals: 33 3", "totals: NaN"),
                     PROFILE.replace("fn=memcpy", ""), PROFILE.replace("255 29 2\n+1 4 1", "")]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.check(text)

    def test_wrong_suite_or_integration_cannot_substitute_its_measurements(self):
        for text in [PROFILE.replace("linked_chunk.rs", "crypto_bench.rs"),
                     PROFILE.replace("Metadata: codspeed-rust 4.7.0", "Metadata: unknown 1")]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.check(text)

    def test_nonzero_other_events_or_summary_cannot_hide_zero_instructions(self):
        for text in [PROFILE.replace("totals: 33 3", "totals: 0 99"),
                     PROFILE.replace("events: Ir Dr", "events: Dr Dw"),
                     PROFILE.replace("totals: 33 3", "summary: 999 999")]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.check(text)

    def test_named_rows_without_a_callgrind_header_cannot_substitute_a_profile(self):
        with self.assertRaises(ValueError):
            self.check(PROFILE.replace("# callgrind format", "unrelated log output"))


if __name__ == "__main__":
    unittest.main()

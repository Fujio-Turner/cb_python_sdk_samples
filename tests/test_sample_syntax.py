"""Compile-check every sample script.

The rest of this suite mocks Couchbase APIs and often reimplements sample
logic instead of importing the scripts (many scripts connect at import time).
This test is the guard that a syntax error in a sample file fails CI.
"""
import py_compile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SAMPLE_FILES = [
    "01a_cb_set_get.py",
    "01b_cb_get_update_w_cas.py",
    "02_cb_upsert_delete.py",
    "03a_cb_query.py",
    "03b_cb_query_profile.py",
    "04_cb_sub_doc_ops.py",
    "05_cb_exception_handling.py",
    "06_cb_get_retry_replica_read.py",
    "07_cb_query_own_write.py",
    "08a_cb_transaction_kv.py",
    "08b_cb_transaction_query.py",
    "09_cb_fts_search.py",
    "fts/create_hotels_index.py",
    "10_cb_debug_tracing.py",
    "11_cb_async_operations.py",
    "12_cb_async_queries.py",
    "13_cb_increment.py",
    "advanced_prepared_statement_wrapper.py",
    "excel_to_json_to_cb.py",
    "ai_vector_sample/04_vector_search_using_python_sdk.py",
    "run_tests.py",
]


class TestSampleSyntax(unittest.TestCase):
    def test_listed_samples_exist(self):
        missing = [name for name in SAMPLE_FILES if not (ROOT / name).is_file()]
        self.assertEqual(missing, [], f"Sample paths missing: {missing}")

    def test_all_sample_python_compiles(self):
        errors = []
        for name in SAMPLE_FILES:
            path = ROOT / name
            try:
                py_compile.compile(str(path), doraise=True)
            except py_compile.PyCompileError as exc:
                errors.append(f"{name}: {exc}")
        self.assertEqual(errors, [], "Sample files failed to compile:\n" + "\n".join(errors))


if __name__ == "__main__":
    unittest.main()

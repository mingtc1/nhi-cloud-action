import json
import os
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import process_nhi


class FakeResponse:
    def __init__(self, status_code, body, headers=None):
        self.status_code = status_code
        self.body = body
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def iter_content(self, chunk_size):
        yield self.body


class DownloadNhiDataTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(f".test-process-nhi-{uuid.uuid4().hex}")
        self.tmpdir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_proxy_is_primary(self):
        response = FakeResponse(200, b"csv-data", {"X-Worker-Colo": "TPE"})
        with patch("requests.get", return_value=response) as request_get, patch(
            "process_nhi.time.sleep"
        ), patch.dict(
            os.environ,
            {"NHI_SOURCE_MODE": "proxy_then_direct", "NHI_PROXY_TOKEN": "secret"},
            clear=False,
        ):
            report = process_nhi.download_nhi_data(
                self.tmpdir / "nhi.csv", self.tmpdir / "report.json"
            )

        self.assertEqual(report["source"], "cloudflare_proxy")
        self.assertEqual(report["proxy_colo"], "TPE")
        self.assertEqual(request_get.call_count, 1)
        self.assertTrue(request_get.call_args.kwargs["verify"])

    def test_direct_fallback_after_proxy_failure(self):
        responses = [
            FakeResponse(502, b'{"error":"upstream"}'),
            FakeResponse(502, b'{"error":"upstream"}'),
            FakeResponse(200, b"csv-data"),
        ]
        with patch("requests.get", side_effect=responses) as request_get, patch(
            "process_nhi.time.sleep"
        ), patch.dict(
            os.environ,
            {"NHI_SOURCE_MODE": "proxy_then_direct", "NHI_PROXY_TOKEN": "secret"},
            clear=False,
        ):
            report_path = self.tmpdir / "report.json"
            report = process_nhi.download_nhi_data(
                self.tmpdir / "nhi.csv", report_path
            )
            with open(report_path, encoding="utf-8") as report_file:
                saved_report = json.load(report_file)

        self.assertEqual(report["source"], "nhi_official_direct")
        self.assertEqual(saved_report["status"], "success")
        self.assertEqual(request_get.call_count, 3)
        self.assertFalse(request_get.call_args.kwargs["verify"])


if __name__ == "__main__":
    unittest.main()

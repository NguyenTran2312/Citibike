import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from ingestion.gbfs.poller import fetch_json


class TestGBFSRetry(unittest.TestCase):
    URL = "https://example.com/gbfs.json"

    @patch("ingestion.gbfs.poller.time.sleep")
    @patch("ingestion.gbfs.poller.urlopen")
    def test_recovers_after_temporary_network_error(self, request, sleep):
        request.side_effect = [
            URLError("temporary failure"),
            io.BytesIO(b'{"version":"2.3"}'),
        ]

        with self.assertLogs("ingestion.gbfs.poller", level="WARNING"):
            self.assertEqual(fetch_json(self.URL), {"version": "2.3"})

        self.assertEqual(request.call_count, 2)
        self.assertEqual(sleep.call_count, 1)
        self.assertTrue(0 <= sleep.call_args.args[0] <= 1)

    @patch("ingestion.gbfs.poller.time.sleep")
    @patch("ingestion.gbfs.poller.urlopen")
    def test_stops_after_three_timeouts(self, request, sleep):
        request.side_effect = TimeoutError("request timed out")

        with self.assertLogs("ingestion.gbfs.poller", level="WARNING"):
            with self.assertRaises(TimeoutError):
                fetch_json(self.URL)

        self.assertEqual(request.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        first, second = [call.args[0] for call in sleep.call_args_list]
        self.assertTrue(0 <= first <= 1)
        self.assertTrue(0 <= second <= 2)

    @patch("ingestion.gbfs.poller.time.sleep")
    @patch("ingestion.gbfs.poller.urlopen")
    def test_does_not_retry_http_404(self, request, sleep):
        request.side_effect = HTTPError(
            self.URL, 404, "Not Found", {}, io.BytesIO()
        )

        with self.assertRaises(HTTPError) as raised:
            fetch_json(self.URL)

        self.assertEqual(raised.exception.code, 404)
        request.assert_called_once()
        sleep.assert_not_called()


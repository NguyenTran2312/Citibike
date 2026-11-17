import unittest
from unittest.mock import Mock, patch

from ingestion.gbfs.poller import run_feed_loop


class TestPollingLoop(unittest.TestCase):
    def run_one_iteration(
        self,
        interval,
        elapsed=2,
        error=None,
    ):
        stop_event = Mock()
        stop_event.is_set.return_value = False

        # Sau lần đợi đầu tiên, giả lập có yêu cầu dừng.
        stop_event.wait.return_value = True

        with (
            patch(
                "ingestion.gbfs.poller.poll_feed"
            ) as poll,
            patch(
                "ingestion.gbfs.poller.time.monotonic",
                side_effect=[100, 100 + elapsed],
            ),
        ):
            if error is not None:
                poll.side_effect = error

            run_feed_loop(
                "station_status",
                "https://example.com/feed.json",
                interval,
                stop_event,
            )

        return poll, stop_event

    def test_wait_accounts_for_processing_time(self):
        for interval in (60, 86400):
            with self.subTest(interval=interval):
                poll, stop = self.run_one_iteration(
                    interval=interval,
                    elapsed=2,
                )

                poll.assert_called_once_with(
                    "station_status",
                    "https://example.com/feed.json",
                )
                stop.wait.assert_called_once_with(interval - 2)

    def test_slow_poll_does_not_create_negative_wait(self):
        poll, stop = self.run_one_iteration(
            interval=60,
            elapsed=70,
        )

        poll.assert_called_once()
        stop.wait.assert_called_once_with(1.0)

    def test_poll_failure_is_logged_and_waits_before_next_cycle(self):
        with self.assertLogs(
            "ingestion.gbfs.poller",
            level="ERROR",
        ) as logs:
            poll, stop = self.run_one_iteration(
                interval=60,
                elapsed=2,
                error=RuntimeError("Simulated network failure"),
            )

        poll.assert_called_once()
        stop.wait.assert_called_once_with(58)
        self.assertIn(
            "Polling thất bại",
            "\n".join(logs.output),
        )

    def test_already_stopped_does_not_poll(self):
        stop_event = Mock()
        stop_event.is_set.return_value = True

        with patch("ingestion.gbfs.poller.poll_feed") as poll:
            run_feed_loop(
                "station_status",
                "https://example.com/feed.json",
                60,
                stop_event,
            )

        poll.assert_not_called()
        stop_event.wait.assert_not_called()


if __name__ == "__main__":
    unittest.main()

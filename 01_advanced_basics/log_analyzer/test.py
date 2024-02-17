import os
import unittest

from log_analyzer import (
    config,
    _find_newest_log,
    _get_prepared_data,
    _calculate_stat,
    _write_report
)


class TestLogAnalyzer(unittest.TestCase):

    def test_create_report(self) -> None:
        log = _find_newest_log(config)
        self.assertEqual(log.filename, 'nginx-access-ui.log-20170630.gz')
        raw_data = _get_prepared_data(log, config)
        self.assertEqual(raw_data.total_urls_count, 2613659)
        report = _calculate_stat(raw_data.total_urls_count, raw_data.total_request_time, raw_data.urls_stat, config)
        self.assertEqual(report[0]['time_avg'], 62.995)
        _write_report(report, log, config)
        self.assertIn(f'report-{log.date}.html', os.listdir(config["REPORT_DIR"]))


if __name__ == '__main__':
    unittest.main()

from datetime import datetime, timedelta
from unittest import mock

import pytest

from ckanext.harvest.logic.action.update import _calculate_next_run


def _now(value):
    return mock.patch(
        'ckanext.harvest.logic.action.update._utcnow', return_value=value)


class TestCalculateNextRun(object):

    def test_daily_run_keeps_schedule(self):
        # the scheduled run at 8:00 is processed a few seconds later
        previous = datetime(2026, 9, 28, 8, 0, 0)
        with _now(datetime(2026, 9, 28, 8, 0, 4)):
            next_run = _calculate_next_run('DAILY', previous)
        assert next_run == datetime(2026, 9, 29, 8, 0, 0)

    def test_daily_run_processed_late_keeps_schedule(self):
        # e.g. the run command is called every 15 minutes
        previous = datetime(2026, 9, 28, 8, 0, 0)
        with _now(datetime(2026, 9, 28, 8, 14, 59)):
            next_run = _calculate_next_run('DAILY', previous)
        assert next_run == datetime(2026, 9, 29, 8, 0, 0)

    def test_missed_runs_are_skipped(self):
        # no jobs were scheduled for three days
        previous = datetime(2026, 9, 25, 8, 0, 0)
        with _now(datetime(2026, 9, 28, 9, 30, 0)):
            next_run = _calculate_next_run('DAILY', previous)
        assert next_run == datetime(2026, 9, 29, 8, 0, 0)

    @pytest.mark.parametrize('frequency, expected', [
        ('WEEKLY', datetime(2026, 10, 5, 8, 0, 0)),
        ('BIWEEKLY', datetime(2026, 10, 12, 8, 0, 0)),
        ('MONTHLY', datetime(2026, 10, 28, 8, 0, 0)),
    ])
    def test_other_frequencies_keep_schedule(self, frequency, expected):
        previous = datetime(2026, 9, 28, 8, 0, 0)
        with _now(datetime(2026, 9, 28, 8, 5, 0)):
            assert _calculate_next_run(frequency, previous) == expected

    def test_monthly_uses_length_of_month(self):
        previous = datetime(2028, 2, 1, 8, 0, 0)
        with _now(datetime(2028, 2, 1, 8, 5, 0)):
            next_run = _calculate_next_run('MONTHLY', previous)
        # 2028 is a leap year
        assert next_run == datetime(2028, 3, 1, 8, 0, 0)

    def test_first_run_is_calculated_from_now(self):
        now = datetime(2026, 9, 28, 8, 0, 4)
        with _now(now):
            next_run = _calculate_next_run('DAILY')
        assert next_run == now + timedelta(days=1)

    def test_always_runs_now(self):
        now = datetime(2026, 9, 28, 8, 0, 4)
        with _now(now):
            assert _calculate_next_run(
                'ALWAYS', datetime(2026, 9, 27, 8, 0, 0)) == now

    def test_unknown_frequency(self):
        with pytest.raises(Exception, match='Frequency ANNUALLY not recognised'):
            _calculate_next_run('ANNUALLY', datetime(2026, 9, 28, 8, 0, 0))

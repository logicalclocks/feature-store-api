#
#   Copyright 2023 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from hsfs.core import monitoring_window_config_engine as mwce
from hsfs.core import monitoring_window_config as mwc
from hsfs.util import convert_event_time_to_timestamp

import pytest
from datetime import timedelta, datetime


class TestMonitoringWindowConfigEngine:
    def test_time_range_str_to_time_delta(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        all_one_expression = "1w1d1h"
        negative_expression = "-1w-1d-1h"
        disordered_expression = "2h1d1w"
        double_expression = "2h3d"
        just_days_expression = "3d"
        just_hours_expression = "4h"
        just_weeks_expression = "5w"
        just_minutes_expression = "6m"
        just_seconds_expression = "7s"
        just_months_expression = "8M"
        just_years_expression = "9y"

        # Act
        all_one = monitoring_window_config_engine.time_range_str_to_time_delta(
            all_one_expression
        )
        just_days = monitoring_window_config_engine.time_range_str_to_time_delta(
            just_days_expression
        )
        just_hours = monitoring_window_config_engine.time_range_str_to_time_delta(
            just_hours_expression
        )
        just_weeks = monitoring_window_config_engine.time_range_str_to_time_delta(
            just_weeks_expression
        )
        disordered = monitoring_window_config_engine.time_range_str_to_time_delta(
            disordered_expression
        )
        double = monitoring_window_config_engine.time_range_str_to_time_delta(
            double_expression
        )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine.time_range_str_to_time_delta(
                negative_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine.time_range_str_to_time_delta(
                just_minutes_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine.time_range_str_to_time_delta(
                just_seconds_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine.time_range_str_to_time_delta(
                just_months_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine.time_range_str_to_time_delta(
                just_years_expression, "window_length"
            )

        # Assert
        assert isinstance(all_one, timedelta)
        assert all_one == timedelta(days=1, hours=1, weeks=1)
        assert isinstance(disordered, timedelta)
        assert disordered == timedelta(days=1, hours=2, weeks=1)
        assert isinstance(double, timedelta)
        assert double == timedelta(days=3, hours=2)
        assert isinstance(just_days, timedelta)
        assert just_days == timedelta(days=3)
        assert isinstance(just_hours, timedelta)
        assert just_hours == timedelta(hours=4)
        assert isinstance(just_weeks, timedelta)
        assert just_weeks == timedelta(weeks=5)

    def test_get_window_start_end_times_all_time(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ALL_TIME,
        )

        # Act
        before_time = convert_event_time_to_timestamp(datetime.now())
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine.get_window_start_end_times(config)
        after_time = convert_event_time_to_timestamp(datetime.now())

        # Assert
        assert start_time is None
        assert before_time <= end_time <= after_time

    def test_get_window_start_end_times_rolling_time_no_window_length(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1w1d1h",
        )

        # Act
        before_time = convert_event_time_to_timestamp(datetime.now())
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine.get_window_start_end_times(config)
        after_time = convert_event_time_to_timestamp(datetime.now())

        # Assert
        assert (
            before_time
            <= (
                start_time
                + (timedelta(weeks=1, days=1, hours=1).total_seconds() * 1000)
            )
            <= after_time
        )
        assert before_time <= end_time <= after_time

    def test_get_window_start_end_times_rolling_time_short_window_length(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="2w1d",
            window_length="1d",
        )

        # Act
        before_time = convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine.get_window_start_end_times(config)
        after_time = convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert (
            before_time
            <= start_time + (timedelta(weeks=2, days=1).total_seconds() * 1000)
            <= after_time
        )
        assert (
            before_time
            <= end_time + (timedelta(weeks=2).total_seconds() * 1000)
            <= after_time
        )

    def test_get_window_start_end_times_rolling_time_long_window_length(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1d",
            window_length="48h",
        )

        # Act
        before_time = convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine.get_window_start_end_times(config)
        after_time = convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert (
            before_time
            <= start_time + (timedelta(days=1).total_seconds() * 1000)
            <= after_time
        )
        assert before_time <= end_time <= after_time

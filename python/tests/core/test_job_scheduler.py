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


from hsfs.core import job_scheduler
import pandas as pd

DEFAULT_TIMESTAMP_DATE_STR = "2023-01-01 00:00:00"


class TestJobScheduler:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job_scheduler"]["get"]["response"]

        # Act
        schedule = job_scheduler.JobScheduler.from_response_json(json)

        # Assert
        assert schedule.id == 222
        assert schedule.start_date_time == 1672569000000
        assert schedule.job_frequency == "DAILY"
        assert schedule.enabled is True
        assert (
            schedule.job_name
            == "fg_or_fv_name_version_fm_config_name_run_feature_monitoring"
        )
        assert schedule.cron_expression == "0 0 10 ? * *"
        assert schedule.next_execution_date_time == 1898589600000
        assert schedule.end_date_time == 1893493800000

    def test_from_response_json_no_end_time(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job_scheduler"]["get_no_end_time"]["response"]

        # Act
        schedule = job_scheduler.JobScheduler.from_response_json(json)

        # Assert
        assert schedule.id == 222
        assert schedule.start_date_time == 1672569000000
        assert schedule.job_frequency == "HOURLY"
        assert schedule.enabled is True
        assert (
            schedule.job_name
            == "fg_or_fv_name_version_fm_config_name_run_feature_monitoring"
        )
        assert schedule.cron_expression == "0 20 * ? * *"
        assert schedule.next_execution_date_time == 1898589600000
        assert schedule.end_date_time is None

    def test_from_response_json_disabled(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job_scheduler"]["get_disabled_schedule"]["response"]

        # Act
        schedule = job_scheduler.JobScheduler.from_response_json(json)

        # Assert
        assert schedule.id == 222
        assert schedule.start_date_time == 1672569000000
        assert schedule.job_frequency == "DAILY"
        assert schedule.enabled is False
        assert (
            schedule.job_name
            == "fg_or_fv_name_version_fm_config_name_run_feature_monitoring"
        )
        assert schedule.cron_expression == "0 0 10 ? * *"
        assert schedule.next_execution_date_time is None
        assert schedule.end_date_time == 1893493800000

    def test_on_local_init_with_timestamp(self):
        # Arrange
        timestamp = pd.Timestamp(DEFAULT_TIMESTAMP_DATE_STR)
        kwargs = {
            "start_date_time": round(timestamp.timestamp() * 1000),
            "job_frequency": "HOURLY",
            "enabled": True,
        }

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)

        # Assert
        assert schedule.id is None
        assert schedule.start_date_time == round(timestamp.value / 1e6)
        assert schedule.job_frequency == "HOURLY"
        assert schedule.enabled is True
        assert schedule.job_name is None

    def test_on_local_init_with_datetime(self):
        # Arrange
        timestamp = pd.Timestamp(DEFAULT_TIMESTAMP_DATE_STR)
        kwargs = {
            "start_date_time": timestamp.to_pydatetime(),
            "job_frequency": "HOURLY",
            "enabled": True,
        }

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)

        # Assert
        assert schedule.id is None
        assert schedule.start_date_time == round(timestamp.value / 1e6)
        assert schedule.job_frequency == "HOURLY"
        assert schedule.enabled is True
        assert schedule.job_name is None

    def test_on_local_init_with_int(self):
        # Arrange
        timestamp = pd.Timestamp(DEFAULT_TIMESTAMP_DATE_STR)
        kwargs = {
            "start_date_time": round(timestamp.value / 1e6),
            "cron_expression": "0 10 10 ? * Mon",
            "enabled": True,
        }

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)

        # Assert
        assert schedule.id is None
        assert schedule.start_date_time == round(timestamp.value / 1e6)
        assert schedule.job_frequency == "WEEKLY"
        assert schedule.cron_expression == "0 10 10 ? * Mon"
        assert schedule.enabled is True
        assert schedule.job_name is None

    def test_on_local_init_with_str(self):
        # Arrange
        timestamp = pd.Timestamp(DEFAULT_TIMESTAMP_DATE_STR)
        kwargs = {
            "start_date_time": DEFAULT_TIMESTAMP_DATE_STR,
            "cron_expression": "0 0 0 ? * Mon-Fre",
            "enabled": False,
        }

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)

        # Assert
        assert schedule.id is None
        assert schedule.start_date_time == round(timestamp.value / 1e6)
        assert schedule.job_frequency == "CUSTOM"
        assert schedule.enabled is False
        assert schedule.job_name is None

    def test_job_frequency_on_cron_expression(self):
        # Arrange
        kwargs = {
            "start_date_time": DEFAULT_TIMESTAMP_DATE_STR,
            "cron_expression": "0 0 0 ? * Mon-Fri",
            "enabled": False,
        }
        cron_expressions = []
        expected = []
        cron_expressions.append("0 */10 * ? * *")  # NEAR REAL-TIME
        cron_expressions.append("0 10/10 * ? * *")  # NEAR REAL-TIME
        expected.extend(["NEAR REAL-TIME", "NEAR REAL-TIME"])
        cron_expressions.append("0 10 * ? * *")  # hourly
        cron_expressions.append("0 30 * ? * *")  # hourly
        expected.extend(["HOURLY", "HOURLY"])
        cron_expressions.append("0 40 19 ? * *")  # daily
        cron_expressions.append("0 10 0 ? * *")  # daily
        expected.extend(["DAILY", "DAILY"])
        cron_expressions.append("0 40 19 ? * Mon")  # weekly
        cron_expressions.append("0 10 0 ? * 5")  # weekly
        expected.extend(["WEEKLY", "WEEKLY"])
        # CUSTOM
        cron_expressions.append("0 10,20 * ? * *")  # custom
        cron_expressions.append("0 0 0 1 * ?")  # monthly
        cron_expressions.append("0 0 0 1 1 ?")  # yearly
        cron_expressions.append("0 30 10 ? * 1-5")  # weekdays
        cron_expressions.append("0 20 14 ? * 6,7")  # weekends
        cron_expressions.append("0 0 0 1-7 * ?")  # daily for first 7 days of month
        cron_expressions.append("0 0 0 ? * 1#2")  # second Monday of the month
        expected.extend(
            ["CUSTOM", "CUSTOM", "CUSTOM", "CUSTOM", "CUSTOM", "CUSTOM", "CUSTOM"]
        )

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)
        job_frequencies = []
        for cron_expression in cron_expressions:
            schedule.cron_expression = cron_expression
            job_frequencies.append(schedule.job_frequency)

        # Assert
        assert all(
            [
                job_frequency == expected_job_frequency
                for job_frequency, expected_job_frequency in zip(
                    job_frequencies, expected
                )
            ]
        )

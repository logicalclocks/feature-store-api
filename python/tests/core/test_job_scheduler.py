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

DEFAULT_TIMESTAMP_DATE_STR = "2021-01-01 00:00:00"


class TestJobScheduler:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job_scheduler"]["get"]["response"]

        # Act
        schedule = job_scheduler.JobScheduler.from_response_json(json)

        # Assert
        assert schedule.id == 222
        assert schedule.start_date_time == 1676457000 * 1000
        assert schedule.job_frequency == "DAILY"
        assert schedule.enabled is True
        assert (
            schedule.job_name
            == "fg_or_fv_name_version_fm_config_name_run_feature_monitoring"
        )
        assert (
            schedule._href
            == "https://hopsworks.ai/hopsworks-api/project/33/jobs/111/schedulev2/222"
        )

    def test_on_local_init_with_timestamp(self):
        # Arrange
        timestamp = pd.Timestamp("2021-01-01 00:00:00")
        kwargs = {
            "start_date_time": timestamp,
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
        assert schedule._href is None

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
        assert schedule._href is None

    def test_on_local_init_with_int(self):
        # Arrange
        timestamp = pd.Timestamp(DEFAULT_TIMESTAMP_DATE_STR)
        kwargs = {
            "start_date_time": round(timestamp.value),
            "job_frequency": "WEEKLY",
            "enabled": True,
        }

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)

        # Assert
        assert schedule.id is None
        assert schedule.start_date_time == round(timestamp.value)
        assert schedule.job_frequency == "WEEKLY"
        assert schedule.enabled is True
        assert schedule.job_name is None
        assert schedule._href is None

    def test_on_local_init_with_str(self):
        # Arrange
        timestamp = pd.Timestamp(DEFAULT_TIMESTAMP_DATE_STR)
        kwargs = {
            "start_date_time": DEFAULT_TIMESTAMP_DATE_STR,
            "job_frequency": "DAILY",
            "enabled": False,
        }

        # Act
        schedule = job_scheduler.JobScheduler(**kwargs)

        # Assert
        assert schedule.id is None
        assert schedule.start_date_time == round(timestamp.value / 1e6)
        assert schedule.job_frequency == "DAILY"
        assert schedule.enabled is False
        assert schedule.job_name is None
        assert schedule._href is None

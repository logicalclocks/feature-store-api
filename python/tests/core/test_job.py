#
#   Copyright 2022 Hopsworks AB
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


import pytest
from hsfs.client import exceptions
from hsfs.core import execution, job


class TestJob:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job"]["get"]["response"]

        # Act
        j = job.Job.from_response_json(json)

        # Assert
        assert j.id == "test_id"
        assert j.name == "test_name"
        assert j.executions == "test_executions"
        assert j.href == "test_href"

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job"]["get_empty"]["response"]

        # Act
        j = job.Job.from_response_json(json)

        # Assert
        assert j.id == "test_id"
        assert j.name == "test_name"
        assert j.executions is None
        assert j.href is None

    def test_wait_for_job(self, mocker, backend_fixtures):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        json = backend_fixtures["job"]["get"]["response"]
        j = job.Job.from_response_json(json)

        # Act
        j._wait_for_job()

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1

    def test_wait_for_job_wait_for_job_false(self, mocker, backend_fixtures):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        json = backend_fixtures["job"]["get"]["response"]
        j = job.Job.from_response_json(json)

        # Act
        j._wait_for_job(False)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 0

    def test_wait_for_job_final_status_succeeded(self, mocker, backend_fixtures):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        json = backend_fixtures["job"]["get"]["response"]
        j = job.Job.from_response_json(json)

        mock_job_api.return_value.last_execution.return_value = [
            execution.Execution(id=1, state=None, final_status="succeeded")
        ]

        # Act
        j._wait_for_job()

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1

    def test_wait_for_job_final_status_failed(self, mocker, backend_fixtures):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        json = backend_fixtures["job"]["get"]["response"]
        j = job.Job.from_response_json(json)

        mock_job_api.return_value.last_execution.return_value = [
            execution.Execution(id=1, state=None, final_status="failed")
        ]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            j._wait_for_job()

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1
        assert (
            str(e_info.value)
            == "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
        )

    def test_wait_for_job_final_status_killed(self, mocker, backend_fixtures):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        json = backend_fixtures["job"]["get"]["response"]
        j = job.Job.from_response_json(json)

        mock_job_api.return_value.last_execution.return_value = [
            execution.Execution(id=1, state=None, final_status="killed")
        ]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            j._wait_for_job()

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1
        assert str(e_info.value) == "The Hopsworks Job was stopped"

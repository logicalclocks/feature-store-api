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


from hsfs.core import job


class TestJob:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job"]["get"]["response"]

        # Act
        j = job.Job.from_response_json(json)

        # Assert
        assert j.id == 111
        assert j.name == "test_job_name"
        assert j.executions == "test_executions"
        assert j.href == "test_href"

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["job"]["get_empty"]["response"]

        # Act
        j = job.Job.from_response_json(json)

        # Assert
        assert j.id == 111
        assert j.name == "test_job_name"
        assert j.executions is None
        assert j.href is None

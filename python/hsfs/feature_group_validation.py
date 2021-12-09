#
#   Copyright 2020 Logical Clocks AB
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

import humps
import json

from hsfs import util


class FeatureGroupValidation:
    """Metadata object representing the validation result of a feature group.

    Refer to expectation_result for individual feature group expectation results.
    """

    def __init__(
        self,
        validation_time,
        expectation_results,
        validation_id=None,
        status=None,
        validation_path=None,
        commit_time=None,
        log_activity=True,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._validation_id = validation_id
        self._validation_time = validation_time
        self._status = status
        self._expectation_results = expectation_results
        self._validation_path = validation_path
        self._commit_time = commit_time
        self._log_activity = log_activity

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**feature_group_validation)
                for feature_group_validation in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "validationId": self._validation_id,
            "validationTime": self._validation_time,
            "expectationResults": self._expectation_results,
            "logActivity": self._log_activity,
        }

    @property
    def validation_id(self):
        """Unique id of the feature group validation."""
        return self._validation_id

    @validation_id.setter
    def validation_id(self, validation_id):
        self._validation_id = validation_id

    @property
    def validation_time(self):
        """Timestamp in seconds of when feature validation started."""
        return self._validation_time

    @validation_time.setter
    def validation_time(self, validation_time):
        self._validation_time = validation_time

    @property
    def status(self):
        """Status of the expectation after feature ingestion, one of "NONE", "SUCCESS", "WARNING", "FAILURE"."""
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    @property
    def expectation_results(self):
        """List of expectation results."""
        return self._expectation_results

    @expectation_results.setter
    def expectation_results(self, expectation_results):
        self._expectation_results = expectation_results

    @property
    def validation_path(self):
        """Path in the Hopsworks datasets where the feature group validation results are persisted."""
        return self._validation_path

    @validation_path.setter
    def validation_path(self, validation_path):
        self._validation_path = validation_path

    @property
    def commit_time(self):
        """Timestamp in seconds of when the feature dataframe was committed (time-travel FGs only)."""
        return self._commit_time

    @commit_time.setter
    def commit_time(self, commit_time):
        self._commit_time = commit_time

    @property
    def log_activity(self):
        """Whether to log the validation as a feature group activity. Default to True. Used internally in hsfs"""
        return self._log_activity

    @log_activity.setter
    def log_activity(self, log_activity):
        self._log_activity = log_activity

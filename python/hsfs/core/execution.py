#
#   Copyright 2021 Logical Clocks AB
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


class Execution:
    def __init__(
        self,
        id,
        state,
        final_status,
        submission_time=None,
        stdout_path=None,
        stderr_path=None,
        app_id=None,
        hdfs_user=None,
        args=None,
        progress=None,
        user=None,
        files_to_remove=None,
        duration=None,
        flink_master_url=None,
        monitoring=None,
        type=None,
        href=None,
    ):
        self._id = id
        self._final_status = final_status
        self._state = state

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        return [cls(**execution) for execution in json_decamelized["items"]]

    @property
    def id(self):
        return self._id

    @property
    def final_status(self):
        return self._final_status

    @property
    def state(self):
        return self._state

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

from hsfs import client
from hsfs.core import fs_query


class QueryConstructorApi:
    def construct_query(self, query):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "featurestores", "query"]
        headers = {"content-type": "application/json"}
        return fs_query.FsQuery.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=query.json()
            )
        )

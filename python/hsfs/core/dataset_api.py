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

from hsfs import client, util
from hsfs.core import inode


class DatasetApi:

    def upload(self, feature_group, path, dataframe):
        # Convert the dataframe into PARQUET for upload
        df_parquet = dataframe.to_parquet(index=False)
        parquet_length = len(df_parquet)
        
        self._upload_request(
            path,
            util.feature_group_name(feature_group),
            df_parquet,
            parquet_length,
        )

    def _upload_request(self, path, file_name, file, file_size):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", "v2", "upload", path]

        _client._send_request(
            "POST", path_params, files={"file": (file_name, file), "fileName": file_name, "fileSize": file_size}
        )

    def list_files(self, path, offset, limit):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            path[(path.index("/", 10) + 1) :],
        ]
        query_params = {
            "action": "listing",
            "offset": offset,
            "limit": limit,
            "sort_by": "ID:asc",
        }

        inode_lst = _client._send_request("GET", path_params, query_params)

        return inode_lst["count"], inode.Inode.from_response_json(inode_lst)

    def read_content(self, path):
        _client = client.get_instance()

        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "download",
            "with_auth",
            path[1:],
        ]

        return _client._send_request("GET", path_params, stream=True)

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

import math

from hsfs import client, util


class DatasetApi:
    DEFAULT_FLOW_CHUNK_SIZE = 1048576

    def upload(self, feature_group, path, dataframe):
        # Convert the dataframe into CSV for upload
        df_csv = dataframe.to_csv(index=False)
        num_chunks = math.ceil(len(df_csv) / self.DEFAULT_FLOW_CHUNK_SIZE)

        base_params = self._get_flow_base_params(feature_group, num_chunks, len(df_csv))

        chunks = [
            df_csv[i : i + self.DEFAULT_FLOW_CHUNK_SIZE]
            for i in range(0, len(df_csv), self.DEFAULT_FLOW_CHUNK_SIZE)
        ]

        chunk_number = 1
        for chunk in chunks:
            query_params = base_params
            query_params["flowCurrentChunkSize"] = len(chunk)
            query_params["flowChunkNumber"] = chunk_number

            self._upload_request(
                query_params, path, util.feature_group_name(feature_group), chunk
            )

            chunk_number += 1

    def _get_flow_base_params(self, feature_group, num_chunks, size):
        # TODO(fabio): flow identifier is not unique
        return {
            "templateId": -1,
            "flowChunkSize": self.DEFAULT_FLOW_CHUNK_SIZE,
            "flowTotalSize": size,
            "flowIdentifier": util.feature_group_name(feature_group),
            "flowFilename": util.feature_group_name(feature_group),
            "flowRelativePath": util.feature_group_name(feature_group),
            "flowTotalChunks": num_chunks,
        }

    def _upload_request(self, params, path, file_name, chunk):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", "upload", path]

        # Flow configuration params are sent as form data
        _client._send_request(
            "POST", path_params, data=params, files={"file": (file_name, chunk)}
        )

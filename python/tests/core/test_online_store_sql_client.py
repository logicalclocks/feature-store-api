#
#   Copyright 2024 Hopsworks AB
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
import hsfs


class TestOnlineStoreSqlClient:
    def test_init(self, mocker):
        # Arrange
        feature_store_id = 1
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        # Act
        online_store_sql_client = (
            hsfs.core.online_store_sql_client.OnlineStoreSqlClient(
                feature_store_id=feature_store_id
            )
        )

        # Assert
        assert online_store_sql_client._feature_store_id == feature_store_id

    # def test_init_serving(self):
    #     # Arrange
    #     online_store_sql_client = hsfs.core.online_store_sql_client.OnlineStoreSqlClient(1)

    #     # Act
    #     online_store_sql_client.init_serving()

    #     # Assert
    #     assert online_store_sql_client._prepared_statement is not None

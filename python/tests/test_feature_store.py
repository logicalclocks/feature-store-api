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


from hsfs import feature_store


class TestFeatureStore:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]

        # Act
        fs = feature_store.FeatureStore.from_response_json(json)

        # Assert
        assert fs.id == 11
        assert fs.name == "test_featurestore_name"
        assert fs._created == "test_created"
        assert fs.project_name == "test_project_name"
        assert fs.project_id == 67
        assert fs._online_feature_store_name == "test_online_featurestore_name"
        assert fs._online_feature_store_size == 31
        assert fs._offline_feature_store_name == "test_offline_featurestore_name"
        assert fs.hive_endpoint == "test_hive_endpoint"
        assert fs.mysql_server_endpoint == "test_mysql_server_endpoint"
        assert fs.online_enabled is True
        assert fs._num_feature_groups == 2
        assert fs._num_training_datasets == 3
        assert fs._num_storage_connectors == 4
        assert fs._num_feature_views == 5

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["feature_store"]["get_basic_info"]["response"]

        # Act
        fs = feature_store.FeatureStore.from_response_json(json)

        # Assert
        assert fs.id == 11
        assert fs.name == "test_featurestore_name"
        assert fs._created == "test_created"
        assert fs.project_name == "test_project_name"
        assert fs.project_id == 67
        assert fs._online_feature_store_name is None
        assert fs._online_feature_store_size is None
        assert fs._offline_feature_store_name == "test_offline_featurestore_name"
        assert fs.hive_endpoint == "test_hive_endpoint"
        assert fs.mysql_server_endpoint is None
        assert fs.online_enabled is True
        assert fs._num_feature_groups is None
        assert fs._num_training_datasets is None
        assert fs._num_storage_connectors is None
        assert fs._num_feature_views is None

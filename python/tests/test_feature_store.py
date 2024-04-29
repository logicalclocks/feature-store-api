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


from hsfs import feature_group as feature_group_mod
from hsfs import feature_store as feature_store_mod


class TestFeatureStore:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]

        # Act
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Assert
        assert fs.id == 11
        assert fs.name == "test_featurestore_name"
        assert fs._created == "test_created"
        assert fs.project_name == "test_project_name"
        assert fs.project_id == 67
        assert fs._online_feature_store_name == "test_online_featurestore_name"
        assert fs._online_feature_store_size == 31
        assert fs._offline_feature_store_name == "test_offline_featurestore_name"
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
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Assert
        assert fs.id == 11
        assert fs.name == "test_featurestore_name"
        assert fs._created == "test_created"
        assert fs.project_name == "test_project_name"
        assert fs.project_id == 67
        assert fs._online_feature_store_name is None
        assert fs._online_feature_store_size is None
        assert fs._offline_feature_store_name == "test_offline_featurestore_name"
        assert fs.online_enabled is True
        assert fs._num_feature_groups is None
        assert fs._num_training_datasets is None
        assert fs._num_storage_connectors is None
        assert fs._num_feature_views is None

    def test_get_feature_group(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        fg = feature_group_mod.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi.get", return_value=fg)

        # Act
        fg_res = fs.get_feature_group("test_feature_group_name")

        # Assert
        assert fg_res.feature_store == fs
        assert fg_res._feature_store == fs

    def test_create_feature_group(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_feature_group(
            "test_feature_group_name",
            version=1,
            primary_key=["bob"],
            event_time="when",
            online_enabled=True,
        )

        # Assert
        assert fg_res.name == "test_feature_group_name"
        assert fg_res.version == 1
        assert fg_res.primary_key == ["bob"]
        assert fg_res.event_time == "when"
        assert fg_res.online_enabled is True
        assert fg_res.feature_store == fs
        assert fg_res._feature_store == fs

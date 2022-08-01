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

from hsfs import feature_group, feature, storage_connector
from hsfs.core import external_feature_group_engine


class TestExternalFeatureGroupEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_api_save = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.save"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        mock_engine_get_instance.return_value.parse_schema_feature_group.return_value = [
            f
        ]

        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert (
            mock_engine_get_instance.return_value.register_external_temporary_table.call_count
            == 1
        )
        assert (
            mock_engine_get_instance.return_value.parse_schema_feature_group.call_count
            == 1
        )
        assert mock_fg_api_save.call_count == 1
        assert len(mock_fg_api_save.call_args[0][0].features) == 1
        assert not mock_fg_api_save.call_args[0][0].features[0].primary

    def test_save_primary_key(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_api_save = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.save"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=["f"],
            partition_key=[],
        )

        mock_engine_get_instance.return_value.parse_schema_feature_group.return_value = [
            f
        ]

        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert (
            mock_engine_get_instance.return_value.register_external_temporary_table.call_count
            == 1
        )
        assert (
            mock_engine_get_instance.return_value.parse_schema_feature_group.call_count
            == 1
        )
        assert mock_fg_api_save.call_count == 1
        assert len(mock_fg_api_save.call_args[0][0].features) == 1
        assert mock_fg_api_save.call_args[0][0].features[0].primary

    def test_save_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_api_save = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.save"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[f],
            id=10,
        )

        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert (
            mock_engine_get_instance.return_value.register_external_temporary_table.call_count
            == 0
        )
        assert (
            mock_engine_get_instance.return_value.parse_schema_feature_group.call_count
            == 0
        )
        assert mock_fg_api_save.call_count == 1
        assert len(mock_fg_api_save.call_args[0][0].features) == 1
        assert not mock_fg_api_save.call_args[0][0].features[0].primary

    def test_update_features_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api_update_metadata = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.update_metadata"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )

        f = {"name": "f"}
        features = [f]

        external_fg = feature_group.ExternalFeatureGroup(
            storage_connector=jdbc_connector, id=10
        )

        # Act
        external_fg_engine._update_features_metadata(
            feature_group=external_fg, features=features
        )

        # Assert
        assert mock_fg_api_update_metadata.call_count == 1
        assert (
            mock_fg_api_update_metadata.call_args[0][1].storage_connector
            == external_fg.storage_connector
        )
        assert mock_fg_api_update_metadata.call_args[0][1].id == external_fg.id
        assert mock_fg_api_update_metadata.call_args[0][1].features == features

    def test_update_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_external_fg_engine_new_feature_list = mocker.patch(
            "hsfs.core.external_feature_group_engine.ExternalFeatureGroupEngine.new_feature_list"
        )
        mock_external_fg_engine_update_features_metadata = mocker.patch(
            "hsfs.core.external_feature_group_engine.ExternalFeatureGroupEngine._update_features_metadata"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        external_fg_engine.update_features(feature_group=None, updated_features=None)

        # Assert
        assert mock_external_fg_engine_new_feature_list.call_count == 1
        assert mock_external_fg_engine_update_features_metadata.call_count == 1

    def test_append_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_external_fg_engine_update_features_metadata = mocker.patch(
            "hsfs.core.external_feature_group_engine.ExternalFeatureGroupEngine._update_features_metadata"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="str")
        f2 = feature.Feature(name="f2", type="str")

        external_fg = feature_group.ExternalFeatureGroup(
            storage_connector=None, primary_key=[], features=[f, f1]
        )

        # Act
        external_fg_engine.append_features(
            feature_group=external_fg, new_features=[f1, f2]
        )

        # Assert
        assert mock_external_fg_engine_update_features_metadata.call_count == 1
        assert (
            len(mock_external_fg_engine_update_features_metadata.call_args[0][1]) == 4
        )  # todo why are there duplicates?

    def test_update_description(self, mocker):
        # Arrange
        feature_store_id = 99
        description = "temp_description"

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api_update_metadata = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.update_metadata"
        )

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        external_fg = feature_group.ExternalFeatureGroup(
            storage_connector=None, primary_key=[], id=10
        )

        # Act
        external_fg_engine.update_description(
            feature_group=external_fg, description=description
        )

        # Assert
        assert mock_fg_api_update_metadata.call_count == 1
        assert mock_fg_api_update_metadata.call_args[0][1].description == description

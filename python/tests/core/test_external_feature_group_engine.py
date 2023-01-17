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
import pytest

from hsfs import feature_group, feature, storage_connector
from hsfs.client import exceptions
from hsfs.core import external_feature_group_engine
from hsfs.engine import python


class TestExternalFeatureGroupEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.ExternalFeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            id=10,
            storage_connector=mocker.patch("hsfs.storage_connector.JdbcConnector"),
        )

        mock_engine_get_instance.return_value.parse_schema_feature_group.return_value = [
            f
        ]

        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert len(mock_fg_api.return_value.save.call_args[0][0].features) == 1
        assert not mock_fg_api.return_value.save.call_args[0][0].features[0].primary

    def test_save_primary_key(self, mocker):
        # Arrange
        feature_store_id = 99
        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.ExternalFeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=["f"],
            storage_connector=mocker.patch("hsfs.storage_connector.JdbcConnector"),
        )

        mock_engine_get_instance.return_value.parse_schema_feature_group.return_value = [
            f
        ]

        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert len(mock_fg_api.return_value.save.call_args[0][0].features) == 1
        assert mock_fg_api.return_value.save.call_args[0][0].features[0].primary

    def test_save_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.ExternalFeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            features=[f],
            id=10,
            storage_connector=mocker.patch("hsfs.storage_connector.JdbcConnector"),
        )

        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert len(mock_fg_api.return_value.save.call_args[0][0].features) == 1
        assert not mock_fg_api.return_value.save.call_args[0][0].features[0].primary

    def test_update_features_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

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
        assert mock_fg_api.return_value.update_metadata.call_count == 1
        assert (
            mock_fg_api.return_value.update_metadata.call_args[0][1].storage_connector
            == external_fg.storage_connector
        )
        assert (
            mock_fg_api.return_value.update_metadata.call_args[0][1].id
            == external_fg.id
        )
        assert (
            mock_fg_api.return_value.update_metadata.call_args[0][1].features
            == features
        )

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
        )

    def test_update_description(self, mocker):
        # Arrange
        feature_store_id = 99
        description = "temp_description"

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

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
        assert mock_fg_api.return_value.update_metadata.call_count == 1
        assert (
            mock_fg_api.return_value.update_metadata.call_args[0][1].description
            == description
        )

    def test_save_feature_group_metadata_primary_eventtime_error(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")

        f = feature.Feature(name="f", type="str")

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_pk_info:
            fg = feature_group.ExternalFeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["feature_name"],
                event_time="f",
                storage_connector=jdbc_connector,
                features=[f],
            )
            fg.save()

        with pytest.raises(exceptions.FeatureStoreException) as e_eventt_info:
            fg = feature_group.ExternalFeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                event_time="feature_name",
                storage_connector=jdbc_connector,
                features=[f],
            )
            fg.save()

        assert (
            str(e_pk_info.value)
            == "Provided primary key(s) feature_name doesn't exist in feature dataframe"
        )
        assert (
            str(e_eventt_info.value)
            == "Provided event_time feature feature_name doesn't exist in feature dataframe"
        )

    def test_save_python_engine_no_features(self, mocker):
        sc = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )
        mocker.patch("hsfs.engine.get_instance", return_value=python.Engine())
        mocker.patch("hsfs.engine.get_type")
        feature_store_id = 99
        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.ExternalFeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            storage_connector=sc,
            features=[],
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_save_info:
            external_fg_engine.save(feature_group=fg)

        # Assert
        assert e_save_info.type == exceptions.FeatureStoreException

    def test_save_python_engine_features(self, mocker):
        sc = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )

        mocker.patch("hsfs.engine.get_instance", return_value=python.Engine())
        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        feature_store_id = 99
        external_fg_engine = external_feature_group_engine.ExternalFeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        features = [
            feature.Feature(name="f1", type="int"),
            feature.Feature(name="f2", type="string"),
        ]

        fg = feature_group.ExternalFeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            storage_connector=sc,
            features=features,
            id=10,
        )
        # Act
        external_fg_engine.save(feature_group=fg)

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert len(mock_fg_api.return_value.save.call_args[0][0].features) == 2
        assert (
            mock_fg_api.return_value.save.call_args[0][0].storage_connector
            == fg.storage_connector
        )
        assert mock_fg_api.return_value.save.call_args[0][0].features == features
        assert mock_fg_api.return_value.save.call_args[0][0].id == fg.id

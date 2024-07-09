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
import warnings

import hsfs
import pytest
from hsfs import (
    engine,
    expectation_suite,
    feature,
    feature_group,
    feature_group_writer,
    feature_store,
    statistics_config,
    storage_connector,
    user,
    util,
)
from hsfs.client.exceptions import FeatureStoreException, RestAPIError
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS
from hsfs.engine import python
from hsfs.hopsworks_udf import UDFType


engine.init("python")
test_feature_group = feature_group.FeatureGroup(
    name="test",
    version=1,
    description="description",
    online_enabled=False,
    time_travel_format="HUDI",
    partition_key=[],
    primary_key=["pk"],
    hudi_precombine_key="pk",
    featurestore_id=1,
    featurestore_name="fs",
    features=[
        feature.Feature("pk", primary=True),
        feature.Feature("ts", primary=False),
        feature.Feature("f1", primary=False),
        feature.Feature("f2", primary=False),
    ],
    statistics_config={},
    event_time="ts",
    stream=False,
    expectation_suite=None,
)


class TestFeatureGroup:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is False
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_list"]["response"]

        # Act
        fg_list = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is False
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert len(fg.features) == 2
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format is None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is False
        assert fg.expectation_suite is None
        assert fg.deprecated is False

    def test_from_response_json_basic_info_deprecated(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_basic_info_deprecated"][
            "response"
        ]

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert len(fg.features) == 2
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format is None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is False
        assert fg.expectation_suite is None
        assert fg.deprecated is True
        assert len(warning_record) == 1
        assert str(warning_record[0].message) == (
            f"Feature Group `{fg.name}`, version `{fg.version}` is deprecated"
        )

    def test_from_response_json_stream(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_stream"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is True
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_stream_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_stream_list"]["response"]

        # Act
        fg_list = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is True
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_stream_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == []
        assert fg.hudi_precombine_key is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert len(fg.features) == 0
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format is None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is True
        assert fg.expectation_suite is None

    def test_constructor_with_list_event_time_for_compatibility(
        self, mocker, backend_fixtures, dataframe_fixture_basic
    ):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        json = backend_fixtures["feature_store"]["get"]["response"]

        features = [
            feature.Feature(name="pk", type="int"),
            feature.Feature(name="et", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        # Act
        fs = feature_store.FeatureStore.from_response_json(json)
        with warnings.catch_warnings(record=True) as warning_record:
            new_fg = fs.create_feature_group(
                name="fg_name",
                version=1,
                description="fg_description",
                event_time=["event_date"],
                features=features,
            )
        with pytest.raises(FeatureStoreException):
            util.verify_attribute_key_names(new_fg, False)

        # Assert
        assert new_fg.event_time == "event_date"
        assert len(warning_record) == 1
        assert issubclass(warning_record[0].category, DeprecationWarning)
        assert str(warning_record[0].message) == (
            "Providing event_time as a single-element list is deprecated"
            + " and will be dropped in future versions. Provide the feature_name string instead."
        )

    def test_select_all(self):
        query = test_feature_group.select_all()
        features = query.features
        assert len(features) == 4
        assert set([f.name for f in features]) == {"pk", "ts", "f1", "f2"}

    def test_select_all_exclude_pk(self):
        query = test_feature_group.select_all(include_primary_key=False)
        features = query.features
        assert len(features) == 3
        assert set([f.name for f in features]) == {"ts", "f1", "f2"}

    def test_select_all_exclude_ts(self):
        query = test_feature_group.select_all(include_event_time=False)
        features = query.features
        assert len(features) == 3
        assert set([f.name for f in features]) == {"pk", "f1", "f2"}

    def test_select_all_exclude_pk_ts(self):
        query = test_feature_group.select_all(
            include_primary_key=False, include_event_time=False
        )
        features = query.features
        assert len(features) == 2
        assert set([f.name for f in features]) == {"f1", "f2"}

    def test_materialization_job(self, mocker):
        mock_job = mocker.Mock()
        mock_job_api = mocker.patch(
            "hsfs.core.job_api.JobApi.get", return_value=mock_job
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # call first time should populate cache
        fg.materialization_job  # noqa: B018

        mock_job_api.assert_called_once_with("test_fg_2_offline_fg_materialization")
        assert fg._materialization_job == mock_job

        # call second time
        fg.materialization_job  # noqa: B018

        # make sure it still was called only once
        mock_job_api.assert_called_once  # noqa: B018
        assert fg.materialization_job == mock_job

    def test_materialization_job_retry_success(self, mocker):
        # Arrange
        mocker.patch("time.sleep")

        mock_response_job_not_found = mocker.Mock()
        mock_response_job_not_found.status_code = 404
        mock_response_job_not_found.json.return_value = {"errorCode": 130009}

        mock_response_not_found = mocker.Mock()
        mock_response_not_found.status_code = 404

        mock_job = mocker.Mock()

        mock_job_api = mocker.patch(
            "hsfs.core.job_api.JobApi.get",
            side_effect=[
                RestAPIError("", mock_response_job_not_found),
                RestAPIError("", mock_response_not_found),
                RestAPIError("", mock_response_not_found),
                mock_job,
            ],
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        job_result = fg.materialization_job

        # Assert
        assert job_result is mock_job
        assert mock_job_api.call_count == 4
        assert mock_job_api.call_args_list[0][0] == (
            "test_fg_2_offline_fg_materialization",
        )
        assert mock_job_api.call_args_list[1][0] == ("test_fg_2_offline_fg_backfill",)
        assert mock_job_api.call_args_list[2][0] == ("test_fg_2_offline_fg_backfill",)
        assert mock_job_api.call_args_list[3][0] == ("test_fg_2_offline_fg_backfill",)

    def test_materialization_job_retry_fail(self, mocker):
        # Arrange
        mocker.patch("time.sleep")

        mock_response_not_found = mocker.Mock()
        mock_response_not_found.status_code = 404

        mock_job_api = mocker.patch(
            "hsfs.core.job_api.JobApi.get",
            side_effect=RestAPIError("", mock_response_not_found),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            fg.materialization_job  # noqa: B018

        # Assert
        assert mock_job_api.call_count == 6
        assert str(e_info.value) == "No materialization job was found"

    def test_multi_part_insert_return_writer(self, mocker):
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        result = fg.multi_part_insert()
        assert isinstance(result, feature_group_writer.FeatureGroupWriter)
        assert result._feature_group == fg

    def test_multi_part_insert_call_insert(self, mocker, dataframe_fixture_basic):
        mock_writer = mocker.Mock()
        mocker.patch(
            "hsfs.feature_group_writer.FeatureGroupWriter", return_value=mock_writer
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fg.multi_part_insert(dataframe_fixture_basic)
        mock_writer.insert.assert_called_once()
        assert fg._multi_part_insert is True

    def test_save_feature_list(self, mocker):
        mock_save_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata",
            return_value=None,
        )

        features = [
            feature.Feature(name="pk", type="int"),
            feature.Feature(name="et", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
        )

        fg.save(features)
        mock_save_metadata.assert_called_once_with(fg, None, {})

    def test_save_feature_in_create(self, mocker):
        mock_save_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata",
            return_value=None,
        )

        features = [
            feature.Feature(name="pk", type="int"),
            feature.Feature(name="et", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            features=features,
            primary_key=[],
            partition_key=[],
        )

        fg.save()
        mock_save_metadata.assert_called_once_with(fg, None, {})

    def test_save_exception_empty_input(self):
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
        )

        with pytest.raises(FeatureStoreException) as e:
            fg.save()

        assert "Feature list not provided" in str(e.value)

    def test_save_with_non_feature_list(self, mocker):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mock_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save",
            return_value=(None, None),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
        )

        data = [[1, "test_1"], [2, "test_2"]]
        fg.save(data)

        mock_convert_to_default_dataframe.assert_called_once_with(data)

    def test_save_code_true(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.insert",
            return_value=(None, None),
        )
        mock_code_engine = mocker.patch("hsfs.core.code_engine.CodeEngine.save_code")

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fg.insert(dataframe_fixture_basic, save_code=True)

        mock_code_engine.assert_called_once_with(fg)

    def test_save_code_false(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.insert",
            return_value=(None, None),
        )
        mock_code_engine = mocker.patch("hsfs.core.code_engine.CodeEngine.save_code")

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fg.insert(dataframe_fixture_basic, save_code=False)

        mock_code_engine.assert_not_called()

    def test_save_report_true_default(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe",
            return_value=dataframe_fixture_basic,
        )
        mock_insert = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.insert",
            return_value=(None, None),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fg.insert(dataframe_fixture_basic)
        mock_insert.assert_called_once_with(
            fg,
            feature_dataframe=dataframe_fixture_basic,
            overwrite=False,
            operation="upsert",
            storage=None,
            write_options={"wait_for_job": False},
            validation_options={"save_report": True},
        )

    def test_save_report_default_overwritable(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe",
            return_value=dataframe_fixture_basic,
        )
        mock_insert = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.insert",
            return_value=(None, None),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fg.insert(dataframe_fixture_basic, validation_options={"save_report": False})

        mock_insert.assert_called_once_with(
            fg,
            feature_dataframe=dataframe_fixture_basic,
            overwrite=False,
            operation="upsert",
            storage=None,
            write_options={"wait_for_job": False},
            validation_options={"save_report": False},
        )


class TestExternalFeatureGroup:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
        assert fg.query == "Select * from "
        assert fg.data_format == "HUDI"
        assert fg.path == "test_path"
        assert fg.options == {"test_name": "test_value"}
        assert fg.name == "external_fg_test"
        assert fg.version == 1
        assert fg.description == "test description"
        assert fg.primary_key == ["intt"]
        assert fg._feature_store_id == 67
        assert fg._feature_store_name == "test_project_featurestore"
        assert fg.created == "2022-08-16T07:19:12Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 14
        assert len(fg.features) == 3
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/test_project_featurestore.db/external_fg_test_1"
        )
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == "datet"
        assert isinstance(fg.expectation_suite, expectation_suite.ExpectationSuite)

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get_list"]["response"]

        # Act
        fg_list = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
        assert fg.query == "Select * from "
        assert fg.data_format == "HUDI"
        assert fg.path == "test_path"
        assert fg.options == {"test_name": "test_value"}
        assert fg.name == "external_fg_test"
        assert fg.version == 1
        assert fg.description == "test description"
        assert fg.primary_key == ["intt"]
        assert fg._feature_store_id == 67
        assert fg._feature_store_name == "test_project_featurestore"
        assert fg.created == "2022-08-16T07:19:12Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 14
        assert len(fg.features) == 3
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/test_project_featurestore.db/external_fg_test_1"
        )
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == "datet"
        assert isinstance(fg.expectation_suite, expectation_suite.ExpectationSuite)

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get_basic_info"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
        assert fg.query is None
        assert fg.data_format is None
        assert fg.path is None
        assert fg.options is None
        assert fg.name is None
        assert fg.version is None
        assert fg.description is None
        assert fg.primary_key == []
        assert fg._feature_store_id is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert fg.features is None
        assert fg.location is None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time is None
        assert fg.expectation_suite is None

    def test_feature_group_set_expectation_suite(
        self,
        mocker,
        backend_fixtures,
    ):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]
        es = expectation_suite.ExpectationSuite.from_response_json(json)
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)

        fg.expectation_suite = es

        assert fg.expectation_suite.id == es.id
        assert fg.expectation_suite._feature_group_id == fg.id
        assert fg.expectation_suite._feature_store_id == fg.feature_store_id

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_feature_group_save_expectation_suite_from_ge_type(
        self, mocker, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]
        es = expectation_suite.ExpectationSuite.from_response_json(json)
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)

        # to mock delete we need get_expectation_suite to not return none
        mock_get_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.get",
            return_value=es,
        )
        mock_create_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.create",
            return_value=es,
        )
        mock_delete_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.delete"
        )

        version_api = mocker.patch("hsfs.core.variable_api.VariableApi.get_version")
        version_api.return_value = hsfs.__version__

        mock_es_engine_get_expectation_suite_url = mocker.patch(
            "hsfs.core.expectation_suite_engine.ExpectationSuiteEngine._get_expectation_suite_url"
        )
        mock_print = mocker.patch("builtins.print")

        # Act
        fg.save_expectation_suite(es.to_ge_type(), overwrite=True)
        # Assert
        assert mock_delete_expectation_suite_api.call_count == 1
        assert mock_create_expectation_suite_api.call_count == 1
        assert mock_get_expectation_suite_api.call_count == 1
        assert mock_es_engine_get_expectation_suite_url.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0][:55]
            == "Attached expectation suite to Feature Group, edit it at"
        )

    def test_feature_group_save_expectation_suite_from_hopsworks_type(
        self, mocker, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]
        es = expectation_suite.ExpectationSuite.from_response_json(json)
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)

        mock_update_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.update"
        )

        version_api = mocker.patch("hsfs.core.variable_api.VariableApi.get_version")
        version_api.return_value = hsfs.__version__

        mock_es_engine_get_expectation_suite_url = mocker.patch(
            "hsfs.core.expectation_suite_engine.ExpectationSuiteEngine._get_expectation_suite_url"
        )
        mock_print = mocker.patch("builtins.print")

        # Act
        fg.save_expectation_suite(es)
        # Assert

        assert mock_update_expectation_suite_api.call_count == 1
        assert mock_es_engine_get_expectation_suite_url.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0][:63]
            == "Updated expectation suite attached to Feature Group, edit it at"
        )

    def test_from_response_json_transformation_functions(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_transformations"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert len(fg.transformation_functions) == 2
        assert (
            fg.transformation_functions[0].hopsworks_udf.function_name == "add_one_fs"
        )
        assert fg.transformation_functions[1].hopsworks_udf.function_name == "add_two"
        assert (
            fg.transformation_functions[0].hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )
        assert (
            fg.transformation_functions[1].hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_two(data1 : pd.Series):\n    return data1 + 2\n"
        )
        assert (
            fg.transformation_functions[0].hopsworks_udf.udf_type == UDFType.ON_DEMAND
        )
        assert (
            fg.transformation_functions[1].hopsworks_udf.udf_type == UDFType.ON_DEMAND
        )
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is False
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

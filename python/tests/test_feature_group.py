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


from hsfs import (
    feature_store,
    feature_group,
    user,
    statistics_config,
    feature,
    storage_connector,
    expectation_suite,
    util,
    engine,
)
from hsfs.client.exceptions import FeatureStoreException
import pytest
import warnings

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
        assert fg.stream is False
        assert fg.expectation_suite is None

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

        # Act
        fs = feature_store.FeatureStore.from_response_json(json)
        with warnings.catch_warnings(record=True) as warning_record:
            new_fg = fs.create_feature_group(
                name="fg_name",
                version=1,
                description="fg_description",
                event_time=["event_date"],
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

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


from hsfs import feature_group, user, statistics_config, feature, storage_connector, expectation_suite

class TestFeatureGroup:

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ['intt']
        assert fg.hudi_precombine_key == 'intt'
        assert fg._feature_store_name == 'test_featurestore'
        assert fg.created == '2022-08-01T11:07:55Z'
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert fg.location == 'hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1'
        assert fg.online_enabled == True
        assert fg.time_travel_format == 'HUDI'
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == '119_15_fg_test_1_onlinefs'
        assert fg.event_time == None
        assert fg.stream == False
        assert fg.expectation_suite == None

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group_list"]["response"]

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
        assert fg.primary_key == ['intt']
        assert fg.hudi_precombine_key == 'intt'
        assert fg._feature_store_name == 'test_featurestore'
        assert fg.created == '2022-08-01T11:07:55Z'
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert fg.location == 'hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1'
        assert fg.online_enabled == True
        assert fg.time_travel_format == 'HUDI'
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == '119_15_fg_test_1_onlinefs'
        assert fg.event_time == None
        assert fg.stream == False
        assert fg.expectation_suite == None

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group_basic_info"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == []
        assert fg.hudi_precombine_key == None
        assert fg._feature_store_name == None
        assert fg.created == None
        assert fg.creator == None
        assert fg.id == 15
        assert len(fg.features) == 0
        assert fg.location == None
        assert fg.online_enabled == False
        assert fg.time_travel_format == None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == None
        assert fg.event_time == None
        assert fg.stream == False
        assert fg.expectation_suite == None

    def test_from_response_json_stream(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group_stream"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ['intt']
        assert fg.hudi_precombine_key == 'intt'
        assert fg._feature_store_name == 'test_featurestore'
        assert fg.created == '2022-08-01T11:07:55Z'
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert fg.location == 'hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1'
        assert fg.online_enabled == True
        assert fg.time_travel_format == 'HUDI'
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == '119_15_fg_test_1_onlinefs'
        assert fg.event_time == None
        assert fg.stream == True
        assert fg.expectation_suite == None

    def test_from_response_json_stream_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group_stream_list"]["response"]

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
        assert fg.primary_key == ['intt']
        assert fg.hudi_precombine_key == 'intt'
        assert fg._feature_store_name == 'test_featurestore'
        assert fg.created == '2022-08-01T11:07:55Z'
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
        assert fg.location == 'hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1'
        assert fg.online_enabled == True
        assert fg.time_travel_format == 'HUDI'
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == '119_15_fg_test_1_onlinefs'
        assert fg.event_time == None
        assert fg.stream == True
        assert fg.expectation_suite == None

    def test_from_response_json_stream_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group_stream_basic_info"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == []
        assert fg.hudi_precombine_key == None
        assert fg._feature_store_name == None
        assert fg.created == None
        assert fg.creator == None
        assert fg.id == 15
        assert len(fg.features) == 0
        assert fg.location == None
        assert fg.online_enabled == False
        assert fg.time_travel_format == None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == None
        assert fg.event_time == None
        assert fg.stream == True
        assert fg.expectation_suite == None


class TestExternalFeatureGroup:

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_external_feature_group"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
        assert fg.query == "Select * from "
        assert fg.data_format == "HUDI"
        assert fg.path == "test_path"
        assert fg.options == {'test_name': 'test_value'}
        assert fg.name == "external_fg_test"
        assert fg.version == 1
        assert fg.description == "test description"
        assert fg.primary_key == ['intt']
        assert fg._feature_store_id == 67
        assert fg._feature_store_name == 'test_project_featurestore'
        assert fg.created == "2022-08-16T07:19:12Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 14
        assert len(fg.features) == 3
        assert isinstance(fg.features[0], feature.Feature)
        assert fg.location == "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/test_project_featurestore.db/external_fg_test_1"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == "datet"
        assert isinstance(fg.expectation_suite, expectation_suite.ExpectationSuite)

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_external_feature_group_list"]["response"]

        # Act
        fg_list = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
        assert fg.query == "Select * from "
        assert fg.data_format == "HUDI"
        assert fg.path == "test_path"
        assert fg.options == {'test_name': 'test_value'}
        assert fg.name == "external_fg_test"
        assert fg.version == 1
        assert fg.description == "test description"
        assert fg.primary_key == ['intt']
        assert fg._feature_store_id == 67
        assert fg._feature_store_name == 'test_project_featurestore'
        assert fg.created == "2022-08-16T07:19:12Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 14
        assert len(fg.features) == 3
        assert isinstance(fg.features[0], feature.Feature)
        assert fg.location == "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/test_project_featurestore.db/external_fg_test_1"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == "datet"
        assert isinstance(fg.expectation_suite, expectation_suite.ExpectationSuite)

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_external_feature_group_basic_info"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
        assert fg.query == None
        assert fg.data_format == None
        assert fg.path == None
        assert fg.options == None
        assert fg.name == None
        assert fg.version == None
        assert fg.description == None
        assert fg.primary_key == []
        assert fg._feature_store_id == None
        assert fg._feature_store_name == None
        assert fg.created == None
        assert fg.creator == None
        assert fg.id == 15
        assert fg.features == None
        assert fg.location == None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == None
        assert fg.expectation_suite == None

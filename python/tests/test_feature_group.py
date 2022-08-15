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


from hsfs import feature_group, user, statistics_config, feature

class TestFeatureGroup:

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_feature_group"]["response"]

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
        fg_list = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
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



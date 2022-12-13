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


from hsfs import feature_group, feature
from hsfs.constructor import query, join, filter


class TestQuery:
    def test_from_response_json_python(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["query"]["get"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == "test_feature_store_id"
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is True

    def test_from_response_json_external_fg_python(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["query"]["get_external_fg"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == "test_feature_store_id"
        assert isinstance(q._left_feature_group, feature_group.ExternalFeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is True

    def test_from_response_json_spark(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        json = backend_fixtures["query"]["get"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == "test_feature_store_id"
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is False

    def test_from_response_json_external_fg_spark(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        json = backend_fixtures["query"]["get_external_fg"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == "test_feature_store_id"
        assert isinstance(q._left_feature_group, feature_group.ExternalFeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is False

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["query"]["get_basic_info"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name is None
        assert q._feature_store_id is None
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time is None
        assert q._left_feature_group_end_time is None
        assert len(q._joins) == 0
        assert q._filter is None
        assert q._python_engine is True

    def test_as_of(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")
        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])
        q.as_of("2022-01-01 00:00:00")

        assert q.left_feature_group_end_time == 1640995200000
        assert q._joins[0].query.left_feature_group_end_time == 1640995200000

        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])
        q.as_of(None, "2022-01-01 00:00:00")

        assert q.left_feature_group_start_time == 1640995200000
        assert q._joins[0].query.left_feature_group_start_time == 1640995200000

        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])
        q.as_of("2022-01-02 00:00:00", exclude_until="2022-01-01 00:00:00")

        assert q.left_feature_group_end_time == 1641081600000
        assert q.left_feature_group_start_time == 1640995200000
        assert q._joins[0].query.left_feature_group_end_time == 1641081600000
        assert q._joins[0].query.left_feature_group_start_time == 1640995200000

        q.as_of()

        assert q.left_feature_group_end_time is None
        assert q.left_feature_group_start_time is None
        assert q._joins[0].query.left_feature_group_end_time is None
        assert q._joins[0].query.left_feature_group_start_time is None

    def test_collect_feature(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")
        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])

        features = q._collect_features()
        feature_names = [feature.name for feature in features]

        expected_feature_names = ["test_left_features", "test_left_features2"]
        assert len(feature_names) == len(expected_feature_names)
        assert feature_names == expected_feature_names

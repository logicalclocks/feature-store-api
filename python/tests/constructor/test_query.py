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
from hsfs import feature_group, feature
from hsfs.constructor import query, join, filter
from hsfs.client.exceptions import FeatureStoreException


class TestQuery:
    fg1 = feature_group.FeatureGroup(
        name="test1",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=11),
            feature.Feature("label", feature_group_id=11),
            feature.Feature("tf_name", feature_group_id=11),
        ],
        id=11,
        stream=False,
    )

    fg2 = feature_group.FeatureGroup(
        name="test2",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=12),
            feature.Feature("tf1_name", feature_group_id=12),
        ],
        id=12,
        stream=False,
    )

    fg3 = feature_group.FeatureGroup(
        name="test3",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=13),
            feature.Feature("tf_name", feature_group_id=13),
            feature.Feature("tf1_name", feature_group_id=13),
            feature.Feature("tf3_name", feature_group_id=13),
        ],
        id=13,
        stream=False,
    )

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

        # Act
        q = TestQuery.fg1.select(["label"]).join(TestQuery.fg2.select(["tf1_name"]))

        features = q.features
        feature_names = [feature.name for feature in features]

        expected_features = [TestQuery.fg1["label"], TestQuery.fg2["tf1_name"]]
        expected_feature_names = ["label", "tf1_name"]

        # Assert
        assert len(feature_names) == len(expected_features)
        for i, feat in enumerate(expected_features):
            assert feat.name == expected_feature_names[i]

    def test_collect_featuregroups(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select(["label"])
            .join(TestQuery.fg2.select(["tf1_name"]))
            .join(TestQuery.fg2.select(["tf1_name"]))
        )
        expected_featuregroups = [TestQuery.fg1, TestQuery.fg2]

        # Assert
        assert len(q.featuregroups) == len(expected_featuregroups)
        assert set(q.featuregroups) == set(expected_featuregroups)

    def test_append_feature(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select([TestQuery.fg1["label"]]).append_feature("id")
        expected_features = [TestQuery.fg1["label"], feature.Feature("id")]

        # Assert
        assert len(q.features) == len(expected_features)
        for i, feat in enumerate(expected_features):
            assert feat.name == expected_features[i].name

    def test_get_feature(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(TestQuery.fg1["label"]).join(
            TestQuery.fg2.select(TestQuery.fg2["tf1_name"])
        )

        # Assert
        assert id(q.get_feature("tf1_name")) == id(
            TestQuery.fg2.get_feature("tf1_name")
        )

    def test_get_index(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(TestQuery.fg1["label"]).join(
            TestQuery.fg2.select(TestQuery.fg2["tf1_name"])
        )

        # Assert
        assert id(q.get_feature("tf1_name")) == id(
            TestQuery.fg2.get_feature("tf1_name")
        )

    def test_get_attr(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(TestQuery.fg1["label"]).join(
            TestQuery.fg2.select(TestQuery.fg2["tf1_name"])
        )

        # Assert
        assert id(q.get_feature("tf1_name")) == id(
            TestQuery.fg2.get_feature("tf1_name")
        )

    def test_get_feature_by_name(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all())
        )

        # Assert
        assert (
            q._get_feature_by_name("tf3_name")[0].name == TestQuery.fg3["tf3_name"].name
        )
        assert (
            q._get_feature_by_name("tf3_name")[0].feature_group_id
            == TestQuery.fg3["tf3_name"].feature_group_id
        )

    def test_get_feature_by_name_prefix(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        assert (
            q._get_feature_by_name("tf_name")[0].name == TestQuery.fg1["tf_name"].name
        )
        assert (
            q._get_feature_by_name("tf_name")[0].feature_group_id
            == TestQuery.fg1["tf_name"].feature_group_id
        )

    def test_get_feature_by_name_ambiguous(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        with pytest.raises(FeatureStoreException) as e_info:
            q._get_feature_by_name("id")[0]

        assert str(e_info.value) == query.Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS.format(
            "id"
        )

    def test_get_feature_by_feature_ambiguous(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        with pytest.raises(FeatureStoreException) as e_info:
            q._get_featuregroup_by_feature(feature.Feature("id"))[0]

        assert str(
            e_info.value
        ) == query.Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS_FG.format("id")

    def test_get_feature_by_feature_non_ambiguous(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        assert q._get_featuregroup_by_feature(TestQuery.fg3["id"]) == TestQuery.fg3

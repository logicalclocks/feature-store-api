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

from hsfs import feature_view, training_dataset_feature
from hsfs.constructor import fs_query, query
from hsfs.feature_store import FeatureStore
from hsfs.hopsworks_udf import hopsworks_udf


class TestFeatureView:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch.object(
            FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi.get")
        json = backend_fixtures["feature_view"]["get"]["response"]
        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id == 11
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version == 1
        assert fv.description == "test_description"
        assert fv.labels == ["intt"]
        assert fv.transformation_functions == []
        assert len(fv.schema) == 2
        assert isinstance(fv.schema[0], training_dataset_feature.TrainingDatasetFeature)

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["feature_view"]["get_basic_info"]["response"]

        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id is None
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version is None
        assert fv.description is None
        assert fv.labels == []
        assert fv.transformation_functions == []
        assert len(fv.schema) == 0
        assert fv.query._left_feature_group.deprecated is False

    def test_from_response_json_transformation_function(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch.object(
            FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi.get")
        json = backend_fixtures["feature_view"]["get_transformations"]["response"]
        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id == 11
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version == 1
        assert fv.description == "test_description"
        assert fv.labels == ["intt"]
        assert len(fv.transformation_functions) == 2
        assert (
            fv.transformation_functions[0].hopsworks_udf.function_name == "add_mean_fs"
        )
        assert (
            fv.transformation_functions[1].hopsworks_udf.function_name == "add_one_fs"
        )
        assert (
            fv.transformation_functions[0].hopsworks_udf._function_source
            == "\n@hopsworks_udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics_data1):\n    return data1 + statistics_data1.mean\n"
        )
        assert (
            fv.transformation_functions[1].hopsworks_udf._function_source
            == "\n@hopsworks_udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )
        assert len(fv.schema) == 2
        assert isinstance(fv.schema[0], training_dataset_feature.TrainingDatasetFeature)

    def test_from_response_json_basic_info_deprecated(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["feature_view"]["get_basic_info_deprecated"]["response"]

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id is None
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version is None
        assert fv.description is None
        assert fv.labels == []
        assert fv.transformation_functions == []
        assert len(fv.schema) == 0
        assert fv.query._left_feature_group.deprecated is True
        assert len(warning_record) == 1
        assert str(warning_record[0].message) == (
            f"Feature Group `{fv.query._left_feature_group.name}`, version `{fv.query._left_feature_group.version}` is deprecated"
        )

    def test_transformation_function_instances(self, mocker, backend_fixtures):
        # Arrange
        feature_store_id = 99
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        json = backend_fixtures["fs_query"]["get"]["response"]

        # Act
        q = fs_query.FsQuery.from_response_json(json)

        @hopsworks_udf(int)
        def test(col1):
            return col1 + 1

        fv = feature_view.FeatureView(
            featurestore_id=feature_store_id,
            name="test_fv",
            version=1,
            query=q,
            transformation_functions=[test("data1"), test("data2")],
        )

        transformation_functions = fv.transformation_functions

        assert transformation_functions[0] != transformation_functions[1]

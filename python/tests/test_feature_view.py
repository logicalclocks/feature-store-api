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


from hsfs import feature_view, training_dataset_feature, transformation_function
from hsfs.constructor import query, fs_query


class TestFeatureView:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
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
        assert fv.transformation_functions == {}
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
        assert fv.transformation_functions == {}
        assert len(fv.schema) == 0

    def test_transformation_function_instances(self, mocker, backend_fixtures):
        # Arrange
        feature_store_id = 99
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        json = backend_fixtures["fs_query"]["get"]["response"]

        # Act
        q = fs_query.FsQuery.from_response_json(json)

        def testFunction():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            transformation_fn=testFunction,
            builtin_source_code="",
            output_type="str",
        )

        transformation_fn_dict = dict()
        transformation_fn_dict["tf_name"] = tf
        transformation_fn_dict["tf1_name"] = tf

        fv = feature_view.FeatureView(
            featurestore_id=feature_store_id,
            name="test_fv",
            version=1,
            query=q,
            transformation_functions=transformation_fn_dict,
        )

        updated_transformation_fn_dict = fv.transformation_functions

        assert (
            updated_transformation_fn_dict["tf_name"]
            != updated_transformation_fn_dict["tf1_name"]
        )
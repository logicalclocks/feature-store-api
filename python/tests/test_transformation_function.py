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


from hsfs import transformation_function


class TestTransformationFunction:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get"]["response"]

        # Act
        tf = transformation_function.TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 43
        assert tf._featurestore_id == 11
        assert tf.version == 1
        assert tf.name == "test_name"
        assert tf.transformation_fn is None
        assert tf.output_type == "FLOAT"
        assert (
            tf.source_code_content
            == '{"module_imports": "", "transformer_code": "test_builtin_source_code"}'
        )
        assert tf._feature_group_feature_name is None
        assert tf._feature_group_id is None

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch(
            "hsfs.transformation_function.TransformationFunction._load_source_code"
        )
        json = backend_fixtures["transformation_function"]["get_basic_info"]["response"]

        # Act
        tf = transformation_function.TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id is None
        assert tf._featurestore_id == 11
        assert tf.version is None
        assert tf.name is None
        assert tf.transformation_fn is None
        assert tf.output_type == "STRING"
        assert tf.source_code_content is None
        assert tf._feature_group_feature_name is None
        assert tf._feature_group_id is None

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list"]["response"]

        # Act
        tf_list = transformation_function.TransformationFunction.from_response_json(
            json
        )

        # Assert
        assert len(tf_list) == 1
        tf = tf_list[0]
        assert tf.id == 43
        assert tf._featurestore_id == 11
        assert tf.version == 1
        assert tf.name == "test_name"
        assert tf.transformation_fn is None
        assert tf.output_type == "FLOAT"
        assert (
            tf.source_code_content
            == '{"module_imports": "", "transformer_code": "test_builtin_source_code"}'
        )
        assert tf._feature_group_feature_name is None
        assert tf._feature_group_id is None

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list_empty"]["response"]

        # Act
        tf_list = transformation_function.TransformationFunction.from_response_json(
            json
        )

        # Assert
        assert len(tf_list) == 0

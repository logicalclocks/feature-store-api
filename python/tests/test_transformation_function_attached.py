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


from hsfs import transformation_function, transformation_function_attached


class TestTransformationFunctionAttached:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function_attached"]["get"]["response"]

        # Act
        tf_attached = transformation_function_attached.TransformationFunctionAttached.from_response_json(
            json
        )

        # Assert
        assert tf_attached.name == "test_name"
        assert isinstance(
            tf_attached.transformation_function,
            transformation_function.TransformationFunction,
        )

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function_attached"]["get_basic_info"][
            "response"
        ]

        # Act
        tf_attached = transformation_function_attached.TransformationFunctionAttached.from_response_json(
            json
        )

        # Assert
        assert tf_attached.name == "test_name"
        assert isinstance(
            tf_attached.transformation_function,
            transformation_function.TransformationFunction,
        )

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function_attached"]["get_list"][
            "response"
        ]

        # Act
        tf_attached_list = transformation_function_attached.TransformationFunctionAttached.from_response_json(
            json
        )

        # Assert
        assert len(tf_attached_list) == 1
        tf_attached = tf_attached_list[0]
        assert tf_attached.name == "test_name"
        assert isinstance(
            tf_attached.transformation_function,
            transformation_function.TransformationFunction,
        )

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function_attached"]["get_list_empty"][
            "response"
        ]

        # Act
        tf_attached_list = transformation_function_attached.TransformationFunctionAttached.from_response_json(
            json
        )

        # Assert
        assert len(tf_attached_list) == 0

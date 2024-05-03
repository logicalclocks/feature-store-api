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


from hsfs.transformation_function import TransformationFunction


class TestTransformationFunction:
    def test_from_response_json_one_argument_no_statistics(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_one_argument_no_statistics_function"
        ]["response"]

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_one_fs"
        assert tf.hopsworks_udf.output_types == ["double"]
        assert not tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["col1"]
        assert tf.hopsworks_udf.statistics_features == []
        assert (
            tf.hopsworks_udf._function_source
            == "\n@hopsworks_udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )

    def test_from_response_json_one_argument_with_statistics(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_one_argument_with_statistics_function"
        ]["response"]

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_mean_fs"
        assert tf.hopsworks_udf.output_types == ["double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["data"]
        assert tf.hopsworks_udf.statistics_features == ["data"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@hopsworks_udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics_data1):\n    return data1 + statistics_data1.mean\n"
        )

    def test_from_response_json_multiple_argument_with_statistics(
        self, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_multiple_argument_with_statistics_function"
        ]["response"]

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "test_func"
        assert tf.hopsworks_udf.output_types == ["string"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == [
            "feature1",
            "feature2",
            "feature3",
        ]
        assert tf.hopsworks_udf.statistics_features == ["feature1", "feature2"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@hopsworks_udf(str)\ndef test_func(data1 : pd.Series, statistics_data1, data2, statistics_data2, data3):\n    return data1 + statistics_data1.mean\n"
        )

    def test_from_response_json_multiple_return_type_functions(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_multiple_return_type_functions"
        ]["response"]

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "test_func"
        assert tf.hopsworks_udf.output_types == ["string", "double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == [
            "feature1",
            "feature2",
            "feature3",
        ]
        assert tf.hopsworks_udf.statistics_features == ["feature1", "feature2"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@hopsworks_udf(str, float)\ndef test_func(data1 : pd.Series, statistics_data1, data2, statistics_data2, data3):\n    return pd.DataFrame('col1': ['a', 'b'], 'col2':[1,2])\n"
        )

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list_empty"]["response"]

        # Act
        tf_list = TransformationFunction.from_response_json(json)

        # Assert
        assert len(tf_list) == 0

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list"]["response"]

        # Act
        tf_list = TransformationFunction.from_response_json(json)

        # Assert
        assert len(tf_list) == 2
        tf = tf_list[0]
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_mean_fs"
        assert tf.hopsworks_udf.output_types == ["double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["data"]
        assert tf.hopsworks_udf.statistics_features == ["data"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@hopsworks_udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics_data1):\n    return data1 + statistics_data1.mean\n"
        )

        tf = tf_list[1]
        assert tf.id == 2
        assert tf._featurestore_id == 11
        assert tf.version == 1
        assert tf.hopsworks_udf.function_name == "add_one_fs"
        assert tf.hopsworks_udf.output_types == ["double"]
        assert not tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["col1"]
        assert tf.hopsworks_udf.statistics_features == []
        assert (
            tf.hopsworks_udf._function_source
            == "\n@hopsworks_udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )


"""
    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_basic_info"]["response"]

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id is None
        assert tf._featurestore_id == 11
        assert tf.version is None
        assert tf.hopsworks_udf is None
"""

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
import numpy
import datetime

from hsfs import (
    training_dataset,
    transformation_function,
    transformation_function_attached,
    feature_view,
)
from hsfs.core import transformation_function_engine


class TestTransformationFunctionEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin"
        )
        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id, builtin_source_code="", output_type="str", name="tf_name"
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            tf_engine.save(transformation_fn_instance=tf)

        # Assert
        assert mock_tf_api.return_value.register_transformation_fn.call_count == 0
        assert (
            str(e_info.value)
            == "Transformation function name 'tf_name' with version 1 is reserved for built-in "
            "hsfs functions. Please use other name or version"
        )

    def test_save_is_builtin(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_engine_is_builtin = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin"
        )
        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id, builtin_source_code="", output_type="str", name="tf_name"
        )

        mock_tf_engine_is_builtin.return_value = False

        # Act
        with pytest.raises(ValueError) as e_info:
            tf_engine.save(transformation_fn_instance=tf)

        # Assert
        assert mock_tf_api.return_value.register_transformation_fn.call_count == 0
        assert str(e_info.value) == "transformer must be callable"

    def test_save_is_builtin_callable(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.transformation_function.TransformationFunction._extract_source_code"
        )
        mock_tf_engine_is_builtin = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin"
        )
        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def testFunction():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            transformation_fn=testFunction,
            builtin_source_code="",
            output_type="str",
        )

        mock_tf_engine_is_builtin.return_value = False

        # Act
        tf_engine.save(transformation_fn_instance=tf)

        # Assert
        assert mock_tf_api.return_value.register_transformation_fn.call_count == 1

    def test_get_transformation_fn(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id, builtin_source_code="", output_type="str", name="tf_name"
        )
        tf1 = transformation_function.TransformationFunction(
            feature_store_id, builtin_source_code="", output_type="str", name="tf1_name"
        )
        transformations = [tf, tf1]

        mock_tf_api.return_value.get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine.get_transformation_fn(name=None, version=None)

        # Assert
        assert mock_tf_api.return_value.get_transformation_fn.call_count == 1
        assert result == tf

    def test_get_transformation_fns(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id, builtin_source_code="", output_type="str", name="tf_name"
        )
        tf1 = transformation_function.TransformationFunction(
            feature_store_id, builtin_source_code="", output_type="str", name="tf1_name"
        )
        transformations = [tf, tf1]

        mock_tf_api.return_value.get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine.get_transformation_fns()

        # Assert
        assert mock_tf_api.return_value.get_transformation_fn.call_count == 1
        assert result == transformations

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        # Act
        tf_engine.delete(transformation_function_instance=None)

        # Assert
        assert mock_tf_api.return_value.delete.call_count == 1

    def test_get_td_transformation_fn(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def plus_one(a):
            return a + 1

        tf_attached = transformation_function_attached.TransformationFunctionAttached(
            name="tf_name", transformation_function=plus_one
        )
        tf1_attached = transformation_function_attached.TransformationFunctionAttached(
            name="tf1_name", transformation_function=plus_one
        )

        transformations_attached = [tf_attached, tf1_attached]

        mock_tf_api.return_value.get_td_transformation_fn.return_value = (
            transformations_attached
        )

        # Act
        result = tf_engine.get_td_transformation_fn(training_dataset=None)

        # Assert
        assert "tf_name" in result
        assert "tf1_name" in result
        assert mock_tf_api.return_value.get_td_transformation_fn.call_count == 1

    def test_attach_transformation_fn_td(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

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

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
            transformation_functions=transformation_fn_dict,
        )

        # Act
        with pytest.raises(AttributeError) as e_info:
            tf_engine.attach_transformation_fn(
                training_dataset_obj=td, feature_view_obj=None
            )

        # Assert
        assert str(e_info.value) == "'TrainingDataset' object has no attribute 'labels'"

    def test_attach_transformation_fn_fv(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

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
            name="test",
            query="",
            featurestore_id=99,
            transformation_functions=transformation_fn_dict,
            labels=[],
        )

        # Act
        tf_engine.attach_transformation_fn(
            training_dataset_obj=None, feature_view_obj=fv
        )

        # Assert
        assert len(fv._features) == 2
        assert fv._features[0].name == "tf_name"
        assert fv._features[1].name == "tf1_name"

    def test_attach_transformation_fn_fv_labels(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

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
            name="test",
            query="",
            featurestore_id=99,
            transformation_functions=transformation_fn_dict,
            labels=["tf_name"],
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            tf_engine.attach_transformation_fn(
                training_dataset_obj=None, feature_view_obj=fv
            )

        # Assert
        assert (
            str(e_info.value)
            == "Online transformations for training dataset labels are not supported."
        )

    def test_is_builtin(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            builtin_source_code="",
            output_type="str",
            name="tf_name",
            version=1,
        )

        # Act
        result = tf_engine.is_builtin(transformation_fn_instance=tf)

        # Assert
        assert result is False

    def test_is_builtin_min_max_scaler(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            builtin_source_code="",
            output_type="str",
            name="min_max_scaler",
            version=1,
        )

        # Act
        result = tf_engine.is_builtin(transformation_fn_instance=tf)

        # Assert
        assert result is True

    def test_is_builtin_min_max_scaler_version(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            builtin_source_code="",
            output_type="str",
            name="min_max_scaler",
            version=2,
        )

        # Act
        result = tf_engine.is_builtin(transformation_fn_instance=tf)

        # Assert
        assert result is False

    def test_is_builtin_standard_scaler(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            builtin_source_code="",
            output_type="str",
            name="standard_scaler",
            version=1,
        )

        # Act
        result = tf_engine.is_builtin(transformation_fn_instance=tf)

        # Assert
        assert result is True

    def test_is_builtin_robust_scaler(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            builtin_source_code="",
            output_type="str",
            name="robust_scaler",
            version=1,
        )

        # Act
        result = tf_engine.is_builtin(transformation_fn_instance=tf)

        # Assert
        assert result is True

    def test_is_builtin_label_encoder(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            builtin_source_code="",
            output_type="str",
            name="label_encoder",
            version=1,
        )

        # Act
        result = tf_engine.is_builtin(transformation_fn_instance=tf)

        # Assert
        assert result is True

    def test_populate_builtin_fn_arguments(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def tf_name():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id, transformation_fn=tf_name, output_type="str"
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            tf_engine.populate_builtin_fn_arguments(
                feature_name=None,
                transformation_function_instance=tf,
                stat_content=None,
            )

        # Assert
        assert str(e_info.value) == "Not implemented"

    def test_populate_builtin_fn_arguments_min_max_scaler(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.builtin_transformation_function.BuiltInTransformationFunction.min_max_scaler_stats",
            return_value=(1, 100),
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def min_max_scaler():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id, transformation_fn=min_max_scaler, output_type="str"
        )

        # Act
        tf_engine.populate_builtin_fn_arguments(
            feature_name=None, transformation_function_instance=tf, stat_content=None
        )

        # Assert
        assert tf.transformation_fn.keywords["min_value"] == 1
        assert tf.transformation_fn.keywords["max_value"] == 100

    def test_populate_builtin_fn_arguments_standard_scaler(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.builtin_transformation_function.BuiltInTransformationFunction.standard_scaler_stats",
            return_value=(1, 100),
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def standard_scaler():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id, transformation_fn=standard_scaler, output_type="str"
        )

        # Act
        tf_engine.populate_builtin_fn_arguments(
            feature_name=None, transformation_function_instance=tf, stat_content=None
        )

        # Assert
        assert tf.transformation_fn.keywords["mean"] == 1
        assert tf.transformation_fn.keywords["std_dev"] == 100

    def test_populate_builtin_fn_arguments_robust_scaler(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.builtin_transformation_function.BuiltInTransformationFunction.robust_scaler_stats",
            return_value={24: 1, 49: 2, 74: 3},
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def robust_scaler():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id, transformation_fn=robust_scaler, output_type="str"
        )

        # Act
        tf_engine.populate_builtin_fn_arguments(
            feature_name=None, transformation_function_instance=tf, stat_content=None
        )

        # Assert
        assert tf.transformation_fn.keywords["p25"] == 1
        assert tf.transformation_fn.keywords["p50"] == 2
        assert tf.transformation_fn.keywords["p75"] == 3

    def test_populate_builtin_fn_arguments_label_encoder(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.builtin_transformation_function.BuiltInTransformationFunction.encoder_stats",
            return_value="test",
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def label_encoder():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id, transformation_fn=label_encoder, output_type="str"
        )

        # Act
        tf_engine.populate_builtin_fn_arguments(
            feature_name=None, transformation_function_instance=tf, stat_content=None
        )

        # Assert
        assert tf.transformation_fn.keywords["value_to_index"] == "test"

    def test_populate_builtin_attached_fns(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin",
            return_value=False,
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.populate_builtin_fn_arguments"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def testFunction():
            print("Test")

        tf_attached = transformation_function_attached.TransformationFunctionAttached(
            name="tf_name", transformation_function=testFunction
        )
        tf1_attached = transformation_function_attached.TransformationFunctionAttached(
            name="tf1_name", transformation_function=testFunction
        )

        transformation_fn_dict = dict()

        transformation_fn_dict["tf_name"] = tf_attached
        transformation_fn_dict["tf1_name"] = tf1_attached

        # Act
        tf_engine.populate_builtin_attached_fns(
            attached_transformation_fns=transformation_fn_dict, stat_content=None
        )

        # Assert
        assert transformation_fn_dict["tf_name"] == tf_attached
        assert transformation_fn_dict["tf1_name"] == tf1_attached

    def test_populate_builtin_attached_fns_is_builtin(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.populate_builtin_fn_arguments"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def testFunction():
            print("Test")

        tf_attached = transformation_function_attached.TransformationFunctionAttached(
            name="tf_name", transformation_function=testFunction
        )
        tf1_attached = transformation_function_attached.TransformationFunctionAttached(
            name="tf1_name", transformation_function=testFunction
        )

        transformation_fn_dict = dict()

        transformation_fn_dict["tf_name"] = tf_attached
        transformation_fn_dict["tf1_name"] = tf1_attached

        # Act
        tf_engine.populate_builtin_attached_fns(
            attached_transformation_fns=transformation_fn_dict, stat_content=None
        )

        # Assert
        assert transformation_fn_dict["tf_name"] != tf_attached
        assert transformation_fn_dict["tf1_name"] != tf1_attached

    def test_infer_spark_type_wrong_type(self):
        # Arrange
        feature_store_id = 99

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        # Act
        with pytest.raises(TypeError) as e_info:
            tf_engine.infer_spark_type("wrong")

        # Assert
        assert str(e_info.value) == "Not supported type wrong."

    def test_compute_transformation_fn_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            id=10,
        )

        # Act
        tf_engine.compute_transformation_fn_statistics(
            training_dataset_obj=td,
            builtin_tffn_features=None,
            label_encoder_features=None,
            feature_dataframe=None,
            feature_view_obj=None,
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 1
        )

    def test_populate_builtin_transformation_functions(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin"
        )
        mock_tf_engine_compute_transformation_fn_statistics = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.compute_transformation_fn_statistics"
        )
        mock_tf_engine_populate_builtin_attached_fns = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.populate_builtin_attached_fns"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def testFunction():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            transformation_fn=testFunction,
            builtin_source_code="",
            output_type="str",
        )

        def label_encoder():
            print("Test")

        tf_label_encoder = transformation_function.TransformationFunction(
            feature_store_id,
            transformation_fn=label_encoder,
            builtin_source_code="",
            output_type="str",
        )

        transformation_fn_dict = dict()

        transformation_fn_dict["tf_name"] = tf
        transformation_fn_dict["label_encoder"] = tf_label_encoder

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
            transformation_functions=transformation_fn_dict,
        )

        dataset = mocker.Mock()

        # Act
        tf_engine.populate_builtin_transformation_functions(
            training_dataset=td, feature_view_obj=None, dataset=dataset
        )

        # Assert
        assert mock_tf_engine_compute_transformation_fn_statistics.call_count == 1
        assert mock_tf_engine_populate_builtin_attached_fns.call_count == 1
        assert dataset.get.call_count == 0

    def test_populate_builtin_transformation_functions_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.is_builtin"
        )
        mock_tf_engine_compute_transformation_fn_statistics = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.compute_transformation_fn_statistics"
        )
        mock_tf_engine_populate_builtin_attached_fns = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.populate_builtin_attached_fns"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        def testFunction():
            print("Test")

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            transformation_fn=testFunction,
            builtin_source_code="",
            output_type="str",
        )

        def label_encoder():
            print("Test")

        tf_label_encoder = transformation_function.TransformationFunction(
            feature_store_id,
            transformation_fn=label_encoder,
            builtin_source_code="",
            output_type="str",
        )

        transformation_fn_dict = dict()

        transformation_fn_dict["tf_name"] = tf
        transformation_fn_dict["label_encoder"] = tf_label_encoder

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={"key": "value"},
            id=10,
            transformation_functions=transformation_fn_dict,
        )

        dataset = mocker.Mock()

        # Act
        tf_engine.populate_builtin_transformation_functions(
            training_dataset=td, feature_view_obj=None, dataset=dataset
        )

        # Assert
        assert mock_tf_engine_compute_transformation_fn_statistics.call_count == 1
        assert mock_tf_engine_populate_builtin_attached_fns.call_count == 1
        assert dataset.get.call_count == 1

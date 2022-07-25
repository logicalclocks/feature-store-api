#
#   Copyright 2022 Logical Clocks AB
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

# todo import as necessary for ../../../hsfs/transformation_function.py:143: TypeError
import unittest as unittest
import numpy as numpy
import datetime as datetime
from unittest.mock import patch, Mock

from python.hsfs import transformation_function, training_dataset, feature_view
from python.hsfs.core import (
    statistics_engine,
    builtin_transformation_function,
    transformation_function_engine,
    transformation_function_api,
)


class TestTransformationFunction(unittest.TestCase):
    def test_transformation_functions_save_builtin(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "register_transformation_fn",
        ) as mock_transformation_function_api, patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin:
            # Arrange
            mock_transformation_function_builtin.return_value = True
            tf = transformation_function.TransformationFunction(
                99, builtin_source_code="", output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            try:
                transformationFunctionEngine.save(tf)
            except:
                pass

            # Assert
            self.assertEqual(0, mock_transformation_function_api.call_count)
            self.assertEqual(1, mock_transformation_function_builtin.call_count)

    def test_transformation_functions_save_not_callable(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "register_transformation_fn",
        ) as mock_transformation_function_api, patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin:
            # Arrange
            mock_transformation_function_builtin.return_value = False
            tf = transformation_function.TransformationFunction(
                99, builtin_source_code="", output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            try:
                transformationFunctionEngine.save(tf)
            except:
                pass

            # Assert
            self.assertEqual(0, mock_transformation_function_api.call_count)
            self.assertEqual(1, mock_transformation_function_builtin.call_count)

    def test_transformation_functions_save(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "register_transformation_fn",
        ) as mock_transformation_function_api, patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin:
            # Arrange
            mock_transformation_function_builtin.return_value = False

            def testFunction():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99,
                transformation_fn=testFunction,
                builtin_source_code="",
                output_type="str",
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.save(tf)

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)
            self.assertEqual(1, mock_transformation_function_builtin.call_count)

    def test_transformation_functions_get_name(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "get_transformation_fn",
        ) as mock_transformation_function_api:
            # Arrange
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            tf2 = transformation_function.TransformationFunction(
                99, name="tf", version=2, builtin_source_code="", output_type="str"
            )
            mock_transformation_function_api.return_value = [tf, tf2]
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            result = transformationFunctionEngine.get_transformation_fn("fn")

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)
            self.assertEqual(1, result.version)

    def test_transformation_functions_get_name_version(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "get_transformation_fn",
        ) as mock_transformation_function_api:
            # Arrange
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            mock_transformation_function_api.return_value = [tf]
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            result = transformationFunctionEngine.get_transformation_fn("fn", 1)

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)
            self.assertEqual(1, result.version)

    def test_transformation_functions_get_no_result(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "get_transformation_fn",
        ) as mock_transformation_function_api:
            # Arrange
            mock_transformation_function_api.return_value = []
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            try:
                result = transformationFunctionEngine.get_transformation_fn("fn")
            except:
                result = None

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)
            self.assertEqual(None, result)

    def test_transformation_functions_get_all(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "get_transformation_fn",
        ) as mock_transformation_function_api:
            # Arrange
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            tf1 = transformation_function.TransformationFunction(
                99, name="tf", version=2, builtin_source_code="", output_type="str"
            )
            tf2 = transformation_function.TransformationFunction(
                99, name="tf2", version=1, builtin_source_code="", output_type="str"
            )
            mock_transformation_function_api.return_value = [tf, tf1, tf2]
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            result = transformationFunctionEngine.get_transformation_fns()

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)
            self.assertEqual(3, len(result))

    def test_transformation_functions_delete(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi, "delete"
        ) as mock_transformation_function_api:
            # Arrange
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.delete(tf)

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)

    def test_transformation_functions_get_td(self):
        with patch.object(
            transformation_function_api.TransformationFunctionApi,
            "get_td_transformation_fn",
        ) as mock_transformation_function_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            def testFunction():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99,
                name="tf",
                version=1,
                transformation_fn=testFunction,
                builtin_source_code="",
                output_type="str",
            )
            tf2 = transformation_function.TransformationFunction(
                99,
                name="tf2",
                version=1,
                transformation_fn=testFunction,
                builtin_source_code="",
                output_type="str",
            )
            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
            )
            mock_transformation_function_api.return_value = [tf, tf2]
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            results = transformationFunctionEngine.get_td_transformation_fn(td)

            # Assert
            self.assertEqual(1, mock_transformation_function_api.call_count)
            self.assertEqual(1, len(results))
            self.assertIn("testFunction", results)

    def test_transformation_functions_attach_td_with_fn(self):
        with patch("python.hsfs.core.feature_view_api.FeatureViewApi"):
            # Arrange
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn.transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            try:
                transformationFunctionEngine.attach_transformation_fn(
                    training_dataset_obj=td
                )
            except AttributeError:
                pass

            # Assert
            self.assertEqual(0, len(td._features))

    def test_transformation_functions_attach_td_without_fn(self):
        with patch("python.hsfs.core.feature_view_api.FeatureViewApi"):
            # Arrange
            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
                features=[],
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.attach_transformation_fn(
                training_dataset_obj=td
            )

            # Assert
            self.assertEqual(0, len(td._features))

    def test_transformation_functions_attach_fv_with_fn(self):
        with patch("python.hsfs.core.feature_view_api.FeatureViewApi"):
            # Arrange
            def testFunction():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99,
                name="tf",
                version=1,
                transformation_fn=testFunction,
                output_type="str",
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            fv = feature_view.FeatureView(
                name="test",
                query="",
                featurestore_id=99,
                transformation_functions=transformation_fn_dict,
                labels=[],
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.attach_transformation_fn(feature_view_obj=fv)

            # Assert
            self.assertEqual(1, len(fv._features))

    def test_transformation_functions_attach_fv_without_fn(self):
        with patch("python.hsfs.core.feature_view_api.FeatureViewApi"):
            # Arrange
            fv = feature_view.FeatureView(
                name="test", query="", featurestore_id=99, labels=[]
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.attach_transformation_fn(feature_view_obj=fv)

            # Assert
            self.assertEqual(0, len(fv._features))

    def test_transformation_functions_is_builtin(self):
        # Arrange
        tf = transformation_function.TransformationFunction(
            99,
            name="min_max_scaler",
            version=1,
            builtin_source_code="",
            output_type="str",
        )
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.is_builtin(tf)

        # Assert
        self.assertEqual(True, result)

    def test_transformation_functions_is_builtin_wrong_version(self):
        # Arrange
        tf = transformation_function.TransformationFunction(
            99,
            name="min_max_scaler",
            version=2,
            builtin_source_code="",
            output_type="str",
        )
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.is_builtin(tf)

        # Assert
        self.assertEqual(False, result)

    def test_transformation_functions_is_builtin_wrong_fn_name(self):
        # Arrange
        tf = transformation_function.TransformationFunction(
            99, name="tf", version=1, builtin_source_code="", output_type="str"
        )
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.is_builtin(tf)

        # Assert
        self.assertEqual(False, result)

    def test_transformation_functions_populate_builtin_fn_arguments_min_max_scaler(
        self,
    ):
        with patch.object(
            builtin_transformation_function.BuiltInTransformationFunction,
            "min_max_scaler_stats",
        ) as mock_builtin_transformation_function:
            # Arrange
            mock_builtin_transformation_function.return_value = (1, 100)

            def min_max_scaler():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=min_max_scaler, output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_fn_arguments(
                "test_feature", tf, None
            )

            # Assert
            self.assertEqual(1, mock_builtin_transformation_function.call_count)
            self.assertEqual(1, tf.transformation_fn.keywords["min_value"])
            self.assertEqual(100, tf.transformation_fn.keywords["max_value"])

    def test_transformation_functions_populate_builtin_fn_arguments_standard_scaler(
        self,
    ):
        with patch.object(
            builtin_transformation_function.BuiltInTransformationFunction,
            "standard_scaler_stats",
        ) as mock_builtin_transformation_function:
            # Arrange
            mock_builtin_transformation_function.return_value = (1, 100)

            def standard_scaler():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=standard_scaler, output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_fn_arguments(
                "test_feature", tf, None
            )

            # Assert
            self.assertEqual(1, mock_builtin_transformation_function.call_count)
            self.assertEqual(1, tf.transformation_fn.keywords["mean"])
            self.assertEqual(100, tf.transformation_fn.keywords["std_dev"])

    def test_transformation_functions_populate_builtin_fn_arguments_robust_scaler(self):
        with patch.object(
            builtin_transformation_function.BuiltInTransformationFunction,
            "robust_scaler_stats",
        ) as mock_builtin_transformation_function:
            # Arrange
            mock_builtin_transformation_function.return_value = {24: 1, 49: 2, 74: 3}

            def robust_scaler():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=robust_scaler, output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_fn_arguments(
                "test_feature", tf, None
            )

            # Assert
            self.assertEqual(1, mock_builtin_transformation_function.call_count)
            self.assertEqual(1, tf.transformation_fn.keywords["p25"])
            self.assertEqual(2, tf.transformation_fn.keywords["p50"])
            self.assertEqual(3, tf.transformation_fn.keywords["p75"])

    def test_transformation_functions_populate_builtin_fn_arguments_label_encoder(self):
        with patch.object(
            builtin_transformation_function.BuiltInTransformationFunction,
            "encoder_stats",
        ) as mock_builtin_transformation_function:
            # Arrange
            mock_builtin_transformation_function.return_value = "test"

            def label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=label_encoder, output_type="str"
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_fn_arguments(
                "test_feature", tf, None
            )

            # Assert
            self.assertEqual(1, mock_builtin_transformation_function.call_count)
            self.assertEqual("test", tf.transformation_fn.keywords["value_to_index"])

    def test_transformation_functions_populate_builtin_fn_arguments_wrong_fn(self):
        # Arrange
        def wrong_builtin_fn():
            print("Test")

        tf = transformation_function.TransformationFunction(
            99, transformation_fn=wrong_builtin_fn, output_type="str"
        )
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        try:
            result = transformationFunctionEngine.populate_builtin_fn_arguments(
                "test_feature", tf, None
            )
        except:
            result = None

        # Assert
        self.assertEqual(None, result)

    def test_transformation_functions_populate_builtin_attached_fns(self):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_is_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_fn_arguments",
        ) as mock_transformation_function_populate_builtin_fn_arguments:
            # Arrange
            mock_transformation_function_is_builtin.return_value = True
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            tf1 = transformation_function.TransformationFunction(
                99, name="tf", version=2, builtin_source_code="", output_type="str"
            )
            tf2 = transformation_function.TransformationFunction(
                99, name="tf2", version=1, builtin_source_code="", output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf, tf1, tf2]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn.transformation_fn
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_attached_fns(
                transformation_fn_dict, None
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_is_builtin.call_count)
            self.assertEqual(
                2, mock_transformation_function_populate_builtin_fn_arguments.call_count
            )

    def test_transformation_functions_populate_builtin_attached_fns_not_builtin(self):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_is_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_fn_arguments",
        ) as mock_transformation_function_populate_builtin_fn_arguments:
            # Arrange
            mock_transformation_function_is_builtin.return_value = False
            tf = transformation_function.TransformationFunction(
                99, name="tf", version=1, builtin_source_code="", output_type="str"
            )
            tf1 = transformation_function.TransformationFunction(
                99, name="tf", version=2, builtin_source_code="", output_type="str"
            )
            tf2 = transformation_function.TransformationFunction(
                99, name="tf2", version=1, builtin_source_code="", output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf, tf1, tf2]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn.transformation_fn
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_attached_fns(
                transformation_fn_dict, None
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_is_builtin.call_count)
            self.assertEqual(
                0, mock_transformation_function_populate_builtin_fn_arguments.call_count
            )

    def test_transformation_functions_infer_spark_type_string_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(str)

        # Assert
        self.assertEqual("StringType()", result)

    def test_transformation_functions_infer_spark_type_string_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("str")

        # Assert
        self.assertEqual("StringType()", result)

    def test_transformation_functions_infer_spark_type_string_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("string")

        # Assert
        self.assertEqual("StringType()", result)

    def test_transformation_functions_infer_spark_type_byte_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(bytes)

        # Assert
        self.assertEqual("BinaryType()", result)

    def test_transformation_functions_infer_spark_type_int8_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.int8)

        # Assert
        self.assertEqual("ByteType()", result)

    def test_transformation_functions_infer_spark_type_int8_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("int8")

        # Assert
        self.assertEqual("ByteType()", result)

    def test_transformation_functions_infer_spark_type_int8_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("byte")

        # Assert
        self.assertEqual("ByteType()", result)

    def test_transformation_functions_infer_spark_type_int16_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.int16)

        # Assert
        self.assertEqual("ShortType()", result)

    def test_transformation_functions_infer_spark_type_int16_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("int16")

        # Assert
        self.assertEqual("ShortType()", result)

    def test_transformation_functions_infer_spark_type_int16_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("short")

        # Assert
        self.assertEqual("ShortType()", result)

    def test_transformation_functions_infer_spark_type_int_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(int)

        # Assert
        self.assertEqual("IntegerType()", result)

    def test_transformation_functions_infer_spark_type_int_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("int")

        # Assert
        self.assertEqual("IntegerType()", result)

    def test_transformation_functions_infer_spark_type_int_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.int)

        # Assert
        self.assertEqual("IntegerType()", result)

    def test_transformation_functions_infer_spark_type_int_type_4(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.int32)

        # Assert
        self.assertEqual("IntegerType()", result)

    def test_transformation_functions_infer_spark_type_int64_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.int64)

        # Assert
        self.assertEqual("LongType()", result)

    def test_transformation_functions_infer_spark_type_int64_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("int64")

        # Assert
        self.assertEqual("LongType()", result)

    def test_transformation_functions_infer_spark_type_int64_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("long")

        # Assert
        self.assertEqual("LongType()", result)

    def test_transformation_functions_infer_spark_type_int64_type_4(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("bigint")

        # Assert
        self.assertEqual("LongType()", result)

    def test_transformation_functions_infer_spark_type_float_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(float)

        # Assert
        self.assertEqual("FloatType()", result)

    def test_transformation_functions_infer_spark_type_float_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("float")

        # Assert
        self.assertEqual("FloatType()", result)

    def test_transformation_functions_infer_spark_type_float_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.float)

        # Assert
        self.assertEqual("FloatType()", result)

    def test_transformation_functions_infer_spark_type_double_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.float64)

        # Assert
        self.assertEqual("DoubleType()", result)

    def test_transformation_functions_infer_spark_type_double_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("float64")

        # Assert
        self.assertEqual("DoubleType()", result)

    def test_transformation_functions_infer_spark_type_double_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("double")

        # Assert
        self.assertEqual("DoubleType()", result)

    def test_transformation_functions_infer_spark_type_timestamp_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(datetime.datetime)

        # Assert
        self.assertEqual("TimestampType()", result)

    def test_transformation_functions_infer_spark_type_timestamp_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.datetime64)

        # Assert
        self.assertEqual("TimestampType()", result)

    def test_transformation_functions_infer_spark_type_date_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(datetime.date)

        # Assert
        self.assertEqual("DateType()", result)

    def test_transformation_functions_infer_spark_type_bool_type_1(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(bool)

        # Assert
        self.assertEqual("BooleanType()", result)

    def test_transformation_functions_infer_spark_type_bool_type_2(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("boolean")

        # Assert
        self.assertEqual("BooleanType()", result)

    def test_transformation_functions_infer_spark_type_bool_type_3(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type("bool")

        # Assert
        self.assertEqual("BooleanType()", result)

    def test_transformation_functions_infer_spark_type_bool_type_4(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        result = transformationFunctionEngine.infer_spark_type(numpy.bool)

        # Assert
        self.assertEqual("BooleanType()", result)

    def test_transformation_functions_infer_spark_type_wrong_type(self):
        # Arrange
        transformationFunctionEngine = (
            transformation_function_engine.TransformationFunctionEngine(99)
        )

        # Act
        try:
            result = transformationFunctionEngine.infer_spark_type("wrong")
        except TypeError:
            result = None

        # Assert
        self.assertEqual(None, result)

    def test_transformation_functions_compute_transformation_fn_statistics(self):
        with patch.object(
            statistics_engine.StatisticsEngine, "compute_transformation_fn_statistics"
        ) as mock_statistics_engine, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.compute_transformation_fn_statistics(
                td, None, None, None, None
            )

            # Assert
            self.assertEqual(1, mock_statistics_engine.call_count)

    def test_transformation_functions_populate_builtin_transformation_functions_label_encoder_is_builtin_splits(
        self,
    ):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "compute_transformation_fn_statistics",
        ) as mock_compute_transformation_fn_statistics, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_attached_fns",
        ) as mock_populate_builtin_attached_fns, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            mock_transformation_function_builtin.return_value = True

            def label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=label_encoder, output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={"key": "value"},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
                train_split=True,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_transformation_functions(
                td, None, Mock()
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_builtin.call_count)
            self.assertEqual(1, mock_compute_transformation_fn_statistics.call_count)
            self.assertEqual(1, mock_populate_builtin_attached_fns.call_count)

    def test_transformation_functions_populate_builtin_transformation_functions_is_builtin_splits(
        self,
    ):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "compute_transformation_fn_statistics",
        ) as mock_compute_transformation_fn_statistics, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_attached_fns",
        ) as mock_populate_builtin_attached_fns, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            mock_transformation_function_builtin.return_value = True

            def not_label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=not_label_encoder, output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={"key": "value"},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
                train_split=True,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_transformation_functions(
                td, None, Mock()
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_builtin.call_count)
            self.assertEqual(1, mock_compute_transformation_fn_statistics.call_count)
            self.assertEqual(1, mock_populate_builtin_attached_fns.call_count)

    def test_transformation_functions_populate_builtin_transformation_functions_label_encoder_is_builtin(
        self,
    ):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "compute_transformation_fn_statistics",
        ) as mock_compute_transformation_fn_statistics, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_attached_fns",
        ) as mock_populate_builtin_attached_fns, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            mock_transformation_function_builtin.return_value = True

            def label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=label_encoder, output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
                train_split=True,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_transformation_functions(
                td, None, None
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_builtin.call_count)
            self.assertEqual(1, mock_compute_transformation_fn_statistics.call_count)
            self.assertEqual(1, mock_populate_builtin_attached_fns.call_count)

    def test_transformation_functions_populate_builtin_transformation_functions_is_builtin(
        self,
    ):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "compute_transformation_fn_statistics",
        ) as mock_compute_transformation_fn_statistics, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_attached_fns",
        ) as mock_populate_builtin_attached_fns, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            mock_transformation_function_builtin.return_value = True

            def not_label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=not_label_encoder, output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
                train_split=True,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_transformation_functions(
                td, None, None
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_builtin.call_count)
            self.assertEqual(1, mock_compute_transformation_fn_statistics.call_count)
            self.assertEqual(1, mock_populate_builtin_attached_fns.call_count)

    def test_transformation_functions_populate_builtin_transformation_functions_label_encoder(
        self,
    ):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "compute_transformation_fn_statistics",
        ) as mock_compute_transformation_fn_statistics, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_attached_fns",
        ) as mock_populate_builtin_attached_fns, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            mock_transformation_function_builtin.return_value = False

            def label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=label_encoder, output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={"key": "value"},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
                train_split=True,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_transformation_functions(
                td, None, None
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_builtin.call_count)
            self.assertEqual(0, mock_compute_transformation_fn_statistics.call_count)
            self.assertEqual(0, mock_populate_builtin_attached_fns.call_count)

    def test_transformation_functions_populate_builtin_transformation_functions(self):
        with patch.object(
            transformation_function_engine.TransformationFunctionEngine, "is_builtin"
        ) as mock_transformation_function_builtin, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "compute_transformation_fn_statistics",
        ) as mock_compute_transformation_fn_statistics, patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "populate_builtin_attached_fns",
        ) as mock_populate_builtin_attached_fns, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            mock_transformation_function_builtin.return_value = False

            def not_label_encoder():
                print("Test")

            tf = transformation_function.TransformationFunction(
                99, transformation_fn=not_label_encoder, output_type="str"
            )
            transformation_fn_dict = {}
            for attached_transformation_fn in [tf]:
                transformation_fn_dict[
                    attached_transformation_fn.name
                ] = attached_transformation_fn

            td = training_dataset.TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={"key": "value"},
                id=0,
                transformation_functions=transformation_fn_dict,
                features=[],
                train_split=80,
            )
            transformationFunctionEngine = (
                transformation_function_engine.TransformationFunctionEngine(99)
            )

            # Act
            transformationFunctionEngine.populate_builtin_transformation_functions(
                td, None, None
            )

            # Assert
            self.assertEqual(2, mock_transformation_function_builtin.call_count)
            self.assertEqual(0, mock_compute_transformation_fn_statistics.call_count)
            self.assertEqual(0, mock_populate_builtin_attached_fns.call_count)

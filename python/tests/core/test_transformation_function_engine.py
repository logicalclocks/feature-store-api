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

import pandas as pd
from hsfs import (
    engine,
    feature,
    feature_group,
    feature_view,
    training_dataset,
    transformation_function,
)
from hsfs.core import transformation_function_engine
from hsfs.hopsworks_udf import hopsworks_udf


fg1 = feature_group.FeatureGroup(
    name="test1",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[
        feature.Feature("id"),
        feature.Feature("label"),
        feature.Feature("tf_name"),
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
    features=[feature.Feature("id"), feature.Feature("tf1_name")],
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
        feature.Feature("id"),
        feature.Feature("tf_name"),
        feature.Feature("tf1_name"),
        feature.Feature("tf3_name"),
    ],
    id=12,
    stream=False,
)
engine.init("python")
query = fg1.select_all().join(fg2.select(["tf1_name"]), on=["id"])
query_self_join = fg1.select_all().join(fg1.select_all(), on=["id"], prefix="fg1_")
query_prefix = (
    fg1.select_all()
    .join(fg2.select(["tf1_name"]), on=["id"], prefix="second_")
    .join(fg3.select(["tf_name", "tf1_name"]), on=["id"], prefix="third_")
)


class TestTransformationFunctionEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @hopsworks_udf(int)
        def testFunction(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction,
        )

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

        @hopsworks_udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        @hopsworks_udf(float)
        def testFunction2(data2, statistics_data2):
            return data2 + 1

        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction2,
        )

        transformations = [tf1, tf2]

        mock_tf_api.return_value.get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine.get_transformation_fn(name=None, version=None)

        # Assert
        assert mock_tf_api.return_value.get_transformation_fn.call_count == 1
        assert result == transformations

    def test_get_transformation_fns(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @hopsworks_udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        @hopsworks_udf(float)
        def testFunction2(data2, statistics_data2):
            return data2 + 1

        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction2,
        )

        transformations = [tf1, tf2]

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

        @hopsworks_udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        # Act
        tf_engine.delete(transformation_function_instance=tf1)

        # Assert
        assert mock_tf_api.return_value.delete.call_count == 1

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
            statistics_features=None,
            label_encoder_features=None,
            feature_dataframe=None,
            feature_view_obj=None,
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 1
        )

    def test_compute_and_set_feature_statistics_no_split(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @hopsworks_udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("label")],
            id=11,
            stream=False,
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
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        dataset = pd.DataFrame()

        # Act
        tf_engine.compute_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 0
        )

    def test_compute_and_set_feature_statistics_train_test_split(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @hopsworks_udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("label")],
            id=11,
            stream=False,
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.8, "test": 0.2},
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        dataset = pd.DataFrame()

        # Act
        tf_engine.compute_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 0
        )

    def test_get_and_set_feature_statistics_no_statistics_required(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @hopsworks_udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("label")],
            id=11,
            stream=False,
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.8, "test": 0.2},
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        # Act
        tf_engine.get_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_s_engine.return_value.get.call_count == 0

    def test_get_and_set_feature_statistics_statistics_required(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @hopsworks_udf(int)
        def testFunction1(col1, statistics_col1):
            return col1 + statistics_col1.mean

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
        )

        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("label")],
            id=11,
            stream=False,
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.8, "test": 0.2},
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        # Act
        tf_engine.get_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_s_engine.return_value.get.call_count == 1

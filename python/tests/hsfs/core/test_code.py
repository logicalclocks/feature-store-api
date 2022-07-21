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

import os
import sys
import unittest
from unittest.mock import Mock, patch

from python.hsfs import engine
from python.hsfs.core.code_engine import CodeEngine, RunType
from python.hsfs.training_dataset import TrainingDataset
from python.hsfs.core.code_api import CodeApi
from python.hsfs.feature_group import FeatureGroup


class TestCodeTrainingDataset(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("HOPSWORKS_KERNEL_ID", None)
        os.environ.pop("HOPSWORKS_JOB_NAME", None)
        sys.modules.pop("pyspark.dbutils", None)

    def test_save_jupyter(self):
        with patch.object(CodeApi, "post") as mock_code_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            os.environ.setdefault("HOPSWORKS_KERNEL_ID", "1")

            td = TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
            )
            codeEngine = CodeEngine(99, "trainingdatasets")

            # Act
            codeEngine.save_code(td)

            # Assert
            self.assertEqual(1, mock_code_api.call_count)
            self.assertEqual(
                RunType.JUPYTER, mock_code_api.call_args.kwargs["code_type"]
            )

    def test_save_job(self):
        with patch.object(CodeApi, "post") as mock_code_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            os.environ.setdefault("HOPSWORKS_JOB_NAME", "1")

            td = TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
            )
            codeEngine = CodeEngine(99, "trainingdatasets")

            # Act
            codeEngine.save_code(td)

            # Assert
            self.assertEqual(1, mock_code_api.call_count)
            self.assertEqual(RunType.JOB, mock_code_api.call_args.kwargs["code_type"])

    def test_save_databricks(self):
        with patch.object(CodeApi, "post") as mock_code_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ):
            # Arrange
            sys.modules["pyspark.dbutils"] = Mock()
            sys.modules["pyspark.dbutils"].__spec__ = Mock()
            json = """{"extraContext": {"notebook_path": "test_path"},
                    "tags": {"browserHostName": "test_browser_host_name"}}"""
            sys.modules[
                "pyspark.dbutils"
            ].DBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson = Mock(
                return_value=json
            )

            td = TrainingDataset(
                name="test",
                version=1,
                data_format="CSV",
                featurestore_id=99,
                splits={},
                id=0,
            )
            codeEngine = CodeEngine(99, "trainingdatasets")

            # Act
            codeEngine.save_code(td)

            # Assert
            self.assertEqual(1, mock_code_api.call_count)
            self.assertEqual(
                RunType.DATABRICKS, mock_code_api.call_args.kwargs["code_type"]
            )


class TestCodeFeatureGroup(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("HOPSWORKS_KERNEL_ID", None)
        os.environ.pop("HOPSWORKS_JOB_NAME", None)
        sys.modules.pop("pyspark.dbutils", None)

    def test_save_jupyter(self):
        with patch.object(CodeApi, "post") as mock_code_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ), patch.object(engine, "get_type"):
            # Arrange
            os.environ.setdefault("HOPSWORKS_KERNEL_ID", "1")

            fg = FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            codeEngine = CodeEngine(99, "featuregroups")

            # Act
            codeEngine.save_code(fg)

            # Assert
            self.assertEqual(1, mock_code_api.call_count)
            self.assertEqual(
                RunType.JUPYTER, mock_code_api.call_args.kwargs["code_type"]
            )

    def test_save_job(self):
        with patch.object(CodeApi, "post") as mock_code_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ), patch.object(engine, "get_type"):
            # Arrange
            os.environ.setdefault("HOPSWORKS_JOB_NAME", "1")

            fg = FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            codeEngine = CodeEngine(99, "featuregroups")

            # Act
            codeEngine.save_code(fg)

            # Assert
            self.assertEqual(1, mock_code_api.call_count)
            self.assertEqual(RunType.JOB, mock_code_api.call_args.kwargs["code_type"])

    def test_save_databricks(self):
        with patch.object(CodeApi, "post") as mock_code_api, patch(
            "python.hsfs.core.feature_view_api.FeatureViewApi"
        ), patch.object(engine, "get_type"):
            # Arrange
            sys.modules["pyspark.dbutils"] = Mock()
            sys.modules["pyspark.dbutils"].__spec__ = Mock()
            json = """{"extraContext": {"notebook_path": "test_path"},
                        "tags": {"browserHostName": "test_browser_host_name"}}"""
            sys.modules[
                "pyspark.dbutils"
            ].DBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson = Mock(
                return_value=json
            )

            fg = FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            codeEngine = CodeEngine(99, "featuregroups")

            # Act
            codeEngine.save_code(fg)

            # Assert
            self.assertEqual(1, mock_code_api.call_count)
            self.assertEqual(
                RunType.DATABRICKS, mock_code_api.call_args.kwargs["code_type"]
            )

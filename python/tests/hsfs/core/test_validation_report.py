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

import unittest
from unittest.mock import patch

from python.hsfs import util, client, engine, feature_group, validation_report
from python.hsfs.core import (
    validation_report_engine,
    validation_report_api
)

class TestValidationReport(unittest.TestCase):
    def test_validation_report_save(self):
        with patch.object(
            validation_report_api.ValidationReportApi, "create"
        ) as mock_validation_report_api, patch.object(engine, "get_type"), patch.object(
            validation_report_engine.ValidationReportEngine, "_get_validation_report_url"
        ) as mock_validation_report_url:
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            validationReportEngine = validation_report_engine.ValidationReportEngine(99)
            vr = validation_report.ValidationReport(True, [], {}, {})

            # Act
            validationReportEngine.save(fg, vr)

            # Assert
            self.assertEqual(1, mock_validation_report_api.call_count)
            self.assertEqual(1, mock_validation_report_url.call_count)

    def test_validation_report_get_last(self):
        with patch.object(
                validation_report_api.ValidationReportApi, "get_last"
        ) as mock_validation_report_api, patch.object(engine, "get_type"), patch.object(
            validation_report_engine.ValidationReportEngine, "_get_validation_report_url"
        ) as mock_validation_report_url:
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            validationReportEngine = validation_report_engine.ValidationReportEngine(99)

            # Act
            validationReportEngine.get_last(fg)

            # Assert
            self.assertEqual(1, mock_validation_report_api.call_count)
            self.assertEqual(1, mock_validation_report_url.call_count)

    def test_validation_report_get_all(self):
        with patch.object(
                validation_report_api.ValidationReportApi, "get_all"
        ) as mock_validation_report_api, patch.object(engine, "get_type"), patch.object(
            validation_report_engine.ValidationReportEngine, "_get_validation_report_url"
        ) as mock_validation_report_url:
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            validationReportEngine = validation_report_engine.ValidationReportEngine(99)

            # Act
            validationReportEngine.get_all(fg)

            # Assert
            self.assertEqual(1, mock_validation_report_api.call_count)
            self.assertEqual(1, mock_validation_report_url.call_count)

    def test_validation_report_delete(self):
        with patch.object(
                validation_report_api.ValidationReportApi, "delete"
        ) as mock_validation_report_api, patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            validationReportEngine = validation_report_engine.ValidationReportEngine(99)
            vr = validation_report.ValidationReport(True, [], {}, {})

            # Act
            validationReportEngine.delete(fg, vr)

            # Assert
            self.assertEqual(1, mock_validation_report_api.call_count)

    def test_validation_report_get_url(self):
        with patch.object(client, "get_instance") as mock_client, patch.object(
                util, "get_hostname_replaced_url"
        ) as mock_util, patch.object(engine, "get_type"):
            # Arrange
            mock_client.return_value._project_id = 10
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            validationReportEngine = validation_report_engine.ValidationReportEngine(99)

            # Act
            validationReportEngine._get_validation_report_url(fg)

            # Assert
            self.assertEqual(1, mock_util.call_count)
            self.assertEqual("/p/10/fs/99/fg/0", mock_util.call_args.args[0])
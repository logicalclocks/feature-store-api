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

from python.hsfs import util, engine, client, expectation_suite, feature_group
from python.hsfs.core import expectation_suite_api, expectation_suite_engine


class TestExpectationSuite(unittest.TestCase):
    def test_expectation_suite_create(self):
        with patch.object(
            expectation_suite_api.ExpectationSuiteApi, "create"
        ) as mock_expectation_api, patch.object(engine, "get_type"), patch.object(
            expectation_suite_engine.ExpectationSuiteEngine,
            "_get_expectation_suite_url",
        ) as mock_expectation_suite_url:
            # Arrange
            mock_expectation_suite_url.return_value = "localhost"
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            expectationEngine = expectation_suite_engine.ExpectationSuiteEngine(99)
            expectation = expectation_suite.ExpectationSuite(
                expectation_suite_name="", expectations="", meta="{}"
            )

            # Act
            expectationEngine.save(fg, expectation)

            # Assert
            self.assertEqual(1, mock_expectation_api.call_count)
            self.assertEqual(1, mock_expectation_suite_url.call_count)

    def test_expectation_suite_get(self):
        with patch.object(
            expectation_suite_api.ExpectationSuiteApi, "get"
        ) as mock_expectation_api, patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            expectationEngine = expectation_suite_engine.ExpectationSuiteEngine(99)

            # Act
            expectationEngine.get(fg)

            # Assert
            self.assertEqual(1, mock_expectation_api.call_count)

    def test_expectation_suite_delete(self):
        with patch.object(
            expectation_suite_api.ExpectationSuiteApi, "delete"
        ) as mock_expectation_api, patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            expectationEngine = expectation_suite_engine.ExpectationSuiteEngine(99)

            # Act
            expectationEngine.delete(fg)

            # Assert
            self.assertEqual(1, mock_expectation_api.call_count)

    def test_expectation_suite_get_url(self):
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
            expectationEngine = expectation_suite_engine.ExpectationSuiteEngine(99)

            # Act
            expectationEngine._get_expectation_suite_url(fg)

            # Assert
            self.assertEqual(1, mock_util.call_count)
            self.assertEqual("/p/10/fs/99/fg/0", mock_util.call_args.args[0])

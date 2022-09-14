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

from hsfs import feature_group
from hsfs.core import expectation_suite_engine


class TestExpectationSuiteEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99
        expectation_suite_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mock_es_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi"
        )
        mock_es_engine_get_expectation_suite_url = mocker.patch(
            "hsfs.core.expectation_suite_engine.ExpectationSuiteEngine._get_expectation_suite_url"
        )
        mock_print = mocker.patch("builtins.print")

        es_engine = expectation_suite_engine.ExpectationSuiteEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        mock_es_engine_get_expectation_suite_url.return_value = expectation_suite_url

        # Act
        es_engine.save(feature_group=fg, expectation_suite=None)

        # Assert
        assert mock_es_api.return_value.create.call_count == 1
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][
            0
        ] == "Attached expectation suite to featuregroup, edit it at {}".format(
            expectation_suite_url
        )

    def test_get(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_es_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi"
        )

        es_engine = expectation_suite_engine.ExpectationSuiteEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        es_engine.get(feature_group=fg)

        # Assert
        assert mock_es_api.return_value.get.call_count == 1

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_es_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi"
        )

        es_engine = expectation_suite_engine.ExpectationSuiteEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        es_engine.delete(feature_group=fg)

        # Assert
        assert mock_es_api.return_value.delete.call_count == 1

    def test_get_expectation_suite_url(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hsfs.util.get_hostname_replaced_url"
        )

        es_engine = expectation_suite_engine.ExpectationSuiteEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        mock_client_get_instance.return_value._project_id = 50

        # Act
        es_engine._get_expectation_suite_url(feature_group=fg)

        # Assert
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args[0][0] == "/p/50/fs/99/fg/10"
        )

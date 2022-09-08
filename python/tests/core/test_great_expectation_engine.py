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

from hsfs import feature_group, expectation_suite
from hsfs.core import great_expectation_engine


class TestCodeEngine:
    def test_validate(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        mock_fg_get_expectation_suite.return_value = None

        # Act
        ge_engine.validate(
            feature_group=fg, dataframe=None, save_report=None, validation_options={}
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 0

    def test_validate_suite(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        suite = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": False}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            save_report=None,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 0

    def test_validate_suite_validation_options(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        suite = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": True}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            save_report=None,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 1

    def test_validate_suite_validation_options_save_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        suite = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": True}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            save_report=True,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 1
        assert mock_vr.call_count == 0

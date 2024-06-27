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

import hsfs.expectation_suite as es
import pandas as pd
import pytest
from hsfs import feature_group, validation_report
from hsfs.core import great_expectation_engine
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS


if HAS_GREAT_EXPECTATIONS:
    import great_expectations


class TestCodeEngine:
    def test_validate(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
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

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
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

        suite = es.ExpectationSuite(
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

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_validate_suite_validation_options(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
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

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": True}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_validate_suite_validation_options_save_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
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

        suite = es.ExpectationSuite(
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

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_convert_expectation_suite(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = great_expectations.core.ExpectationSuite(
            expectation_suite_name="suite_name",
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=great_expectations.core.ExpectationSuite(
                expectation_suite_name="attached_to_feature_group",
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        converted_suite = ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg, expectation_suite=suite
        )

        # Assert
        assert isinstance(converted_suite, es.ExpectationSuite)
        assert converted_suite.expectation_suite_name == "suite_name"
        assert mock_fg_get_expectation_suite.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_fake_convert_expectation_suite(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=[], meta={}
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=great_expectations.core.ExpectationSuite(
                expectation_suite_name="attached_to_feature_group",
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        converted_suite = ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg, expectation_suite=suite
        )

        # Assert
        assert isinstance(converted_suite, es.ExpectationSuite)
        assert converted_suite.expectation_suite_name == "suite_name"
        assert mock_fg_get_expectation_suite.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_fetch_expectation_suite(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = None

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=great_expectations.core.ExpectationSuite(
                expectation_suite_name="attached_to_feature_group",
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg, expectation_suite=suite
        )

        # Assert
        assert mock_fg_get_expectation_suite.call_count == 1

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_fetch_expectation_suite_false(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = None

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=great_expectations.core.ExpectationSuite(
                expectation_suite_name="attached_to_feature_group",
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        result = ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg,
            expectation_suite=suite,
            validation_options={"fetch_expectation_suite": False},
        )

        # Assert
        assert mock_fg_get_expectation_suite.call_count == 0
        assert result.expectation_suite_name == "attached_to_feature_group"
        assert isinstance(result, es.ExpectationSuite)

    def test_should_run_validation_based_on_suite(self):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=True,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={}
        )

        # Assert
        assert run_validation is True

    def test_should_not_run_validation_based_on_suite(self):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=False,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={}
        )

        # Assert
        assert run_validation is False

    def test_should_run_validation_based_validation_options(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=True,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={"run_validation": False}
        )

        # Assert
        assert run_validation is False

    def test_should_not_run_validation_based_validation_options(self):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=False,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={"run_validation": True}
        )

        # Assert
        assert run_validation is True

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_not_save_but_convert_report(self, mocker):
        # Arrange
        feature_store_id = 99
        save_report = False
        ge_type = False
        validation_options = {}

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        report = great_expectations.core.ExpectationSuiteValidationResult()

        mock_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )

        # Act
        converted_report = ge_engine.save_or_convert_report(
            save_report=save_report,
            ge_type=ge_type,
            feature_group=fg,
            report=report,
            validation_options=validation_options,
        )

        # Assert
        assert isinstance(converted_report, validation_report.ValidationReport)
        assert mock_save_validation_report.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_save_but_not_convert_report(self, mocker):
        # Arrange
        feature_store_id = 99
        save_report = True
        ge_type = True
        validation_options = {}

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        report = great_expectations.core.ExpectationSuiteValidationResult()

        mock_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )

        # Act
        ge_engine.save_or_convert_report(
            save_report=save_report,
            ge_type=ge_type,
            feature_group=fg,
            report=report,
            validation_options=validation_options,
        )

        # Assert
        assert mock_save_validation_report.call_count == 1

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_not_save_and_not_convert_report(self, mocker):
        # Arrange
        feature_store_id = 99
        save_report = True
        ge_type = True
        # This should override save_report
        validation_options = {"save_report": False}

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        report = great_expectations.core.ExpectationSuiteValidationResult()

        mock_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )

        # Act
        converted_report = ge_engine.save_or_convert_report(
            save_report=save_report,
            ge_type=ge_type,
            feature_group=fg,
            report=report,
            validation_options=validation_options,
        )

        # Assert
        assert isinstance(
            converted_report, great_expectations.core.ExpectationSuiteValidationResult
        )
        assert mock_save_validation_report.call_count == 0

    @pytest.mark.skipif(
        HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is installed, do not check for module not found error",
    )
    def test_raise_module_not_found_error(self, mocker):
        # Arrange
        ge_engine = great_expectation_engine.GreatExpectationEngine(feature_store_id=11)
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=11,
            partition_key=[],
            primary_key=["id"],
        )
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=True,
        )
        mocker.patch("hsfs.util.get_feature_group_url", return_value="https://url")

        # Act
        with pytest.raises(ModuleNotFoundError):
            ge_engine.validate(
                feature_group=fg,
                dataframe=df,
                expectation_suite=suite,
            )

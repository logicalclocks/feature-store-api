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

from hsfs.core import validation_result_engine
import pandas as pd
import pytest


class TestValidationResultEngine:
    def test_get_validation_history(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        expectation_id = 31

        mocker.patch("hsfs.client.get_instance")
        mock_validation_result_api = mocker.patch(
            "hsfs.core.validation_result_api.ValidationResultApi"
        )

        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        # Act
        vr_engine.get_validation_history(expectation_id=expectation_id)

        # Assert
        assert (
            mock_validation_result_api.return_value.get_validation_history.call_count
            == 1
        )

    def test_verify_sort_by(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        input_with_type_error = [
            1,
            2.0,
            {},
            ["validation_time:desc"],
            pd.Timestamp(200),
        ]
        input_with_value_error = [
            "sort_by=validation_time:desc",
            "desc",
            "asc",
            "validation_time_gte:21343243",
            "ingestion_result_eq:REJECTED" "validation_time:confused",
        ]
        clean_inputs = ["validation_time:desc", "validation_time:asc"]

        # Act
        for failing_type_input in input_with_type_error:
            with pytest.raises(TypeError):
                vr_engine._verify_sort_by(failing_type_input)

        for failing_value_input in input_with_value_error:
            with pytest.raises(ValueError):
                vr_engine._verify_sort_by(failing_value_input)

        for passing_input in clean_inputs:
            vr_engine._verify_sort_by(passing_input)

        # Assert
        assert True

    def test_verify_filter_by(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        input_with_type_error = [1, 2.0, {}, pd.Timestamp(200)]
        input_with_value_error = [
            "sort_by=validation_time:desc",
            "desc",
            "asc",
            "validation_time_gte:2134WW243",
            "ingestion_result_eq:REJ3CTED",
            "validation_time:confused",
            ["validation_time_gte:2134WW243", "validation_time_lte:2134WW243"],
            ["validation_time_gte:2134243", "ingestion_result_eq:REJ3CTED"],
        ]
        clean_inputs = [
            "ingestion_result_eq:REJECTED",
            "ingestion_result_eq:INGESTED",
            "iNGEstion_REsult_eQ:INGESTED",
            "VALIDATION_time_Gte:213321",
            "validation_time_gte:12213",
            "validation_time_gt:12213",
            "validation_time_eq:12213",
            "validation_time_lte:12213",
            "validation_time_lt:12213213",
            [],
        ]

        # Act
        for failing_type_input in input_with_type_error:
            with pytest.raises(TypeError):
                vr_engine._verify_filter_by(failing_type_input)

        for failing_value_input in input_with_value_error:
            with pytest.raises(ValueError):
                vr_engine._verify_filter_by(failing_value_input)

        for passing_input in clean_inputs:
            vr_engine._verify_filter_by(passing_input)

        # Assert
        assert True

    def test_match_filter_by_validation_time_or_ingestion_result(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )
        input_with_value_error = [
            "sort_by=validation_time:desc",
            "desc",
            "asc",
            "validation_time_gte:2134WW243",
            "ingestion_result_eq:REJ3CTED",
            "validation_time:confused",
        ]
        clean_inputs = [
            "ingestion_result_eq:REJECTED",
            "ingestion_result_eq:INGESTED",
            "iNGEstion_REsult_eQ:INGESTED",
            "VALIDATION_time_Gte:213321",
            "validation_time_gte:12213",
            "validation_time_gt:12213",
            "validation_time_eq:12213"
            "validation_time_lte:12213"
            "validation_time_lt:12213213",
        ]

        # Act
        for failing_value_input in input_with_value_error:
            with pytest.raises(ValueError):
                vr_engine._match_filter_by_validation_time_or_ingestion_result(
                    failing_value_input
                )

        for passing_input in clean_inputs:
            vr_engine._match_filter_by_validation_time_or_ingestion_result(
                passing_input
            )

        # Assert
        assert True

    def test_verify_offset(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        input_with_type_error = ["1", 2.0, {}, ["2"]]
        input_with_value_error = [-1]
        clean_inputs = [0, 10]

        # Act
        for failing_type_input in input_with_type_error:
            with pytest.raises(TypeError):
                vr_engine._verify_offset(failing_type_input)

        for failing_value_input in input_with_value_error:
            with pytest.raises(ValueError):
                vr_engine._verify_offset(failing_value_input)

        for passing_input in clean_inputs:
            vr_engine._verify_offset(passing_input)

        # Assert
        assert True

    def test_verify_limit(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        input_with_type_error = ["1", 2.0, {}, ["2"]]
        input_with_value_error = [-1]
        clean_inputs = [0, 10]

        # Act
        for failing_type_input in input_with_type_error:
            with pytest.raises(TypeError):
                vr_engine._verify_limit(failing_type_input)

        for failing_value_input in input_with_value_error:
            with pytest.raises(ValueError):
                vr_engine._verify_limit(failing_value_input)

        for passing_input in clean_inputs:
            vr_engine._verify_limit(passing_input)

        # Assert
        assert True

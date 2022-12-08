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
from hsfs.util import convert_event_time_to_timestamp
from datetime import date, datetime
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

    def test_build_query_params(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        value_error_inputs = [
            {
                "filter_by": ["UNKNOWN", "INSERTED"],
                "start_validation_time": None,
                "end_validation_time": None,
            },
            {
                "filter_by": ["ingested", "EXPEIRMENT"],
                "start_validation_time": datetime.now(),
                "end_validation_time": datetime(2022, 1, 1, 0, 0, 0),
            },
        ]

        correct_inputs = [
            {
                "filter_by": ["ingested", "EXPERIMENT"],
                "start_validation_time": datetime(2022, 1, 1, 0, 0, 0),
                "end_validation_time": datetime.now(),
            },
            {
                "filter_by": ["UNKNOWN"],
                "start_validation_time": date.fromisoformat("2022-01-01"),
                "end_validation_time": None,
            },
            {
                "filter_by": ["REJECTED", "FG_DATA"],
                "start_validation_time": 12,
                "end_validation_time": "2022-01-01",
            },
            {
                "filter_by": [],
                "start_validation_time": None,
                "end_validation_time": None,
            },
        ]

        # Act
        vr_engine = validation_result_engine.ValidationResultEngine(
            feature_group_id=feature_group_id, feature_store_id=feature_store_id
        )

        for error_inputs in value_error_inputs:
            with pytest.raises(ValueError):
                vr_engine._build_query_params(**error_inputs)

        correct_outputs = []
        for correct_input in correct_inputs:
            correct_outputs.append(vr_engine._build_query_params(**correct_input))

        # Assert
        # All cases
        for correct_output in correct_outputs:
            assert correct_output["sort_by"] == "validation_time:desc"
            assert "offset" not in correct_output.keys()
            assert "limit" not in correct_output.keys()

        # First case
        assert len(correct_outputs[0]["filter_by"]) == 4
        assert "ingestion_result_eq:INGESTED" in correct_outputs[0]["filter_by"]
        assert "ingestion_result_eq:EXPERIMENT" in correct_outputs[0]["filter_by"]
        filter_validation_gte = [
            val
            for val in filter(
                lambda x: "validation_time_gte:" in x, correct_outputs[0]["filter_by"]
            )
        ]
        assert len(filter_validation_gte) == 1
        assert int(filter_validation_gte[0][20:]) == convert_event_time_to_timestamp(
            correct_inputs[0]["start_validation_time"]
        )
        filter_validation_lte = [
            val
            for val in filter(
                lambda x: "validation_time_lte:" in x, correct_outputs[0]["filter_by"]
            )
        ]
        assert len(filter_validation_lte) == 1
        assert int(filter_validation_lte[0][20:]) == convert_event_time_to_timestamp(
            correct_inputs[0]["end_validation_time"]
        )

        # Second case
        assert len(correct_outputs[1]["filter_by"]) == 2
        assert "ingestion_result_eq:UNKNOWN" in correct_outputs[1]["filter_by"]
        filter_validation_gte = [
            val
            for val in filter(
                lambda x: "validation_time_gte:" in x, correct_outputs[1]["filter_by"]
            )
        ]
        assert len(filter_validation_gte) == 1
        assert int(filter_validation_gte[0][20:]) == convert_event_time_to_timestamp(
            correct_inputs[1]["start_validation_time"]
        )
        filter_validation_lte = [
            val
            for val in filter(
                lambda x: "validation_time_lte:" in x, correct_outputs[1]["filter_by"]
            )
        ]
        assert len(filter_validation_lte) == 0

        # Third case
        assert len(correct_outputs[2]["filter_by"]) == 4
        assert "ingestion_result_eq:REJECTED" in correct_outputs[2]["filter_by"]
        assert "ingestion_result_eq:FG_DATA" in correct_outputs[2]["filter_by"]
        filter_validation_gte = [
            val
            for val in filter(
                lambda x: "validation_time_gte:" in x, correct_outputs[2]["filter_by"]
            )
        ]
        assert len(filter_validation_gte) == 1
        assert int(filter_validation_gte[0][20:]) == convert_event_time_to_timestamp(
            correct_inputs[2]["start_validation_time"]
        )
        filter_validation_lte = [
            val
            for val in filter(
                lambda x: "validation_time_lte:" in x, correct_outputs[2]["filter_by"]
            )
        ]
        assert len(filter_validation_lte) == 1
        assert int(filter_validation_lte[0][20:]) == convert_event_time_to_timestamp(
            correct_inputs[2]["end_validation_time"]
        )

        # Fourth case
        assert len(correct_outputs[3]["filter_by"]) == 0

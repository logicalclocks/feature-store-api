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


from hsfs import ge_validation_result, validation_report


class TestValidationReport:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get"]["response"]

        # Act
        vr = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert vr.id == 11
        assert vr.success is True
        assert vr._full_report_path == "test_full_report_path"
        assert vr._validation_time == "test_validation_time"
        assert vr._featurestore_id == "test_featurestore_id"
        assert vr._featuregroup_id == "test_featuregroup_id"
        assert vr.ingestion_result == "INGESTED"
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.statistics == {"statistics_key": "statistics_value"}
        assert vr.evaluation_parameters == {
            "evaluation_parameters_key": "evaluation_parameters_value"
        }

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get_basic_info"]["response"]

        # Act
        vr = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert vr.id is None
        assert vr.success is True
        assert vr._full_report_path is None
        assert vr._validation_time is None
        assert vr._featurestore_id is None
        assert vr._featuregroup_id is None
        assert vr.ingestion_result == "unknown"
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.statistics == {"statistics_key": "statistics_value"}
        assert vr.evaluation_parameters is None

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get_list"]["response"]

        # Act
        vr_list = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert len(vr_list) == 1
        vr = vr_list[0]
        assert vr.id == 11
        assert vr.success is True
        assert vr._full_report_path == "test_full_report_path"
        assert vr._validation_time == "test_validation_time"
        assert vr._featurestore_id == "test_featurestore_id"
        assert vr._featuregroup_id == "test_featuregroup_id"
        assert vr.ingestion_result == "test_ingestion_result"
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.statistics == {"statistics_key": "statistics_value"}
        assert vr.evaluation_parameters == {
            "evaluation_parameters_key": "evaluation_parameters_value"
        }

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get_list_empty"]["response"]

        # Act
        vr_list = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert len(vr_list) == 0

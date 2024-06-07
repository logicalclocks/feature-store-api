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
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import humps
from hsfs import util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.constants import great_expectations_not_installed_message
from hsfs.ge_validation_result import ValidationResult


if TYPE_CHECKING:
    import great_expectations


class ValidationReport:
    """Metadata object representing a validation report generated by Great Expectations in the Feature Store."""

    def __init__(
        self,
        success: bool,
        results: List[
            Union[
                ValidationResult,
                Dict[str, Any],
                great_expectations.core.expectation_validation_result.ExpectationValidationResult,
            ]
        ],
        meta: Optional[Union[Dict[str, Any], str]],
        statistics: Optional[Union[Dict[str, Any], str]],
        evaluation_parameters: Optional[Union[Dict[str, Any], str]] = None,
        id: Optional[int] = None,
        full_report_path: Optional[str] = None,
        featurestore_id: Optional[int] = None,
        featuregroup_id: Optional[int] = None,
        validation_time: Optional[str] = None,
        ingestion_result: Literal[
            "ingested", "rejected", "unknown", "experiment", "fg_data"
        ] = "unknown",
        **kwargs,
    ) -> None:
        self._id = id
        self._success = success
        self._full_report_path = full_report_path
        self._validation_time = validation_time
        self._featurestore_id = featurestore_id
        self._featuregroup_id = featuregroup_id
        self._ingestion_result = ingestion_result

        self.results = results
        self.meta = meta
        self.statistics = statistics
        self.evaluation_parameters = evaluation_parameters

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> Union[List[ValidationReport], ValidationReport]:
        json_decamelized = humps.decamelize(json_dict)
        if (
            "count" in json_decamelized
        ):  # todo it has count either way (if items or dict)
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**validation_report)
                for validation_report in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self) -> Dict[str, Union[int, str, bool]]:
        return {
            "id": self._id,
            "success": self.success,
            "evaluationParameters": json.dumps(self.evaluation_parameters),
            "statistics": json.dumps(self._statistics),
            "results": self._results,
            "meta": json.dumps(self._meta),
            "ingestionResult": self._ingestion_result.upper(),
        }

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            "id": self._id,
            "success": self.success,
            "evaluationParameters": self.evaluation_parameters,
            "statistics": self._statistics,
            "results": [result.to_json_dict() for result in self._results],
            "meta": self._meta,
        }

    def to_ge_type(self) -> great_expectations.core.ExpectationSuiteValidationResult:
        is_ge_installed = util.is_module_available(
            "great_expectations", raise_error=False
        )
        if not is_ge_installed:
            raise FeatureStoreException(great_expectations_not_installed_message)
        return great_expectations.core.ExpectationSuiteValidationResult(
            success=self.success,
            statistics=self.statistics,
            results=[result.to_ge_type() for result in self.results],
            evaluation_parameters=self.evaluation_parameters,
            meta=self.meta,
        )

    @property
    def id(self) -> Optional[int]:
        """Id of the validation report, set by backend."""
        return self._id

    @id.setter
    def id(self, id: Optional[int]) -> None:
        self._id = id

    @property
    def success(self) -> bool:
        """Overall success of the validation step"""
        return self._success

    @success.setter
    def success(self, success: bool) -> None:
        self._success = success

    @property
    def results(self) -> List[ValidationResult]:
        """List of expectation results obtained after validation."""
        return self._results

    @results.setter
    def results(
        self,
        results: List[
            Union[
                ValidationResult,
                Dict[str, Any],
                great_expectations.core.expectation_validation_result.ExpectationValidationResult,
            ]
        ],
    ) -> None:
        if len(results) == 0:
            self._results = []
        elif isinstance(results[0], ValidationResult):
            self._results = results
        elif isinstance(results[0], dict):
            self._results = [ValidationResult(**result) for result in results]
        elif isinstance(
            results[0],
            great_expectations.core.expectation_validation_result.ExpectationValidationResult,
        ):
            self._results = [
                ValidationResult(**result.to_json_dict()) for result in results
            ]

    @property
    def meta(self) -> Optional[Dict[str, Any]]:
        """Meta field of the validation report to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta: Optional[Union[Dict[str, Any], str]]) -> None:
        if meta is None:
            self._meta = None
        elif isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")

    @property
    def statistics(self) -> Optional[Dict[str, Any]]:
        """Statistics field of the validation report which store overall statistics
        about the validation result, e.g number of failing/successful expectations."""
        return self._statistics

    @statistics.setter
    def statistics(self, statistics: Optional[Union[str, Dict[str, Any]]]) -> None:
        if statistics is None:
            self._statistics = None
        elif isinstance(statistics, dict):
            self._statistics = statistics
        elif isinstance(statistics, str):
            self._statistics = json.loads(statistics)
        else:
            raise ValueError("Statistics field must be stringified json or dict")

    @property
    def evaluation_parameters(self) -> Optional[Dict[str, Any]]:
        """Evaluation parameters field of the validation report which store kwargs of the validation."""
        return self._evaluation_parameters

    @evaluation_parameters.setter
    def evaluation_parameters(
        self, evaluation_parameters: Optional[Union[Dict[str, Any], str]]
    ) -> None:
        if evaluation_parameters is None:
            self._evaluation_parameters = None
        elif isinstance(evaluation_parameters, dict):
            self._evaluation_parameters = evaluation_parameters
        elif isinstance(evaluation_parameters, str):
            self._evaluation_parameters = json.loads(evaluation_parameters)
        else:
            raise ValueError(
                "Evaluation parameters field must be stringified json or dict"
            )

    @property
    def ingestion_result(self) -> str:
        """Overall success of the validation run together with the ingestion
        validation policy. Indicating if dataframe was ingested or rejected."""
        return self._ingestion_result

    @ingestion_result.setter
    def ingestion_result(
        self,
        ingestion_result: Optional[
            Literal["ingested", "rejected", "experiment", "unknown", "fg_data"]
        ],
    ) -> None:
        if ingestion_result is None:
            ingestion_result = "UNKNOWN"
        allowed_values = ["ingested", "rejected", "experiment", "unknown", "fg_data"]
        if ingestion_result.lower() in allowed_values:
            self._ingestion_result = ingestion_result
        else:
            raise ValueError(
                f"Invalid Value {ingestion_result} for ingestion_result."
                + f"Allowed values are {', '.join(allowed_values)}."
            )

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return (
            f"ValidationReport(success: {self._success}, "
            + f"{self._statistics}, {len(self._results)} results"
            + f" , {self._meta}, {self._full_report_path})"
        )

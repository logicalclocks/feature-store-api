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

import json

import humps
import great_expectations as ge

from hsfs import util
from hsfs.ge_validation_result import ValidationResult


class ValidationReport:
    """Metadata object representing a validation report generated by Great Expectations in the Feature Store."""

    def __init__(
        self,
        success,
        results,
        meta,
        statistics,
        evaluation_parameters=None,
        id=None,
        full_report_path=None,
        featurestore_id=None,
        featuregroup_id=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        validation_time=None,
        ingestion_result=None,
    ):
        self._id = id
        self._success = success
        self._full_report_path = full_report_path
        self._validation_time = validation_time
        self._featurestore_id = featurestore_id
        self._featuregroup_id = featuregroup_id
        self._ingestion_result = None

        self.results = results
        self.meta = meta
        self.statistics = statistics
        self.evaluation_parameters = evaluation_parameters

    # TODO MORITZ this shouldn't be here
    # def save(self):
    #    """Persist the expectation metadata object to the feature store."""
    #    validation_report_engine.ValidationReportEngine(self._featurestore_id, self._featuregroup_id).save(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**validation_report)
                for validation_report in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "success": self.success,
            "evaluationParameters": json.dumps(self.evaluation_parameters),
            "statistics": json.dumps(self._statistics),
            "results": self._results,
            "meta": json.dumps(self._meta),
        }

    def to_json_dict(self):
        return {
            "id": self._id,
            "success": self.success,
            "evaluationParameters": self.evaluation_parameters,
            "statistics": self._statistics,
            "results": [result.to_json_dict() for result in self._results],
            "meta": self._meta,
        }

    def to_ge_type(self):
        return ge.core.ExpectationSuiteValidationResult(
            success=self.success,
            statistics=self.statistics,
            results=[result.to_ge_type() for result in self.results],
            evaluation_parameters=self.evaluation_parameters,
            meta=self.meta,
        )

    @property
    def id(self):
        """Id of the validation report, set by backend."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def success(self):
        """Overall success of the validation step"""
        return self._success

    @success.setter
    def success(self, success):
        self._success = success

    @property
    def results(self):
        """List of expectation results obtained after validation."""
        return self._results

    @results.setter
    def results(self, results):
        if len(results) == 0:
            self._results = []
        elif isinstance(results[0], ValidationResult):
            self._results = results
        elif isinstance(results[0], dict):
            self._results = [ValidationResult(**result) for result in results]
        elif isinstance(
            results[0],
            ge.core.expectation_validation_result.ExpectationValidationResult,
        ):
            self._results = [
                ValidationResult(**result.to_json_dict()) for result in results
            ]

    @property
    def meta(self):
        """Meta field of the validation report to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta):
        if meta is None:
            self._meta = None
        elif isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")

    @property
    def statistics(self):
        """Statistics field of the validation report which store overall statistics
        about the validation result, e.g number of failing/successful expectations."""
        return self._statistics

    @statistics.setter
    def statistics(self, statistics):
        if statistics is None:
            self._statistics = None
        elif isinstance(statistics, dict):
            self._statistics = statistics
        elif isinstance(statistics, str):
            self._statistics = json.loads(statistics)
        else:
            raise ValueError("Statistics field must be stringified json or dict")

    @property
    def evaluation_parameters(self):
        """Evaluation parameters field of the validation report which store kwargs of the validation."""
        return self._evaluation_parameters

    @evaluation_parameters.setter
    def evaluation_parameters(self, evaluation_parameters):
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

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"ValidationReport(success: {self._success}, "
            + f"{self._statistics}, {len(self._results)} results"
            + f" , {self._meta}, {self._full_report_path_string})"
        )

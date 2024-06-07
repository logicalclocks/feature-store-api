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

import datetime
import json
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Union

import dateutil
import humps
from hsfs import util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.constants import great_expectations_not_installed_message


if TYPE_CHECKING:
    import great_expectations


class ValidationResult:
    """Metadata object representing a validation result generated by Great Expectations in the Feature Store."""

    def __init__(
        self,
        success: bool,
        result: Dict[str, Any],
        expectation_config: str,
        exception_info: Dict[str, Any],
        meta: Optional[Dict[str, Any]] = None,
        id: Optional[int] = None,
        observed_value: Optional[Any] = None,
        expectation_id: Optional[int] = None,
        validation_report_id: Optional[int] = None,
        validation_time: Optional[int] = None,
        ingestion_result: Literal[
            "unknown", "ingested", "rejected", "fg_data", "experiment"
        ] = "UNKNOWN",
        **kwargs,
    ):
        self._id = id
        self._success = success
        self._observed_value = observed_value
        self._expectation_id = expectation_id
        self._validation_report_id = validation_report_id

        self.result = result
        self.meta = meta
        self.exception_info = exception_info
        self.expectation_config = expectation_config

        self.validation_time = validation_time
        self.ingestion_result = ingestion_result

        if (observed_value is None) and ("observed_value" in self.result.keys()):
            self._observed_value = self.result["observed_value"]

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**validation_result)
                for validation_result in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "success": self.success,
            "exceptionInfo": json.dumps(self._exception_info),
            "expectationConfig": json.dumps(self._expectation_config),
            "result": json.dumps(self._result),
            "meta": json.dumps(self._meta),
        }

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            "id": self._id,
            "success": self.success,
            "exceptionInfo": self._exception_info,
            "expectationConfig": self._expectation_config,
            "result": self._result,
            "meta": self._meta,
        }

    def to_ge_type(self) -> great_expectations.core.ExpectationValidationResult:
        is_ge_installed = util.is_package_installed_or_load("great_expectations")
        if not is_ge_installed:
            raise FeatureStoreException(great_expectations_not_installed_message)
        return great_expectations.core.ExpectationValidationResult(
            success=self.success,
            exception_info=self.exception_info,
            expectation_config=self.expectation_config,
            result=self.result,
            meta=self.meta,
        )

    @property
    def id(self) -> Optional[int]:
        """Id of the validation report, set by backend."""
        return self._id

    @id.setter
    def id(self, id: Optional[int] = None) -> None:
        self._id = id

    @property
    def success(self) -> bool:
        """Overall success of the validation step."""
        return self._success

    @success.setter
    def success(self, success: bool) -> None:
        self._success = success

    @property
    def result(self) -> Dict[str, Any]:
        """Result of the expectation after validation."""
        return self._result

    @result.setter
    def result(self, result: Dict[str, Any]) -> None:
        if isinstance(result, dict):
            self._result = result
        elif isinstance(result, str):
            self._result = json.loads(result)
        else:
            raise ValueError("Result field must be stringified json or dict.")

    @property
    def meta(self) -> Dict[str, Any]:
        """Meta field of the validation report to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta: Dict[str, Any] = None):
        if meta is None:
            self._meta = {}
        if isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict")

    @property
    def exception_info(self) -> Dict[str, Any]:
        """Exception info which can be raised when running validation."""
        return self._exception_info

    @exception_info.setter
    def exception_info(self, exception_info: Dict[str, Any]) -> None:
        if isinstance(exception_info, dict):
            self._exception_info = exception_info
        elif isinstance(exception_info, str):
            self._exception_info = json.loads(exception_info)
        else:
            raise ValueError("Exception info field must be stringified json or dict.")

    @property
    def expectation_config(self) -> Dict[str, Any]:
        """Expectation configuration used when running validation."""
        return self._expectation_config

    @expectation_config.setter
    def expectation_config(self, expectation_config: Dict[str, Any]) -> None:
        if isinstance(expectation_config, dict):
            self._expectation_config = expectation_config
        elif isinstance(expectation_config, str):
            self._expectation_config = json.loads(expectation_config)
        else:
            raise ValueError(
                "Expectation config field must be stringified json or dict"
            )

    @property
    def validation_time(self) -> Optional[int]:
        return self._validation_time

    @validation_time.setter
    def validation_time(
        self, validation_time: Union[str, int, datetime.datetime, datetime.date, None]
    ) -> None:
        """
        Time at which validation was run using Great Expectations.

        # Arguments
            validation_time: The time at which validation was performed.
            Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable.
        """
        if isinstance(validation_time, str):
            try:
                # returns a datemtime to be converted to timestamp below
                validation_time = dateutil.parser.parse(validation_time).astimezone(
                    datetime.timezone.utc
                )
            except ValueError:
                pass
        # use the same function as the rest of the client to deal with conversion to timestamps
        # from various types
        if validation_time:
            self._validation_time = util.convert_event_time_to_timestamp(
                validation_time
            )
        else:
            self._validation_time = None

    @property
    def ingestion_result(
        self,
    ) -> Literal["ingested", "rejected", "unknown", "experiment", "fg_data"]:
        return self._ingestion_result

    @ingestion_result.setter
    def ingestion_result(
        self,
        ingestion_result: Literal[
            "ingested", "rejected", "unknown", "experiment", "fg_data"
        ] = "unknown",
    ):
        allowed_values = ["ingested", "rejected", "unknown", "experiment", "fg_data"]
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
        result_string = ""
        if self._result is None and self._observed_value is not None:
            result_string += f"observed_value : {self._observed_value}"
        elif self._result is not None and self._observed_value is None:
            result_string += f"result : {self._result}"

        return (
            f"ValidationResult(success: {self._success},"
            + result_string
            + f"{self._exception_info}, {self._expectation_config}, {self._meta})"
        )

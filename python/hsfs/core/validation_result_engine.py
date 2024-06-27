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
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Dict, List, Union


if TYPE_CHECKING:
    import great_expectations


from hsfs import util
from hsfs.core import validation_result_api
from hsfs.ge_validation_result import ValidationResult


class ValidationResultEngine:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Validation Result engine.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the featuregroup it is attached to
        :type feature_group_id: int
        """
        self._validation_result_api = validation_result_api.ValidationResultApi(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

    def get_validation_history(
        self,
        expectation_id: int,
        start_validation_time: Union[str, int, datetime, date, None] = None,
        end_validation_time: Union[str, int, datetime, date, None] = None,
        filter_by: List[str] = None,
        ge_type: bool = True,
    ) -> Union[
        List[ValidationResult],
        List[great_expectations.core.ExpectationValidationResult],
    ]:
        """Get Validation Results relevant to an Expectation specified by expectation_id.

        :param expectation_id: id of the expectation for which to fetch the validation history
        :type expectation_id: int
        :param ingestion_only: retrieve only validation result linked to data ingested in the Feature Group
        :type ingestion_only: bool
        :param rejected_only: retrieve only validation result linked to data not ingested in the Feature Group
        :type rejected_only: bool
        :param ge_type: whether to convert Hopsworks object to native Great Expectations object
        :type ge_type: bool
        :param start_validation_time: retrieve validation result posterior to start_validation_time.
        Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable.
        :type start_validation_time: Union[str, int, datetime, date, None]
        :param end_validation_time: retrieve validation result anterior to end_validation_time
        Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable.
        :type end_validation_time: Union[str, int, datetime, date, None]
        """
        query_params = self._build_query_params(
            filter_by=filter_by or [],
            start_validation_time=start_validation_time,
            end_validation_time=end_validation_time,
        )

        history = self._validation_result_api.get_validation_history(
            expectation_id=expectation_id, query_params=query_params
        )

        if isinstance(history, ValidationResult):
            history = [history]

        if ge_type:
            return [result.to_ge_type() for result in history]
        else:
            return history

    def _build_query_params(
        self,
        filter_by: List[str] = None,
        start_validation_time: Union[str, int, datetime, date, None] = None,
        end_validation_time: Union[str, int, datetime, date, None] = None,
    ) -> Dict[str, str]:
        query_params = {"filter_by": [], "sort_by": "validation_time:desc"}
        allowed_ingestion_filters = [
            "INGESTED",
            "REJECTED",
            "UNKNOWN",
            "EXPERIMENT",
            "FG_DATA",
        ]
        if isinstance(filter_by, list):
            ingestion_filters = []
            for ingestion_filter in filter_by:
                if ingestion_filter.upper() in allowed_ingestion_filters:
                    ingestion_filters.append(
                        f"ingestion_result_eq:{ingestion_filter.upper()}"
                    )
                else:
                    raise ValueError(
                        f"Illegal Value {ingestion_filter} in filter_by."
                        + f"Allowed values are {', '.join(allowed_ingestion_filters)}"
                    )

        query_params["filter_by"].extend(ingestion_filters)

        if start_validation_time and end_validation_time:
            if util.convert_event_time_to_timestamp(
                start_validation_time
            ) > util.convert_event_time_to_timestamp(end_validation_time):
                raise ValueError(
                    f"start_validation_time : {start_validation_time} is posterior to end_validation_time : {end_validation_time}"
                )

        if start_validation_time:
            query_params["filter_by"].append(
                "validation_time_gte:"
                + str(util.convert_event_time_to_timestamp(start_validation_time))
            )
        if end_validation_time:
            query_params["filter_by"].append(
                "validation_time_lte:"
                + str(util.convert_event_time_to_timestamp(end_validation_time))
            )

        return query_params

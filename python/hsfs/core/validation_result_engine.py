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

from typing import Union, List, Optional
from hsfs.core import validation_result_api
import re

from hsfs.ge_validation_result import ValidationResult


class ValidationResultEngine:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Validation Result engine.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the featuregroup it is attached to
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._validation_result_api = validation_result_api.ValidationResultApi(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

    def get_validation_history(
        self,
        expectation_id: int,
        sort_by: Optional[str] = "validation_time:desc",
        filter_by: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Union[List[ValidationResult], ValidationResult]:
        """Get Validation Results relevant to an Expectation specified by expectation_id.

        :param expectation_id: id of the expectation for which to fetch the validation history
        :type expectation_id: int
        :param sort_by: sort the validation results according to validation_time, descending or ascending
        :type sort_by: Optional[str]
        :param filter_by: filter the validation results based on validation_time_(eq,gt,gte,lt,lte) or ingestion result
        :type filter_by: Optional[str]
        :param offset: offset for pagination
        :type offset: Optional[int]
        :param limit: limit for pagination
        :type limit: int
        """
        if sort_by:
            self._verify_sort_by(sort_by)
        if filter_by:
            self._verify_filter_by(filter_by)
        if offset:
            self._verify_offset(offset)
        if limit:
            self._verify_limit(limit)

        return self._validation_result_api.get_validation_history(
            expectation_id=expectation_id,
            offset=offset,
            limit=limit,
            filter_by=filter_by,
            sort_by=sort_by,
        )

    def _verify_sort_by(self, sort_by: str) -> None:
        if isinstance(sort_by, str):
            if sort_by.lower() not in ["validation_time:asc", "validation_time:desc"]:
                raise ValueError(
                    f"Illegal Value for sort_by : {sort_by}. Allowed values are validation_time:desc or validation_time:asc."
                )
        else:
            raise TypeError(f"sort_by must be a str. Got {type(sort_by)}")

    def _verify_filter_by(self, filter_by: Union[str, List[str], None]) -> None:
        if isinstance(filter_by, str):
            self._match_filter_by_validation_time_or_ingestion_result(
                filter_by=filter_by
            )
            return
        elif isinstance(filter_by, List):
            if len(filter_by) == 0:
                filter_by = None
                return
            for single_filter in filter_by:
                self._verify_filter_by(single_filter)
            return

        raise TypeError(
            f"filter_by must be a str or list of string. Got {type(filter_by)}"
        )

    def _match_filter_by_validation_time_or_ingestion_result(
        self, filter_by: str
    ) -> None:
        if filter_by.upper() in [
            "INGESTION_RESULT_EQ:REJECTED",
            "INGESTION_RESULT_EQ:INGESTED",
        ]:
            return

        match = re.search("([a-z,_]+):([0-9]+)$", filter_by.lower())
        allowed_filters = [
            "validation_time_gt",
            "validation_time_gte",
            "validation_time_eq",
            "validation_time_lt",
            "validation_time_lte",
        ]

        if match:
            groups = match.groups(0)
            if groups[0] in allowed_filters:
                if int(groups[1], base=10) > 0:
                    print(groups)
                    return

        raise ValueError(
            f"Illegal Value for filter_by : {filter_by}."
            + "Allowed values are validation_time_[gt/gte/eq/lt/lte]:(datetime.datetime) or ingestion_result_eq:[ingested/rejected]"
        )

    def _verify_offset(self, offset: int) -> None:
        if isinstance(offset, int):
            if offset >= 0:
                return
            else:
                raise ValueError(
                    f"offset value should be a positive integer, got {offset}"
                )
        else:
            raise TypeError(f"offset should be of type int, got {type(offset)}")

    def _verify_limit(self, limit: int) -> None:
        if isinstance(limit, int):
            if limit >= 0:
                return
            else:
                raise ValueError(
                    f"offset value should be a positive integer, got {limit}"
                )
        else:
            raise TypeError(f"offset should be of type int, got {type(limit)}")

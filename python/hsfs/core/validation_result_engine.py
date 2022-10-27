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
import pandas as pd
import dateutil

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
        as_timeserie: bool,
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

        validation_history = self._validation_result_api.get_validation_history(
            expectation_id=expectation_id,
            offset=offset,
            limit=limit,
            filter_by=filter_by,
            sort_by=sort_by,
        )

        if as_timeserie:
            return self.convert_history_to_timeserie(validation_history)
        else:
            return validation_history

    def convert_history_to_timeserie(
        self, validation_history: List[ValidationResult]
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "observed_value": [
                    result._observed_value for result in validation_history
                ],
                "validation_time": [
                    dateutil.parser.parseiso(result.validation_time)
                    for result in validation_history
                ],
            }
        )

    def _verify_sort_by(self, sort_by: str) -> None:
        assert type(sort_by) is str

    def _verify_filter_by(self, filter_by: str) -> None:
        assert type(filter_by) is str

    def _verify_offset(self, offset: int) -> None:
        assert type(offset) is int
        assert offset >= 0

    def _verify_limit(self, limit: int) -> None:
        assert type(limit) is int
        assert limit > 0

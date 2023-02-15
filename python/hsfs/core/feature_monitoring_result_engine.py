#
#   Copyright 2023 Hopsworks AB
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

from typing import Any, Dict, List, Optional, Union
from datetime import date, datetime
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.feature_monitoring_result_api import FeatureMonitoringResultApi


class FeatureMonitoringResultEngine:
    def __init__(self, feature_store_id: Optional[int]):
        self._feature_store_id = feature_store_id
        self._feature_monitoring_result_api = FeatureMonitoringResultApi(
            feature_store_id=feature_store_id
        )

    def save_feature_monitoring_result(
        self,
        feature_monitoring_config_id: int,
        job_id: int,
        execution_id: int,
        detection_stats_id: int,
        triggered_alert: bool = False,
        difference: Optional[float] = None,
        reference_stats_id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> FeatureMonitoringResult:

        result = FeatureMonitoringResult(
            feature_store_id=self._feature_store_id,
            feature_monitoring_config_id=feature_monitoring_config_id,
            execution_id=execution_id,
            job_id=job_id,
            detection_stats_id=detection_stats_id,
            reference_stats_id=reference_stats_id,
            difference=difference,
            triggered_alert=triggered_alert,
            monitoring_time=int(round(datetime.now().timestamp())),
            entity_id=feature_group_id
            if feature_group_id is not None
            else feature_view_id,
        )

        return self._feature_monitoring_result_api.create(
            result,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def fetch_all_feature_monitoring_results_by_config_id(
        self,
        config_id: int,
        feature_group_id: Optional[int],
        feature_view_name: Optional[str],
        feature_view_version: Optional[int],
        start_time: Union[str, int, datetime, date, None],
        end_time: Union[str, int, datetime, date, None],
    ) -> List[FeatureMonitoringResult]:

        query_params = self.build_query_params(
            start_time=start_time,
            end_time=end_time,
        )

        return self._feature_monitoring_result_api.get_all(
            config_id=config_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            query_params=query_params,
        )

    def build_query_params(
        self,
        start_time: Union[str, int, datetime, date, None],
        end_time: Union[str, int, datetime, date, None],
    ) -> Dict[str, Any]:

        return {
            "filter_by": ["monitoring_time_gte:start_time"],
            "order_by": "monitoring_time:desc",
        }

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

from typing import Any, Dict, Hashable, List, Optional, Union
from datetime import date, datetime
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.feature_monitoring_result_api import FeatureMonitoringResultApi
from hsfs.core.feature_monitoring_config import FeatureMonitoringConfig
from hsfs import util

DEFAULT_EXECUTION_ID = 123


class FeatureMonitoringResultEngine:
    """Logic and helper methods to deal with results from a feature monitoring job.

    Attributes:
        feature_store_id: int. Id of the respective Feature Store.
        feature_group_id: int. Id of the feature group, if monitoring a feature group.
        feature_view_id: int. Id of the feature view, if monitoring a feature view.
        feature_view_name: str. Name of the feature view, if monitoring a feature view.
        feature_view_version: int. Version of the feature view, if monitoring a feature view.
    """

    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ):

        if feature_group_id is None:
            assert feature_view_id is not None
            assert feature_view_name is not None
            assert feature_view_version is not None

        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_id = feature_view_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        self._feature_monitoring_result_api = FeatureMonitoringResultApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def save_feature_monitoring_result(
        self,
        config_id: int,
        execution_id: int,
        detection_stats_id: int,
        shift_detected: bool = False,
        difference: Optional[float] = None,
        reference_stats_id: Optional[int] = None,
    ) -> FeatureMonitoringResult:
        """Save feature monitoring result.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            execution_id: int. Id of the job execution.
            detection_stats_id: int. Id of the detection statistics.
            shift_detected: bool. Whether a shift is detected between the detection and reference window.
                It is used to decide whether to trigger an alert.
            difference: Optional[float]. Difference between detection statistics and reference statistics.
                Defaults to zero if no reference is provided.
            reference_stats_id: Optional[int]. Id of the reference statistics.
                Defaults to None if no reference is provided.

        Returns:
            FeatureMonitoringResult. Saved Feature monitoring result.
        """
        monitoring_time = round(
            util.convert_event_time_to_timestamp(datetime.now()), -3
        )

        result = FeatureMonitoringResult(
            feature_store_id=self._feature_store_id,
            config_id=config_id,
            execution_id=execution_id,
            detection_stats_id=detection_stats_id,
            reference_stats_id=reference_stats_id,
            difference=difference,
            shift_detected=shift_detected,
            monitoring_time=monitoring_time,
        )

        return self._feature_monitoring_result_api.create(
            result,
        )

    def fetch_all_feature_monitoring_results_by_config_id(
        self,
        config_id: int,
        start_time: Union[str, int, datetime, date, None] = None,
        end_time: Union[str, int, datetime, date, None] = None,
    ) -> List[FeatureMonitoringResult]:
        """Fetch all feature monitoring results by config id.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.

        Returns:
            List[FeatureMonitoringResult]. List of feature monitoring results.
        """

        query_params = self.build_query_params(
            start_time=start_time,
            end_time=end_time,
        )

        return self._feature_monitoring_result_api.get_by_config_id(
            config_id=config_id,
            query_params=query_params,
        )

    def build_query_params(
        self,
        start_time: Union[str, int, datetime, date, None],
        end_time: Union[str, int, datetime, date, None],
    ) -> Dict[str, str]:
        """Build query parameters for feature monitoring result API calls.

        Args:
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.

        Returns:
            Dict[str, str]. Query parameters.
        """
        filter_by = []

        if start_time:
            timestamp_start_time = util.convert_event_time_to_timestamp(start_time)
            filter_by.append(f"monitoring_time_gte:{timestamp_start_time}")
        if end_time:
            timestamp_end_time = util.convert_event_time_to_timestamp(end_time)
            filter_by.append(f"monitoring_time_lte:{timestamp_end_time}")

        return {
            "filter_by": filter_by,
            "sort_by": "monitoring_time:desc",
        }

    def run_and_save_statistics_comparison(
        self,
        detection_stats_id: int,
        detection_stats: Dict[Hashable, Any],
        fm_config: FeatureMonitoringConfig,
        reference_stats_id: Optional[int] = None,
        reference_stats: Optional[Dict[Hashable, Any]] = None,
    ) -> FeatureMonitoringResult:
        """Run and upload statistics comparison between detection and reference stats.

        Args:
            detection_stats_id: int. Id of the detection statistics.
            detection_stats: Computed statistics for detection data.
            fm_config: FeatureMonitoringConfig. Feature monitoring configuration.
            reference_stats_id: int. Id of the reference statistics.
            reference_stats: Computed statistics for reference data.

        Returns:
            FeatureMonitoringResult. Feature monitoring result.
        """

        if reference_stats:
            difference = self.compute_difference(
                detection_stats=detection_stats,
                reference_stats=reference_stats,
                metric=fm_config.statistics_comparison_config["compare_on"].lower(),
                relative=fm_config.statistics_comparison_config["relative"],
            )

            if fm_config.statistics_comparison_config["strict"]:
                shift_detected = (
                    True
                    if difference >= fm_config.statistics_comparison_config["threshold"]
                    else False
                )
            else:
                shift_detected = (
                    True
                    if difference > fm_config.statistics_comparison_config["threshold"]
                    else False
                )
        else:
            difference = 0
            shift_detected = False

        return self.save_feature_monitoring_result(
            config_id=fm_config.id,
            execution_id=DEFAULT_EXECUTION_ID,
            detection_stats_id=detection_stats_id,
            reference_stats_id=reference_stats_id,
            difference=difference,
            shift_detected=shift_detected,
        )

    def compute_difference(
        self,
        detection_stats: Dict[Hashable, Any],
        reference_stats: Dict[Hashable, Any],
        metric: str,
        relative: bool = False,
    ) -> float:
        """Compute the difference between the reference and detection statistics.

        Args:
            detection_stats: `Dict[Hashable, Any]`. The statistics computed from detection data.
            reference_stats: `Dict[Hashable, Any]`. The statistics computed from reference data.
            metric: `str`. The metric to compute the difference for.
            relative: `bool`. Whether to compute the relative difference or not.

        Returns:
            `float`. The difference between the reference and detection statistics.
        """
        if reference_stats.get("specific_value", None) is not None:
            reference_stats[metric] = reference_stats["specific_value"]

        if relative:
            if (reference_stats[metric] + detection_stats[metric]) == 0:
                return float("inf")
            else:
                return (detection_stats[metric] - reference_stats[metric]) / (
                    reference_stats[metric] + detection_stats[metric]
                )
        else:
            return detection_stats[metric] - reference_stats[metric]

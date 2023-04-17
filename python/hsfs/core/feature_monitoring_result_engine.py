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

from typing import Dict, List, Optional, Union, Tuple
from datetime import date, datetime
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.feature_monitoring_result_api import FeatureMonitoringResultApi
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs import util
from hsfs.core.job_api import JobApi


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
        self._job_api = JobApi()

    def save_feature_monitoring_result(
        self,
        config_id: int,
        execution_id: int,
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: Optional[FeatureDescriptiveStatistics] = None,
        shift_detected: bool = False,
        difference: Optional[float] = None,
    ) -> FeatureMonitoringResult:
        """Save feature monitoring result.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            execution_id: int. Id of the job execution.
            detection_statistics: FeatureDescriptiveStatistics. Statistics computed from the detection data.
            reference_statistics: Optional[FeatureDescriptiveStatistics]. Statistics computed from the reference data.
                Defaults to None if no reference is provided.
            shift_detected: bool. Whether a shift is detected between the detection and reference window.
                It is used to decide whether to trigger an alert.
            difference: Optional[float]. Difference between detection statistics and reference statistics.
                Defaults to zero if no reference is provided.

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
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
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
        with_statistics: bool = False,
    ) -> List[FeatureMonitoringResult]:
        """Fetch all feature monitoring results by config id.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.
            with_statistics: bool.
                Whether to include the statistics attached to the results or not

        Returns:
            List[FeatureMonitoringResult]. List of feature monitoring results.
        """

        query_params = self._build_query_params(
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

        return self._feature_monitoring_result_api.get_by_config_id(
            config_id=config_id,
            query_params=query_params,
        )

    def _build_query_params(
        self,
        start_time: Union[str, int, datetime, date, None],
        end_time: Union[str, int, datetime, date, None],
        with_statistics: bool,
    ) -> Dict[str, str]:
        """Build query parameters for feature monitoring result API calls.

        Args:
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.
            with_statistics: bool.
                Whether to include the statistics attached to the results or not

        Returns:
            Dict[str, str]. Query parameters.
        """

        query_params = {"sort_by": "monitoring_time:desc"}

        filter_by = []
        if start_time:
            timestamp_start_time = util.convert_event_time_to_timestamp(start_time)
            filter_by.append(f"monitoring_time_gte:{timestamp_start_time}")
        if end_time:
            timestamp_end_time = util.convert_event_time_to_timestamp(end_time)
            filter_by.append(f"monitoring_time_lte:{timestamp_end_time}")
        if len(filter_by) > 0:
            query_params["filter_by"] = filter_by

        if with_statistics:
            query_params["expand"] = "statistics"

        return query_params

    def run_and_save_statistics_comparison(
        self,
        fm_config: "fmc.FeatureMonitoringConfig",
        detection_statistics: List[FeatureDescriptiveStatistics],
        reference_statistics: Optional[
            Union[FeatureDescriptiveStatistics, int, float]
        ] = None,
    ) -> Union[List[FeatureMonitoringResult], FeatureMonitoringResult]:
        """Run and upload statistics comparison between detection and reference stats.

        Args:
            fm_config: FeatureMonitoringConfig. Feature monitoring configuration.
            detection_statistics: List[FeatureDescriptiveStatistics]. Computed statistics from detection data.
            reference_statistics: Optional[Union[FeatureDescriptiveStatistics, int, float]].
                Computed statistics from reference data, or a specific value to use as reference.

        Returns:
            FeatureMonitoringResult. Feature monitoring result.
        """
        # TODO: Cleanup, deduplicate code and make a consistent return type
        if reference_statistics is not None:
            difference, shift_detected = self._compute_difference_and_shift(
                fm_config,
                detection_statistics[0],  # comparison supports only single feature
                reference_statistics,
            )
            if not isinstance(reference_statistics, FeatureDescriptiveStatistics):
                # if specific value, don't save it with the fm result
                reference_statistics = None
        else:
            difference = None
            shift_detected = False

        execution = self._job_api.last_execution(self._job_api.get(fm_config.job_name))
        if execution is not None:
            execution_id = execution._id
        else:
            execution_id = 0

        if len(detection_statistics) > 1:
            return [
                self.save_feature_monitoring_result(
                    config_id=fm_config.id,
                    execution_id=execution_id,
                    detection_statistics=stats_entity,
                )
                for stats_entity in detection_statistics
            ]
        else:
            return self.save_feature_monitoring_result(
                config_id=fm_config.id,
                execution_id=execution_id,
                detection_statistics=detection_statistics[0],
                reference_statistics=reference_statistics,
                difference=difference,
                shift_detected=shift_detected,
            )

    def _compute_difference_and_shift(
        self,
        fm_config: "fmc.FeatureMonitoringConfig",
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: Union[FeatureDescriptiveStatistics, int, float],
    ) -> Tuple[float, bool]:
        """Compute the difference and detect shift between the reference and detection statistics.

        Args:
            fm_config: FeatureMonitoringConfig. Feature monitoring configuration.
            detection_statistics: FeatureDescriptiveStatistics. Computed statistics from detection data.
            reference_statistics: Union[FeatureDescriptiveStatistics, int, float].
                Computed statistics from reference data, or a specific value to use as reference.

        Returns:
            `(float, bool)`. The difference between the reference and detection statistics,
                             and whether shift was detected or not
        """

        difference = self._compute_difference_between_stats(
            detection_statistics,
            reference_statistics,
            metric=fm_config.statistics_comparison_config["metric"].lower(),
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
        return difference, shift_detected

    def _compute_difference_between_stats(
        self,
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: Union[FeatureDescriptiveStatistics, int, float],
        metric: str,
        relative: bool = False,
    ) -> float:
        """Compute the difference between the reference and detection statistics.

        Args:
            detection_statistics: FeatureDescriptiveStatistics. Computed statistics from detection data.
            reference_statistics: Union[FeatureDescriptiveStatistics, int, float].
                Computed statistics from reference data, or a specific value to use as reference
            metric: `str`. The metric to compute the difference for.
            relative: `bool`. Whether to compute the relative difference or not.

        Returns:
            `float`. The difference between the reference and detection statistics.
        """

        detection_value = detection_statistics.get_value(metric)
        reference_value = (
            reference_statistics
            if isinstance(reference_statistics, (int, float))
            else reference_statistics.get_value(metric)
        )
        return self._compute_difference_between_specific_values(
            detection_value, reference_value, relative
        )

    def _compute_difference_between_specific_values(
        self,
        detection_value: Union[int, float],
        reference_value: Union[int, float],
        relative: bool = False,
    ) -> float:
        """Compute the difference between a reference and detection value.

        Args:
            detection_value: Union[int, float]. The detection value.
            reference_value: Union[int, float]. The reference value
            relative: `bool`. Whether to compute the relative difference or not.

        Returns:
            `float`. The difference between the reference and detection values.
        """

        if relative:
            if reference_value == 0:
                return float("inf")
            else:
                return (detection_value - reference_value) / reference_value
        else:
            return detection_value - reference_value

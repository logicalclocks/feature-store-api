#
#   Copyright 2024 Hopsworks AB
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
from hsfs.core.feature_monitoring_config_api import FeatureMonitoringConfigApi
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.feature_monitoring_result_api import FeatureMonitoringResultApi
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.client.exceptions import FeatureStoreException
from hsfs import util
from hsfs.core.job_api import JobApi


class FeatureMonitoringResultEngine:
    """Logic and helper methods to deal with results from a feature monitoring job.

    Attributes:
        feature_store_id: int. Id of the respective Feature Store.
        feature_group_id: int. Id of the feature group, if monitoring a feature group.
        feature_view_name: str. Name of the feature view, if monitoring a feature view.
        feature_view_version: int. Version of the feature view, if monitoring a feature view.
    """

    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
        **kwargs,
    ):
        if feature_group_id is None:
            assert feature_view_name is not None
            assert feature_view_version is not None

        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        self._feature_monitoring_result_api = FeatureMonitoringResultApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )
        self._feature_monitoring_config_api = FeatureMonitoringConfigApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )
        self._job_api = JobApi()

    def build_feature_monitoring_result(
        self,
        config_id: int,
        feature_name: str,
        detection_statistics: Optional[FeatureDescriptiveStatistics] = None,
        reference_statistics: Optional[FeatureDescriptiveStatistics] = None,
        specific_value: Optional[Union[int, float]] = None,
        shift_detected: bool = False,
        difference: Optional[float] = None,
        raised_exception: bool = False,
        execution_id: Optional[int] = None,
        job_name: Optional[str] = None,
    ) -> FeatureMonitoringResult:
        """Build feature monitoring result.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            execution_id: int. Id of the job execution.
            detection_statistics: FeatureDescriptiveStatistics. Statistics computed from the detection data.
            reference_statistics: Optional[Union[FeatureDescriptiveStatistics, int, float]]. Statistics computed from the reference data.
                Defaults to None if no reference is provided.
            shift_detected: bool. Whether a shift is detected between the detection and reference window.
                It is used to decide whether to trigger an alert.
            difference: Optional[float]. Difference between detection statistics and reference statistics.

        Returns:
            FeatureMonitoringResult. Saved Feature monitoring result.
        """
        monitoring_time = round(
            util.convert_event_time_to_timestamp(datetime.now()), -3
        )
        if execution_id is None and job_name is not None:
            execution_id = self.get_monitoring_job_execution_id(job_name)
        else:
            execution_id = 0
        detection_statistics_id = (
            detection_statistics.id
            if isinstance(detection_statistics, FeatureDescriptiveStatistics)
            else None
        )
        reference_statistics_id = (
            reference_statistics.id
            if isinstance(reference_statistics, FeatureDescriptiveStatistics)
            else None
        )
        if raised_exception and feature_name is None:
            # if feature name is null it is a whole entity monitoring job
            feature_name = ""

        return FeatureMonitoringResult(
            feature_store_id=self._feature_store_id,
            config_id=config_id,
            execution_id=execution_id,
            feature_name=feature_name,
            detection_statistics_id=detection_statistics_id,
            reference_statistics_id=reference_statistics_id,
            specific_value=specific_value,
            difference=difference,
            shift_detected=shift_detected,
            monitoring_time=monitoring_time,
            raised_exception=raised_exception,
            empty_detection_window=self.is_monitoring_window_empty(
                detection_statistics
            ),
            empty_reference_window=self.is_monitoring_window_empty(
                reference_statistics
            ),
        )

    def save_feature_monitoring_result(
        self,
        result: FeatureMonitoringResult,
    ) -> FeatureMonitoringResult:
        """Save feature monitoring result.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            execution_id: int. Id of the job execution.
            detection_statistics: FeatureDescriptiveStatistics. Statistics computed from the detection data.
            reference_statistics: Optional[Union[FeatureDescriptiveStatistics, int, float]]. Statistics computed from the reference data.
                Defaults to None if no reference is provided.
            shift_detected: bool. Whether a shift is detected between the detection and reference window.
                It is used to decide whether to trigger an alert.
            difference: Optional[float]. Difference between detection statistics and reference statistics.

        Returns:
            FeatureMonitoringResult. Saved Feature monitoring result.
        """
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

    def get_feature_monitoring_results(
        self,
        config_id: Optional[int] = None,
        config_name: Optional[str] = None,
        start_time: Optional[Union[str, int, datetime, date]] = None,
        end_time: Optional[Union[str, int, datetime, date]] = None,
        with_statistics: bool = True,
    ) -> List[FeatureMonitoringResult]:
        """Convenience method to fetch feature monitoring results from an entity.

        Args:
            config_id: Optional[int]. Id of the feature monitoring configuration.
                Defaults to None if config_name is provided.
            config_name: Optional[str]. Name of the feature monitoring configuration.
                Defaults to None if config_id is provided.
            start_time: Optional[Union[str, int, datetime, date]].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Optional[Union[str, int, datetime, date]].
                Query results with monitoring time less than or equal to end_time.
            with_statistics: bool. Whether to include the statistics attached to the results.
                Defaults to True. Set to False to fetch only monitoring metadata.

        Returns:
            List[FeatureMonitoringResult]. List of feature monitoring results.
        """
        if all([config_id is None, config_name is None]):
            raise ValueError(
                "Either config_id or config_name must be provided to fetch feature monitoring results."
            )
        elif all([config_id is not None, config_name is not None]):
            raise ValueError(
                "Only one of config_id or config_name can be provided to fetch feature monitoring results."
            )
        elif config_name is not None and isinstance(config_name, str):
            config = self._feature_monitoring_config_api.get_by_name(config_name)
            if not isinstance(config, fmc.FeatureMonitoringConfig):
                raise ValueError(
                    f"Feature monitoring configuration with name {config_name} does not exist."
                )
            config_id = config._id
        elif config_name is not None:
            raise TypeError(
                f"config_name must be of type str. Got {type(config_name)}."
            )
        elif config_id is not None and not isinstance(config_id, int):
            raise TypeError(f"config_id must be of type int. Got {type(config_id)}.")

        return self.fetch_all_feature_monitoring_results_by_config_id(
            config_id=config_id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    def _build_query_params(
        self,
        start_time: Union[str, int, datetime, date, None],
        end_time: Union[str, int, datetime, date, None],
        with_statistics: bool,
    ) -> Dict[str, Union[str, List[str]]]:
        """Build query parameters for feature monitoring result API calls.

        Args:
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.
            with_statistics: bool.
                Whether to include the statistics attached to the results or not

        Returns:
            Dict[str, Union[str, List[str]]]. Query parameters.
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
        reference_statistics: Optional[List[FeatureDescriptiveStatistics]] = None,
        specific_value: Optional[Union[int, float]] = None,
    ) -> List[FeatureMonitoringResult]:
        """Run and upload statistics comparison between detection and reference stats.

        Args:
            fm_config: FeatureMonitoringConfig. Feature monitoring configuration.
            detection_statistics: List[FeatureDescriptiveStatistics]. Computed statistics from detection data.
            reference_statistics: Optional[List[FeatureDescriptiveStatistics]]]. Computed statistics from reference data.
            specific_value: Optional[Union[int, float]]. A specific value to use as reference.

        Returns:
            Union[FeatureMonitoringResult, List[FeatureMonitoringResult]]. Feature monitoring result
        """
        if specific_value:
            # compare detection statistics against a specific value
            assert reference_statistics is None
            return self._run_and_save_statistics_comparison_specific_value(
                fm_config, detection_statistics, specific_value
            )

        if reference_statistics is not None:
            # compare detection statistics against reference statistics
            return self._run_and_save_statistics_comparison_reference_stats(
                fm_config, detection_statistics, reference_statistics
            )

        # otherwise, no comparison needed
        return [
            self.save_feature_monitoring_result(
                result=self.build_feature_monitoring_result(
                    config_id=fm_config.id,
                    feature_name=det_fds.feature_name,
                    detection_statistics=det_fds,
                )
            )
            for det_fds in detection_statistics
        ]

    def _run_and_save_statistics_comparison_specific_value(
        self,
        fm_config: "fmc.FeatureMonitoringConfig",
        detection_statistics: List[FeatureDescriptiveStatistics],
        specific_value: Union[int, float],
    ):
        assert len(detection_statistics) == 1, (
            "detection_statistics must a be a single FeatureDescriptiveStatistics object "
            "if reference_statistics or specific_value is provided."
        )
        det_fds = detection_statistics[0]
        difference, shift_detected = self.compute_difference_and_shift(
            fm_config=fm_config,
            detection_statistics=det_fds,
            specific_value=specific_value,
        )
        return [
            self.save_feature_monitoring_result(
                result=self.build_feature_monitoring_result(
                    config_id=fm_config.id,
                    feature_name=det_fds.feature_name,
                    detection_statistics=det_fds,
                    difference=difference,
                    shift_detected=shift_detected,
                    specific_value=specific_value,
                )
            )
        ]

    def _run_and_save_statistics_comparison_reference_stats(
        self,
        fm_config: "fmc.FeatureMonitoringConfig",
        detection_statistics: List[FeatureDescriptiveStatistics],
        reference_statistics: List[FeatureDescriptiveStatistics],
    ):
        assert len(reference_statistics) == len(
            detection_statistics
        ), "detection_statistics and reference_statistics must contain the same number of feature statistics"
        det_stats_set, ref_stats_set = set(), set()
        for det_fds, ref_fds in zip(detection_statistics, reference_statistics):
            det_stats_set.add((det_fds.feature_name, det_fds.feature_type))
            ref_stats_set.add((ref_fds.feature_name, ref_fds.feature_type))

        # check if detection and reference statistics contain the same features and feature types.
        # statistics computed on empty data will have feature type None, which can falsify the equality.
        # we shouldn't raise an exception in that case, so we just ignore it by checking the count statistic.
        if (
            (det_stats_set != ref_stats_set)
            and (detection_statistics[0].count != 0)
            and (reference_statistics[0].count != 0)
        ):
            raise FeatureStoreException(
                "Detection and reference statistics do not contain the same features or feature types"
            )

        # sort by feature name
        sorted_det_stats, sorted_ref_stats = sorted(
            detection_statistics, key=lambda fds: fds.feature_name
        ), sorted(reference_statistics, key=lambda fds: fds.feature_name)

        fm_results = []
        for det_fds, ref_fds in zip(sorted_det_stats, sorted_ref_stats):
            difference, shift_detected = self.compute_difference_and_shift(
                fm_config=fm_config,
                detection_statistics=det_fds,
                reference_statistics=ref_fds,
            )
            fm_results.append(
                self.save_feature_monitoring_result(
                    result=self.build_feature_monitoring_result(
                        config_id=fm_config.id,
                        feature_name=det_fds.feature_name,
                        detection_statistics=det_fds,
                        reference_statistics=ref_fds,
                        difference=difference,
                        shift_detected=shift_detected,
                    )
                )
            )
        return fm_results

    def compute_difference_and_shift(
        self,
        fm_config: "fmc.FeatureMonitoringConfig",
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: Optional[FeatureDescriptiveStatistics] = None,
        specific_value: Optional[Union[int, float]] = None,
    ) -> Tuple[Optional[float], bool]:
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
        difference = self.compute_difference_between_stats(
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            metric=fm_config.statistics_comparison_config["metric"].lower(),
            relative=fm_config.statistics_comparison_config["relative"],
            specific_value=specific_value,
        )
        if difference is None:
            return None, False  # no difference, no shift detected

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

    def compute_difference_between_stats(
        self,
        detection_statistics: FeatureDescriptiveStatistics,
        metric: str,
        relative: bool = False,
        reference_statistics: Optional[FeatureDescriptiveStatistics] = None,
        specific_value: Optional[Union[int, float]] = None,
    ) -> Optional[float]:
        """Compute the difference between the reference and detection statistics.

        Args:
            detection_statistics: `FeatureDescriptiveStatistics`. Computed statistics from detection data.
            reference_statistics: `Optional[FeatureDescriptiveStatistics]`.
                Computed statistics from reference data, or a specific value to use as reference
            metric: `str`. The metric to compute the difference for.
            relative: `bool`. Whether to compute the relative difference or not.
            specific_value: `Optional[Union[int, float]]`. A specific value to use as reference.

        Returns:
            `Optional[float]`. The difference between the reference and detection statistics, or None if there
                               are no values to compare
        """
        if (detection_statistics.count == 0) or (
            specific_value is None and reference_statistics.count == 0
        ):
            # if stats on empty data, no difference can be computed
            return None

        # otherwise, both detection and reference value can be obtained
        detection_value = detection_statistics.get_value(metric)
        reference_value = (
            specific_value
            if specific_value is not None
            else reference_statistics.get_value(metric)
        )
        return self.compute_difference_between_specific_values(
            detection_value, reference_value, relative
        )

    def compute_difference_between_specific_values(
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

        diff = abs(detection_value - reference_value)
        if relative:
            if reference_value == 0:
                return float("inf")
            else:
                return diff / reference_value
        else:
            return diff

    def get_monitoring_job_execution_id(
        self,
        job_name: str,
    ) -> int:
        """Get the execution id of the last execution of the monitoring job.

        The last execution is assumed to be the current execution.
        The id defaults to 0 if no execution is found.

        Args:
            job_name: str. Name of the monitoring job.

        Returns:
            int. Id of the last execution of the monitoring job.
                It is assumed to be the current execution.
        """
        execution = self._job_api.last_execution(self._job_api.get(name=job_name))
        return (
            execution[0]._id
            if isinstance(execution, list) and len(execution) > 0
            else 0
        )

    def is_monitoring_window_empty(
        self,
        monitoring_window_statistics: Optional[FeatureDescriptiveStatistics] = None,
    ) -> bool:
        """Check if the monitoring window is empty.

        Args:
            monitoring_window_statistics: FeatureDescriptiveStatistics. Statistics computed for the monitoring window.

        Returns:
            bool. Whether the monitoring window is empty or not.
        """
        if monitoring_window_statistics is None:
            return True
        elif monitoring_window_statistics.count == 0:
            return True
        else:
            return False

    def save_feature_monitoring_result_with_exception(
        self,
        config_id: int,
        job_name: str,
        feature_name: Optional[str] = None,
    ) -> "FeatureMonitoringResult":
        """Save feature monitoring result with raised_exception flag.

        Args:
            config_id: int. Id of the feature monitoring configuration.
            job_name: str. Name of the monitoring job.

        Returns:
            FeatureMonitoringResult. Saved Feature monitoring result.
        """
        return self.save_feature_monitoring_result(
            result=self.build_feature_monitoring_result(
                config_id=config_id,
                job_name=job_name,
                raised_exception=True,
                feature_name=feature_name,
            ),
        )

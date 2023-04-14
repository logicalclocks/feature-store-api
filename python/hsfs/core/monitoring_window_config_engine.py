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
from typing import Optional, Union, Tuple
import re
from datetime import datetime, timedelta

from hsfs.core.monitoring_window_config import MonitoringWindowConfig, WindowConfigType
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs import feature_group, feature_view


class MonitoringWindowConfigEngine:
    def __init__(self) -> "MonitoringWindowConfigEngine":
        # No need to initialize anything
        pass

    def validate_monitoring_window_config(
        self,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        specific_id: Optional[int] = None,
        specific_value: Optional[Union[int, float]] = None,
        row_percentage: Optional[int] = None,
    ) -> WindowConfigType:
        if isinstance(specific_value, int) or isinstance(specific_value, float):
            if any(
                [
                    time_offset is not None,
                    window_length is not None,
                    specific_id is not None,
                    row_percentage is not None,
                ]
            ):
                raise ValueError(
                    "If specific_value is set, no other parameter can be set."
                )
            return WindowConfigType.SPECIFIC_VALUE

        if isinstance(specific_id, int):
            if any(
                [
                    time_offset is not None,
                    window_length is not None,
                    row_percentage is not None,
                ]
            ):
                raise ValueError(
                    "If training_dataset_id is set, no other parameter can be set."
                )
            return WindowConfigType.TRAINING_DATASET

        if isinstance(time_offset, str):
            return WindowConfigType.ROLLING_TIME

        if isinstance(window_length, str):
            raise ValueError("window_length can only be set if time_offset is set.")

        return WindowConfigType.ALL_TIME

    def build_monitoring_window_config(
        self,
        id: Optional[int] = None,
        window_config_type: Optional[WindowConfigType] = None,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        specific_id: Optional[int] = None,
        specific_value: Optional[Union[int, float]] = None,
        row_percentage: Optional[int] = None,
    ) -> MonitoringWindowConfig:
        """Builds a monitoring window config.

        Args:
            window_config_type: str, required
                Type of the window config, can be either
                `ROLLING_TIME`,`SPECIFIC_VALUE`,`TRAINING_DATASET`.
            time_offset: str, optional
                monitoring window start time is computed as "now - time_offset".
            window_length: str, optional
                monitoring window end time is computed as
                    "now - time_offset + window_length".
            specific_id: int, optional
                Specific id of an entity that has fixed statistics.
            specific_value: float, optional
                Specific value instead of a statistics computed on data.
            row_percentage: int, optional
                Percentage of rows to be used for statistics computation.
            id: int, optional
                Id of the monitoring window config in hopsworks.

        Returns:
            MonitoringWindowConfig The monitoring window configuration.
        """

        detected_window_config_type = self.validate_monitoring_window_config(
            time_offset=time_offset,
            window_length=window_length,
            specific_id=specific_id,
            specific_value=specific_value,
            row_percentage=row_percentage,
        )

        if (
            window_config_type is not None
            and window_config_type != detected_window_config_type
        ):
            raise ValueError(
                "The window_config_type does not match the parameters set."
            )

        return MonitoringWindowConfig(
            id=id,
            window_config_type=detected_window_config_type,
            time_offset=time_offset,
            window_length=window_length,
            specific_id=specific_id,
            specific_value=specific_value,
            row_percentage=row_percentage,
        )

    def fetch_statistics_based_on_monitoring_window_config(
        self,
        entity: Union["feature_group.FeatureGroup", "feature_view.FeatureView"],
        feature_name: str,
        monitoring_window_config: MonitoringWindowConfig,
    ) -> FeatureDescriptiveStatistics:
        """Fetch feature statistics based on a feature monitoring window configuration

        Args:
            entity: Union[FeatureGroup, FeatureView]: Entity on which statistics where computed.
            feature_name: str: Name of the feature from which statistics where computed.
            monitoring_window_config: MonitoringWindowConfig: Monitoring window config.

        Returns:
            FeatureDescriptiveStatistics: Descriptive statistics
        """
        start_time, end_time = self.get_window_start_end_times(
            monitoring_window_config=monitoring_window_config,
        )
        return (
            self._statistics_engine.get_by_feature_name_time_window_and_row_percentage(
                entity,
                feature_name,
                start_time,
                end_time,
                monitoring_window_config.row_percentage,
            )
        )

    def time_range_str_to_time_delta(self, time_range: str) -> timedelta:
        months, weeks, days, hours = re.search(
            r"(\d+)m(\d+)w(\d+)d(\d+)h",
            time_range,
        ).groups(0)

        return timedelta(months=months, weeks=weeks, days=days, hours=hours)

    def get_window_start_end_times(
        self,
        monitoring_window_config: MonitoringWindowConfig,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        end_time = datetime.now()
        if (
            monitoring_window_config.window_config_type != "ROLLING_TIME"
            or monitoring_window_config.window_config_type != "ALL_TIME"
        ):
            return (None, end_time)

        if monitoring_window_config.time_offset is not None:
            time_offset = self.time_range_str_to_time_delta(
                monitoring_window_config.time_offset
            )
            start_time = datetime.now() - time_offset
        else:
            # case where time_offset is None and window_length is None
            return (None, end_time)

        if monitoring_window_config.window_length is not None:
            window_length = self.time_range_str_to_time_delta(
                monitoring_window_config.window_length
            )
            return (
                start_time,
                start_time + window_length,
            )
        else:
            return (start_time, end_time)

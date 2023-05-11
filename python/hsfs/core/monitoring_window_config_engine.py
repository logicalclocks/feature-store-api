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

from hsfs.core import monitoring_window_config as mwc
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.util import convert_event_time_to_timestamp
from hsfs import feature_group, feature_view
from hsfs.core import statistics_engine


class MonitoringWindowConfigEngine:
    _MAX_TIME_RANGE_LENGTH = 12

    def __init__(self) -> "MonitoringWindowConfigEngine":
        # No need to initialize anything
        pass

    def validate_monitoring_window_config(
        self,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        training_dataset_id: Optional[int] = None,
        specific_value: Optional[Union[int, float]] = None,
        row_percentage: Optional[float] = None,
    ) -> "mwc.WindowConfigType":
        if isinstance(specific_value, int) or isinstance(specific_value, float):
            if any(
                [
                    time_offset is not None,
                    window_length is not None,
                    training_dataset_id is not None,
                    row_percentage is not None,
                ]
            ):
                raise ValueError(
                    "If specific_value is set, no other parameter can be set."
                )
            return mwc.WindowConfigType.SPECIFIC_VALUE

        if isinstance(training_dataset_id, int):
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
            return mwc.WindowConfigType.TRAINING_DATASET

        if isinstance(time_offset, str):
            return mwc.WindowConfigType.ROLLING_TIME

        if isinstance(window_length, str):
            raise ValueError("window_length can only be set if time_offset is set.")

        return mwc.WindowConfigType.ALL_TIME

    def build_monitoring_window_config(
        self,
        id: Optional[int] = None,
        window_config_type: Optional[Union["mwc.WindowConfigType", str]] = None,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        training_dataset_id: Optional[int] = None,
        specific_value: Optional[Union[int, float]] = None,
        row_percentage: Optional[float] = None,
    ) -> "mwc.MonitoringWindowConfig":
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
            training_dataset_id: int, optional
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
            training_dataset_id=training_dataset_id,
            specific_value=specific_value,
            row_percentage=row_percentage,
        )

        if (
            isinstance(window_config_type, str)
            and window_config_type != detected_window_config_type
        ):
            raise ValueError(
                "The window_config_type does not match the parameters set."
            )

        if (
            window_config_type
            in [mwc.WindowConfigType.ROLLING_TIME, mwc.WindowConfigType.ALL_TIME]
            and row_percentage is None
        ):
            row_percentage = 1.0

        return mwc.MonitoringWindowConfig(
            id=id,
            window_config_type=detected_window_config_type,
            time_offset=time_offset,
            window_length=window_length,
            training_dataset_id=training_dataset_id,
            specific_value=specific_value,
            row_percentage=row_percentage,
        )

    def fetch_statistics_based_on_monitoring_window_config(
        self,
        entity: Union["feature_group.FeatureGroup", "feature_view.FeatureView"],
        feature_name: str,
        monitoring_window_config: "mwc.MonitoringWindowConfig",
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
        the_statistics_engine = statistics_engine.StatisticsEngine(
            feature_store_id=entity.feature_store_id,
            entity_type=entity.ENTITY_TYPE,
        )
        return the_statistics_engine.get_by_commit_time_window(
            entity,
            start_time=start_time,
            end_time=end_time,
            feature_name=feature_name,
            row_percentage=monitoring_window_config.row_percentage,
        )

    def time_range_str_to_time_delta(
        self, time_range: str, field_name: Optional[str] = "time_offset"
    ) -> timedelta:
        # sanitize input
        value_error_message = f"Invalid {field_name} format: {time_range}. Use format: 1w2d3h for 1 week, 2 days and 3 hours."
        if (
            len(time_range) > self._MAX_TIME_RANGE_LENGTH
            or re.search(r"([^dwh\d]+)", time_range) is not None
        ):
            raise ValueError(value_error_message)

        matches = re.search(
            # r"^(?!$)(?:.*(?P<week>\d+)w)?(?:.*(?P<day>\d+)d)?(?:.*(?P<hour>\d+)h)?$",
            r"(?:(?P<week>\d+w)()|(?P<day>\d+d)()|(?P<hour>\d+h)())+",
            time_range,
        )
        if matches is None:
            raise ValueError(value_error_message)

        weeks = (
            int(matches.group("week").replace("w", ""))
            if matches.group("week") is not None
            else 0
        )
        days = (
            int(matches.group("day").replace("d", ""))
            if matches.group("day") is not None
            else 0
        )
        hours = (
            int(matches.group("hour").replace("h", ""))
            if matches.group("hour") is not None
            else 0
        )

        return timedelta(weeks=weeks, days=days, hours=hours)

    def get_window_start_end_times(
        self,
        monitoring_window_config: "mwc.MonitoringWindowConfig",
    ) -> Tuple[Optional[int], int]:
        end_time = datetime.now()
        if monitoring_window_config.window_config_type not in [
            mwc.WindowConfigType.ROLLING_TIME,
            mwc.WindowConfigType.ALL_TIME,
        ]:
            return (None, convert_event_time_to_timestamp(end_time))

        if monitoring_window_config.time_offset is not None:
            time_offset = self.time_range_str_to_time_delta(
                monitoring_window_config.time_offset
            )
            start_time = datetime.now() - time_offset
        else:
            # case where time_offset is None and window_length is None
            return (None, convert_event_time_to_timestamp(end_time))

        if monitoring_window_config.window_length is not None:
            window_length = self.time_range_str_to_time_delta(
                monitoring_window_config.window_length
            )
            end_time = (
                start_time + window_length
                if start_time + window_length < end_time
                else end_time
            )

        return (
            convert_event_time_to_timestamp(start_time),
            convert_event_time_to_timestamp(end_time),
        )

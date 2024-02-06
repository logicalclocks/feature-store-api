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

import json
import humps
from typing import List, Optional, Union
from hsfs.util import FeatureStoreEncoder
from enum import Enum

from hsfs.core import monitoring_window_config_engine


class WindowConfigType(str, Enum):
    ALL_TIME = "ALL_TIME"
    ROLLING_TIME = "ROLLING_TIME"
    TRAINING_DATASET = "TRAINING_DATASET"
    SPECIFIC_VALUE = "SPECIFIC_VALUE"

    @classmethod
    def list_str(cls) -> List[str]:
        return list(map(lambda c: c.value, cls))

    @classmethod
    def list(cls) -> List["WindowConfigType"]:
        return list(map(lambda c: c, cls))

    @classmethod
    def from_str(cls, value: str) -> "WindowConfigType":
        if value in cls.list_str():
            return cls(value)
        else:
            raise ValueError(
                f"Invalid value {value} for WindowConfigType, allowed values are {cls.list_str()}"
            )

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, self.__class__):
            return self is other
        return False

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


class MonitoringWindowConfig:
    DEFAULT_ROW_PERCENTAGE = 1.0

    def __init__(
        self,
        id: Optional[int] = None,
        window_config_type: Optional[
            Union[str, WindowConfigType]
        ] = WindowConfigType.SPECIFIC_VALUE,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        training_dataset_version: Optional[int] = None,
        specific_value: Optional[float] = None,
        row_percentage: Optional[float] = None,
        **kwargs,
    ):
        """Configuration to define the slice of data to compute statistics on.

        !!! example
            ```python3
            # Detection or reference window
            ## Rolling Time Window
            monitoring_window_config = MonitoringWindowConfig(
                time_offset="1d", # data inserted up to 1 day ago
                window_length="1h", # data inserted until an one hour after time_offset
                row_percentage=0.2, # include only 20% of the rows when computing statistics
            )

            ## Rolling Commit Window (not supported yet)
            monitoring_window_config = MonitoringWindowConfig(
                commit_offset=10, # data inserted up to 10 commits ago
                commit_num=5, # include 5 commits after commit_offset
            )

            # Only available for reference window
            ## Specific value
            monitoring_window_config = MonitoringWindowConfig(specific_value=0.5)

            ## Training dataset
            monitoring_window_config = MonitoringWindowConfig(
                training_dataset_version=my_training_dataset.version
            )
            ```

        # Arguments
            id: int, optional
                The id of the monitoring window config.
            window_config_type: str, optional
                The type of the monitoring window config. One of ROLLING_TIME,
                TRAINING_DATASET, SPECIFIC_VALUE.
            time_offset: str, optional
                The time offset of the monitoring window config. Only used for
                INSERT and SNAPSHOT window config types.
            window_length: str, optional
                The window length of the monitoring window config. Only used for
                INSERT and SNAPSHOT window config types.
            training_dataset_version: int, optional
                The version of the training dataset to use as reference. Only used for
                TRAINING_DATASET window config type.
            specific_value: float, optional
                The specific value to use as reference. Only used for SPECIFIC_VALUE
                window config type.
            row_percentage: float, optional
                The fraction of rows to use when computing statistics [0, 1.0]. Only used
                for ROLLING_TIME and ALL_TIME window config types.

        # Raises
            AttributeError: If window_config_type is not one of INSERT, SNAPSHOT,
                BATCH, TRAINING_DATASET, SPECIFIC_VALUE.
        """
        self._id = id
        self._window_config_type = None
        self.window_config_type = window_config_type
        self._time_offset = time_offset
        self._window_length = window_length
        self._training_dataset_version = training_dataset_version
        self._specific_value = specific_value

        if self.window_config_type in [
            WindowConfigType.SPECIFIC_VALUE,
            WindowConfigType.TRAINING_DATASET,
        ]:
            self._row_percentage = None
        else:
            self.row_percentage = row_percentage

        self._window_config_engine = (
            monitoring_window_config_engine.MonitoringWindowConfigEngine()
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        the_dict = {
            "id": self._id,
            "windowConfigType": self._window_config_type,
        }

        if (
            self._window_config_type == WindowConfigType.ROLLING_TIME
            or self._window_config_type == WindowConfigType.ALL_TIME
        ):
            the_dict["timeOffset"] = self._time_offset
            the_dict["windowLength"] = self._window_length
            the_dict["rowPercentage"] = self.row_percentage
        elif self._window_config_type == WindowConfigType.SPECIFIC_VALUE:
            the_dict["specificValue"] = self._specific_value
        elif self._window_config_type == WindowConfigType.TRAINING_DATASET:
            the_dict["trainingDatasetVersion"] = self._training_dataset_version

        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"MonitoringWindowConfig({self._window_config_type!r})"

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def window_config_type(self) -> WindowConfigType:
        return self._window_config_type

    @window_config_type.setter
    def window_config_type(self, window_config_type: Union[WindowConfigType, str]):
        if self._window_config_type is not None:
            raise AttributeError("window_config_type is a read-only attribute.")

        if isinstance(window_config_type, WindowConfigType):
            self._window_config_type = window_config_type
            return

        if not isinstance(window_config_type, str):
            raise TypeError(
                "window_config_type must be a string or WindowConfigType. "
                "Allowed value are" + str(WindowConfigType.list_str())
            )
        elif window_config_type not in WindowConfigType.list_str():
            raise ValueError(
                "window_config_type must be one of "
                + str(WindowConfigType.list_str())
                + "."
            )
        else:
            self._window_config_type = WindowConfigType.from_str(window_config_type)

    @property
    def time_offset(self) -> Optional[str]:
        return self._time_offset

    @property
    def window_length(self) -> Optional[str]:
        return self._window_length

    @window_length.setter
    def window_length(self, window_length: Optional[str]):
        if window_length is None:
            self._window_length = None
        elif self._window_config_type != WindowConfigType.ROLLING_TIME:
            raise AttributeError(
                "Window length can only be set for if window_config_type is ROLLING_TIME."
            )
        elif isinstance(window_length, str):
            self._window_config_engine.time_range_str_to_time_delta(window_length)
            self._window_length = window_length
        else:
            raise TypeError("window_length must be a string.")

    @property
    def training_dataset_version(self) -> Optional[int]:
        """The version of the training dataset to use as reference. Only used for
        TRAINING_DATASET window config type."""
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version: Optional[int]):
        if (
            self._window_config_type != WindowConfigType.TRAINING_DATASET
            and training_dataset_version is not None
        ):
            raise AttributeError(
                "training_dataset_version can only be set for if window_config_type is TRAINING_DATASET."
            )
        self._training_dataset_version = training_dataset_version

    @property
    def specific_value(self) -> Optional[float]:
        return self._specific_value

    @specific_value.setter
    def specific_value(self, specific_value: Optional[float]):
        if (
            self._window_config_type != WindowConfigType.SPECIFIC_VALUE
            and specific_value is not None
        ):
            raise AttributeError(
                "Specific value can only be set for if window_config_type is SPECIFIC_VALUE."
            )
        self._specific_value = specific_value

    @property
    def row_percentage(self) -> Optional[float]:
        """The percentage of rows to fetch and compute the statistics on. Only used for ROLLING_TIME and ALL_TIME."""
        return self._row_percentage

    @row_percentage.setter
    def row_percentage(self, row_percentage: Optional[float]):
        if self.window_config_type in [
            WindowConfigType.SPECIFIC_VALUE,
            WindowConfigType.TRAINING_DATASET,
        ]:
            raise AttributeError(
                "Row percentage can only be set for ROLLING_TIME and ALL_TIME"
                " window config types."
            )

        if isinstance(row_percentage, int) or isinstance(row_percentage, float):
            row_percentage = float(row_percentage)
            if row_percentage <= 0.0 or row_percentage > 1.0:
                raise ValueError("Row percentage must be a float between 0 and 1.")
            self._row_percentage = row_percentage
        elif row_percentage is None:
            self._row_percentage = self.DEFAULT_ROW_PERCENTAGE
        else:
            raise TypeError("Row percentage must be a float between 0 and 1.")

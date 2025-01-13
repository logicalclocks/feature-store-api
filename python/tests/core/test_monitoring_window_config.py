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
import pytest
from hsfs.core import monitoring_window_config as mwc


class TestMonitoringWindowConfig:
    def test_window_based_on_training_dataset_version(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.TRAINING_DATASET,
            training_dataset_version=1,
        )

        # Act
        # forbidden update for training dataset
        with pytest.raises(AttributeError):
            window_config.time_offset = "1d"
        with pytest.raises(AttributeError):
            window_config.window_length = "1h"
        with pytest.raises(AttributeError):
            window_config.specific_value = 0.2
        with pytest.raises(AttributeError):
            window_config.row_percentage = 0.2

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.TRAINING_DATASET
        assert window_config.training_dataset_version == 1
        assert window_config.time_offset is None
        assert window_config.window_length is None
        assert window_config.specific_value is None
        assert window_config.row_percentage is None

    def test_window_based_on_rolling_time(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1d",
            window_length="1h",
            row_percentage=0.2,
        )

        # Act
        # forbidden update for rolling time
        with pytest.raises(AttributeError):
            window_config.training_dataset_version = 1
        with pytest.raises(AttributeError):
            window_config.specific_value = 0.2

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.ROLLING_TIME
        assert window_config.training_dataset_version is None
        assert window_config.time_offset == "1d"
        assert window_config.window_length == "1h"
        assert window_config.specific_value is None
        assert window_config.row_percentage == 0.2

    def test_window_based_on_all_time(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ALL_TIME,
            row_percentage=0.2,
        )

        # Act
        # forbidden update for all time
        with pytest.raises(AttributeError):
            window_config.training_dataset_version = 1
        with pytest.raises(AttributeError):
            window_config.time_offset = "1d"
        with pytest.raises(AttributeError):
            window_config.window_length = "1h"
        with pytest.raises(AttributeError):
            window_config.specific_value = 0.2

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.ALL_TIME
        assert window_config.training_dataset_version is None
        assert window_config.time_offset is None
        assert window_config.window_length is None
        assert window_config.specific_value is None
        assert window_config.row_percentage == 0.2

    def test_window_based_on_specific_value(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.SPECIFIC_VALUE,
            specific_value=0.2,
        )

        # Act
        # forbidden update for specific value
        with pytest.raises(AttributeError):
            window_config.training_dataset_version = 1
        with pytest.raises(AttributeError):
            window_config.time_offset = "1d"
        with pytest.raises(AttributeError):
            window_config.window_length = "1h"
        with pytest.raises(AttributeError):
            window_config.row_percentage = 0.2

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.SPECIFIC_VALUE
        assert window_config.training_dataset_version is None
        assert window_config.time_offset is None
        assert window_config.window_length is None
        assert window_config.specific_value == 0.2
        assert window_config.row_percentage is None

    def test_window_config_type_list_str(self):
        # Arrange
        window_config_type_list = mwc.WindowConfigType.list_str()

        # Assert
        assert set(window_config_type_list) == set(
            [
                "ALL_TIME",
                "ROLLING_TIME",
                "TRAINING_DATASET",
                "SPECIFIC_VALUE",
            ]
        )

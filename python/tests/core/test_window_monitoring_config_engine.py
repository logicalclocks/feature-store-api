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


from hsfs.core.monitoring_window_config import WindowConfigType
from hsfs.core.monitoring_window_config_engine import MonitoringWindowConfigEngine


class TestMonitoringWindowConfigEngine:
    # This needs more extensive unit testing once we have implemented some logic to
    # verify the compatibility of the different args
    def test_build_monitoring_window_config(self):
        # Arrange
        engine = MonitoringWindowConfigEngine()

        # Act
        window_config = engine.build_monitoring_window_config(
            time_offset="1w",
        )

        # Assert
        assert window_config.window_config_type == WindowConfigType.ROLLING_TIME
        assert window_config.time_offset == "1w"
        assert (
            window_config.row_percentage == 1.0
        )  # Default value set in MonitoringWindowConfig

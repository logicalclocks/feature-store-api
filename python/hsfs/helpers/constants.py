#
#   Copyright 2024 HOPSWOKRS AB
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

VERBOSE_ENV_VAR = "HOPSWORKS_VERBOSE"
USE_RICH_LOGGER_ENV_VAR = "HOPSWORKS_USE_RICH_LOGGER"
USE_RICH_CONSOLE_ENV_VAR = "HOPSWORKS_USE_RICH_CONSOLE"
DEFAULT_VERBOSE_CONFIG = {
    "tab_size": 4,
    "width": 88,
    "color_system": "truecolor",
}
PYTHON_LEXER_THEME = "github-dark"

SHOW_FG_TYPE_MAPPING = {
    "stream": "Stream",
    "spine": "Spine",
    "external": "External",
}
GET_OR_ENABLE_RICH_CONSOLE_ERROR_MESSAGE = (
    "Using `rich` console is not enabled."
    + f" Please set the environment variable `{USE_RICH_CONSOLE_ENV_VAR}` to `true` or `1` to enable it."
)
ENABLE_RICH_FOR_PRETTY_VERBOSITY_ERROR_MESSAGE = (
    "Hopsworks Python SDK has a verbose mode"
    + " using `rich` to print nicely formatted user message design help you get started."
    + f"Please set the environment variable `{USE_RICH_CONSOLE_ENV_VAR}` and `{VERBOSE_ENV_VAR}` to `true` or `1` to enable it."
)

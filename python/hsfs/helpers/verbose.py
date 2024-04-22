#
#   Copyright 2024 HOPSWORKS AB
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
from __future__ import annotations

import os

from hsfs.helpers import constants
from rich.console import Console


_rich_console = None


def enable_rich_verbose_mode() -> None:
    os.environ[constants.VERBOSE_ENV_VAR] = "true"
    os.environ[constants.USE_RICH_CONSOLE_ENV_VAR] = "true"


def is_rich_print_enabled() -> bool:
    use_rich = os.getenv(constants.USE_RICH_CONSOLE_ENV_VAR, "true").lower()
    return use_rich == "true" or use_rich == "1"


def is_hsfs_verbose() -> bool:
    hopsworks_verbose = os.getenv(constants.VERBOSE_ENV_VAR, "1").lower()
    return hopsworks_verbose == "true" or hopsworks_verbose == "1"


def init_rich_with_default_config() -> None:
    global _rich_console
    if _rich_console is None:
        _rich_console = Console(**constants.DEFAULT_VERBOSE_CONFIG)


def get_rich_console() -> Console:
    global _rich_console
    if _rich_console is None:
        init_rich_with_default_config()
    return _rich_console

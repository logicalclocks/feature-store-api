#
#   Copyright 2020 Logical Clocks AB
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
import warnings


# Setting polars skip cpu flag to suppress CPU false positive warning messages printed while importing hsfs
os.environ["POLARS_SKIP_CPU_CHECK"] = "1"

from hsfs import (  # noqa: E402,  Module level import not at top of file because os.environ must be set before importing hsfs
    usage,
    util,
    version,
)
from hsfs.connection import (  # noqa: E402,  Module level import not at top of file because os.environ must be set before importing hsfs
    Connection,
)


__version__ = version.__version__

connection = Connection.connection


def fs_formatwarning(message, category, filename, lineno, line=None):
    return "{}: {}\n".format(category.__name__, message)


warnings.formatwarning = fs_formatwarning
warnings.simplefilter("always", util.VersionWarning)
warnings.filterwarnings(
    action="ignore", category=DeprecationWarning, module=r".*ipykernel"
)


def disable_usage_logging():
    usage.disable()


def get_sdk_info():
    return usage.get_env()


__all__ = ["connection", "disable_usage_logging", "get_sdk_info"]

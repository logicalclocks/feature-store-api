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
from __future__ import annotations

import logging
import os


def is_rich_logger_enabled():
    return os.getenv("HOPSWORKS_USE_RICH_LOGGER", "true").lower() == "true"


def set_rich_for_hsfs_root_logger():
    logger = logging.getLogger("hsfs")
    if is_rich_logger_enabled():
        from rich.logging import RichHandler

        rich_handler = RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            tracebacks_show_hidden_frames=True,
        )
        logger.addHandler(rich_handler)
    return logger

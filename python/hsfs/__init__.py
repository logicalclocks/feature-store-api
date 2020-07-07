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

import warnings

from hsfs import util
from hsfs.connection import Connection

connection = Connection.connection
setup_databricks = Connection.setup_databricks


def fs_formatwarning(message, category, filename, lineno, line=None):
    return "{}: {}\n".format(category.__name__, message)


warnings.formatwarning = fs_formatwarning
warnings.simplefilter("always", util.VersionWarning)

__all__ = ["connection", "setup_databricks"]

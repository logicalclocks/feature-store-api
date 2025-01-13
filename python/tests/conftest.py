#
#   Copyright 2022 Hopsworks AB
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

import os
import sys


pytest_plugins = [
    "tests.fixtures.backend_fixtures",
    "tests.fixtures.dataframe_fixtures",
]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# set hadoop home if on windows
if os.name == "nt":
    current_path = os.path.dirname(os.path.realpath(__file__))
    os.environ["HADOOP_HOME"] = current_path + "/data/hadoop/"

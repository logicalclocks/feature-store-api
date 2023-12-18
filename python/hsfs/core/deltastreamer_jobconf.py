#
#   Copyright 2022 Logical Clocks AB
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

from hsfs import util


class DeltaStreamerJobConf:
    def __init__(self, options, spark_options, **kwargs):
        self._options = options
        self._spark_options = spark_options

    def to_dict(self):
        return {
            "writeOptions": self._options,
            "sparkJobConfiguration": self._spark_options,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

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

import json
from hsfs import util


class IngestionJobConf:
    def __init__(
        self, data_format, data_options, write_options, spark_job_configuration
    ):
        self._data_format = data_format
        self._data_options = data_options
        self._write_options = write_options
        self._spark_job_configuration = spark_job_configuration

    @property
    def data_format(self):
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._data_format = data_format

    @property
    def data_options(self):
        return self._data_options

    @data_options.setter
    def data_options(self, data_options):
        self._data_options = data_options

    @property
    def write_options(self):
        return self._write_options

    @write_options.setter
    def write_options(self, write_options):
        self._write_options = write_options

    @property
    def spark_job_configuration(self):
        return self._spark_job_configuration

    @spark_job_configuration.setter
    def spark_job_configuration(self, spark_job_configuration):
        self._spark_job_configuration = spark_job_configuration

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "dataFormat": self._data_format,
            "dataOptions": self._data_options,
            "writeOptions": [
                {"name": k, "value": v} for k, v in self._write_options.items()
            ]
            if self._write_options
            else None,
            "sparkJobConfiguration": self._spark_job_configuration,
        }

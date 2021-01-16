#
#   Copyright 2021 Logical Clocks AB
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


class TrainingDatsetJobConf:
    def __init__(self, query, overwrite, write_options, spark_job_configuration):
        self._query = query
        self._overwrite = overwrite
        self._write_options = write_options
        self._spark_job_configuration = spark_job_configuration

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = query

    @property
    def overwrite(self):
        return self._overwrite

    @overwrite.setter
    def overwrite(self, overwrite):
        self._overwrite = overwrite

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
            "query": self._query,
            "overwrite": self._overwrite,
            "writeOptions": [
                {"name": k, "value": v} for k, v in self._write_options.items()
            ]
            if self._write_options
            else None,
            "sparkJobConfiguration": self._spark_job_configuration,
        }

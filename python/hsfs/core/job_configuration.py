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


class JobConfiguration:

    PYSPARK_MAIN_CLASS = "org.apache.spark.deploy.PythonRunner"
    PYSPARK_JOB_TYPE = "PySpark"
    DTO_TYPE = "sparkJobConfiguration"

    def __init__(
        self,
        app_path,
        default_args,
        am_memory=1024,
        am_cores=1,
        executor_memory=2048,
        executor_cores=1,
        executor_instances=1,
        dynamic_allocation=True,
        dynamic_min_executors=1,
        dynamic_max_executors=5,
    ):
        self._default_args = default_args
        self._app_path = app_path
        self._am_memory = am_memory
        self._am_cores = am_cores
        self._executor_memory = executor_memory
        self._executor_cores = executor_cores
        self._executor_instances = executor_instances
        self._dynamic_allocation = dynamic_allocation
        self._dynamic_min_executors = dynamic_max_executors
        self._dynamic_max_executors = dynamic_max_executors

    def to_dict(self):
        return {
            "defaultArgs": self._default_args,
            "appPath": self._app_path,
            "amMemory": self._am_memory,
            "amCores": self._am_cores,
            "mainClass": JobConfiguration.PYSPARK_MAIN_CLASS,
            "jobType": JobConfiguration.PYSPARK_JOB_TYPE,
            "spark.executor.memory": self._executor_memory,
            "spark.executor.cores": self._executor_cores,
            "spark.executor.instances": self._executor_instances,
            "spark.dynamicAllocation.enabled": self._dynamic_allocation,
            "spark.dynamicAllocation.minExecutors": self._dynamic_min_executors,
            "spark.dynamicAllocation.maxExecutors": self._dynamic_max_executors,
            "type": JobConfiguration.DTO_TYPE,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

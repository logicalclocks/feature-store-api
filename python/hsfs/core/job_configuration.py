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

import json

from hsfs import util


class JobConfiguration:
    DTO_TYPE = "sparkJobConfiguration"
    DEFAULT_PROPERTIES = {"spark.yarn.maxAppAttempts": 2}

    def __init__(
        self,
        am_memory=2048,
        am_cores=1,
        executor_memory=4096,
        executor_cores=1,
        executor_instances=1,
        dynamic_allocation=True,
        dynamic_min_executors=1,
        dynamic_max_executors=2,
        properties="",
        **kwargs,
    ):
        self._am_memory = am_memory
        self._am_cores = am_cores
        self._executor_memory = executor_memory
        self._executor_cores = executor_cores
        self._executor_instances = executor_instances
        self._dynamic_allocation = dynamic_allocation
        self._dynamic_min_executors = dynamic_min_executors
        self._dynamic_max_executors = dynamic_max_executors

        # Add default properties to the properties
        default_properties = "\n".join([
            f"{key}={value}" if not properties or key not in properties else ""
            for key, value in JobConfiguration.DEFAULT_PROPERTIES.items()])
        if properties:
            self._properties = properties + ("\n" + default_properties if default_properties else "")
        else:
            self._properties = default_properties

    def to_dict(self):
        return {
            "amMemory": self._am_memory,
            "amCores": self._am_cores,
            "spark.executor.memory": self._executor_memory,
            "spark.executor.cores": self._executor_cores,
            "spark.executor.instances": self._executor_instances,
            "spark.dynamicAllocation.enabled": self._dynamic_allocation,
            "spark.dynamicAllocation.minExecutors": self._dynamic_min_executors,
            "spark.dynamicAllocation.maxExecutors": self._dynamic_max_executors,
            "properties": self._properties,
            "type": JobConfiguration.DTO_TYPE,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

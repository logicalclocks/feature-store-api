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


class JobConfiguration:

    PYSPARK_MAIN_CLASS = "org.apache.spark.deploy.PythonRunner"

    def __init__(
        self,
        app_path,
        default_args,
        am_memory=1024,
        am_cores=1,
        main_class=JobConfiguration.PYSPARK_MAIN_CLASS,
        executor_memory=2048,
        executor_cores=2,
        executor_instances=1,
        dynamic_allocation=True,
        dynamic_min_executors=1,
        dynamic_max_executors=5,
    ):
        self._default_args = default_args
        self._app_path = app_path
        self._am_memory = am_memory
        self._am_cores = am_cores
        self._main_class = main_class
        self._executor_memory = executor_memory
        self._executor_cores = executor_cores
        self._executor_instances = executor_instances
        self._dynamic_allocation = dynamic_allocation
        self._dynamic_min_executors = dynamic_max_executors
        self._dynamic_max_executors = dynamic_max_executors

    def to_dict():
        return {}

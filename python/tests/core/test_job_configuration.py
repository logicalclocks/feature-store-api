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


from hsfs.core import job_configuration


class TestJobConfiguration:
    def test_to_dict_defaults(self):
        # Arrange
        job_config = job_configuration.JobConfiguration()

        # Act
        result_dict = job_config.to_dict()

        # Assert
        expected_dict = {
            "amMemory": 2048,
            "amCores": 1,
            "spark.executor.memory": 4096,
            "spark.executor.cores": 1,
            "spark.executor.instances": 1,
            "spark.dynamicAllocation.enabled": True,
            "spark.dynamicAllocation.minExecutors": 1,
            "spark.dynamicAllocation.maxExecutors": 2,
            "type": job_configuration.JobConfiguration.DTO_TYPE,
        }
        assert expected_dict == result_dict

    def test_to_dict_non_defaults(self):
        # Arrange
        job_config = job_configuration.JobConfiguration(
            am_memory=4096,
            am_cores=2,
            executor_memory=8192,
            executor_cores=2,
            executor_instances=2,
            dynamic_allocation=False,
            dynamic_min_executors=2,
            dynamic_max_executors=4,
        )

        # Act
        result_dict = job_config.to_dict()

        # Assert
        expected_dict = {
            "amMemory": 4096,
            "amCores": 2,
            "spark.executor.memory": 8192,
            "spark.executor.cores": 2,
            "spark.executor.instances": 2,
            "spark.dynamicAllocation.enabled": False,
            "spark.dynamicAllocation.minExecutors": 2,
            "spark.dynamicAllocation.maxExecutors": 4,
            "type": job_configuration.JobConfiguration.DTO_TYPE,
        }
        assert expected_dict == result_dict

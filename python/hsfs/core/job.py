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

import humps
from hsfs import engine
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import job_api


class Job:
    def __init__(
        self,
        id,
        name,
        creation_time,
        config,
        job_type,
        creator,
        executions=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._executions = executions
        self._href = href
        self._config = config

        self._job_api = job_api.JobApi()

    @classmethod
    def from_response_json(cls, json_dict):
        # Job config should not be decamelized when updated
        config = json_dict.pop("config")
        json_decamelized = humps.decamelize(json_dict)
        json_decamelized["config"] = config
        return cls(**json_decamelized)

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._id

    @property
    def executions(self):
        return self._executions

    @property
    def href(self):
        return self._href

    @property
    def config(self):
        """Configuration for the job"""
        return self._config

    def run(self, args: str = None, await_termination: bool = True):
        """Run the job.

        Runs the job, by default awaiting its completion.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instances
            fg = fs.get_or_create_feature_group(...)

            # insert in to feature group
            job, _ = fg.insert(df, write_options={"start_offline_materialization": False})

            # run job
            job.run()
            ```

        # Arguments
            args: Optional runtime arguments for the job.
            await_termination: Identifies if the client should wait for the job to complete, defaults to True.
        """
        print(f"Launching job: {self.name}")
        self._job_api.launch(self.name, args=args)
        print(
            "Job started successfully, you can follow the progress at \n{}".format(
                engine.get_instance().get_job_url(self.href)
            )
        )
        engine.get_instance().wait_for_job(self, await_termination=await_termination)

    def get_state(self):
        """Get the state of the job.

        # Returns
            `state`. Current state of the job, which can be one of the following:
            `INITIALIZING`, `INITIALIZATION_FAILED`, `FINISHED`, `RUNNING`, `ACCEPTED`,
            `FAILED`, `KILLED`, `NEW`, `NEW_SAVING`, `SUBMITTED`, `AGGREGATING_LOGS`,
            `FRAMEWORK_FAILURE`, `STARTING_APP_MASTER`, `APP_MASTER_START_FAILED`,
            `GENERATING_SECURITY_MATERIAL`, `CONVERTING_NOTEBOOK`
        """
        last_execution = self._job_api.last_execution(self)
        if len(last_execution) != 1:
            raise FeatureStoreException("No executions found for job")

        return last_execution[0].state

    def get_final_state(self):
        """Get the final state of the job.

        # Returns
            `final_state`. Final state of the job, which can be one of the following:
            `UNDEFINED`, `FINISHED`, `FAILED`, `KILLED`, `FRAMEWORK_FAILURE`,
            `APP_MASTER_START_FAILED`, `INITIALIZATION_FAILED`. `UNDEFINED` indicates
             that the job is still running.
        """
        last_execution = self._job_api.last_execution(self)
        if len(last_execution) != 1:
            raise FeatureStoreException("No executions found for job")

        return last_execution[0].final_status

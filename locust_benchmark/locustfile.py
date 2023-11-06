import random

from common.hopsworks_client import HopsworksClient
from common.stop_watch import stopwatch
from locust import User, task, constant, events, constant_throughput
from locust.runners import MasterRunner, LocalRunner


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print("Locust process init")

    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        # create feature view
        environment.hopsworks_client = HopsworksClient(environment)
        fg = environment.hopsworks_client.get_or_create_fg(fg_name=environment.hopsworks_client.feature_group_name,
                                                           pk_id=environment.hopsworks_client.primary_key)
        environment.hopsworks_client.get_or_create_fv(fg=fg, fv_name=environment.hopsworks_client.feature_view_name)


@events.quitting.add_listener
def on_locust_quitting(environment, **kwargs):
    print("Locust process quit")
    if isinstance(environment.runner, MasterRunner):
        # clean up
        if environment.hopsworks_client.clean_fv:
            environment.hopsworks_client.get_or_create_fv(fv_name=environment.client.feature_view_name).delete()
        environment.hopsworks_client.close()


class FeatureVectorLookup(User):
    wait_time = constant(0.1)
    weight = 5
    # fixed_count = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv = self.client.get_or_create_fv(self.client.feature_view_name)

    def on_start(self):
        print("Init user")
        self.fv.init_serving(external=self.client.external)

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def get_feature_vector(self):
        self._get_feature_vector({self.client.primary_key: random.randint(0, self.client.rows - 1)})


    @stopwatch
    def _get_feature_vector(self, pk):
        self.fv.get_feature_vector(pk)


class FeatureVectorBatchLookup(User):
    wait_time = constant(0)
    weight = 1
    # fixed_count = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv = self.client.get_or_create_fv(fv_name=self.client.feature_view_name)

    def on_start(self):
        print("Init user")
        self.fv.init_serving(external=self.client.external)

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def get_feature_vector_batch(self):
        pks = [
            {self.client.primary_key: random.randint(0, self.client.rows - 1)}
            for i in range(self.client.batch_size)
        ]
        features = self._get_feature_vectors(pks)

    @stopwatch
    def _get_feature_vectors(self, pk):
        return self.fv.get_feature_vectors(pk)
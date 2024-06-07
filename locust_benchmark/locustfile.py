import random

from common.hopsworks_client import HopsworksClient
from common.stop_watch import stopwatch
from locust import HttpUser, User, task, constant, events
from locust.runners import MasterRunner, LocalRunner
from urllib3 import PoolManager
import nest_asyncio


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print("Locust process init")

    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        # create feature view
        environment.hopsworks_client = HopsworksClient(environment)
        fg = environment.hopsworks_client.get_or_create_fg()
        environment.hopsworks_client.get_or_create_fv(fg)


@events.quitting.add_listener
def on_locust_quitting(environment, **kwargs):
    print("Locust process quit")
    if isinstance(environment.runner, MasterRunner):
        # clean up
        environment.hopsworks_client.get_or_create_fv(None).delete()
        environment.hopsworks_client.close()


class RESTFeatureVectorLookup(HttpUser):
    wait_time = constant(0)
    weight = 5
    pool_manager = PoolManager(maxsize=10, block=True)

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.hopsworks_client = HopsworksClient(environment)
        self.fv = self.hopsworks_client.get_or_create_fv()

        with open(".api_key", "r") as f:
            self.headers = {"X-API-KEY": f.read().strip()}

    def on_stop(self):
        print("Closing user")
        self.hopsworks_client.close()

    @task
    def get_feature_vector(self):
        data = {
            "featureStoreName": self.hopsworks_client.hopsworks_config.get(
                "project", "test"
            ),
            "featureViewName": self.fv.name,
            "featureViewVersion": self.fv.version,
            "entries": {"ip": random.randint(0, self.hopsworks_client.rows - 1)},
            "metadataOptions": {"featureName": False, "featureType": False},
        }
        self.client.post("/0.1.0/feature_store", json=data, headers=self.headers)


class MySQLFeatureVectorLookup(User):
    wait_time = constant(0)
    weight = 5
    # fixed_count = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv = self.client.get_or_create_fv()

    def on_start(self):
        print("Init user")
        self.fv.init_serving(external=self.client.external)
        nest_asyncio.apply()

    def on_stop(self):
        print("Closing user")

    @task
    def get_feature_vector(self):
        self._get_feature_vector({"ip": random.randint(0, self.client.rows - 1)})

    @stopwatch
    def _get_feature_vector(self, pk):
        self.fv.get_feature_vector(pk)


class MySQLFeatureVectorBatchLookup(User):
    wait_time = constant(0)
    weight = 1
    # fixed_count = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv = self.client.get_or_create_fv()

    def on_start(self):
        print("Init user")
        self.fv.init_serving(external=self.client.external)
        nest_asyncio.apply()

    def on_stop(self):
        print("Closing user")

    @task
    def get_feature_vector_batch(self):
        pks = [
            {"ip": random.randint(0, self.client.rows - 1)}
            for i in range(self.client.batch_size)
        ]
        self._get_feature_vectors(pks)

    @stopwatch
    def _get_feature_vectors(self, pk):
        self.fv.get_feature_vectors(pk)

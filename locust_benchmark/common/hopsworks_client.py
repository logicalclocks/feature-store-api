import datetime
import random
import string
import json

import numpy as np
import pandas as pd

from locust.runners import MasterRunner, LocalRunner
import hsfs

from hsfs import client
from hsfs.client.exceptions import RestAPIError


class HopsworksClient:
    def __init__(self, environment=None):
        with open("hopsworks_config.json") as json_file:
            self.hopsworks_config = json.load(json_file)
        if environment is None or isinstance(
            environment.runner, (MasterRunner, LocalRunner)
        ):
            print(self.hopsworks_config)
        self.connection = hsfs.connection(
            project=self.hopsworks_config.get("project", "test"),
            host=self.hopsworks_config.get("host", "localhost"),
            port=self.hopsworks_config.get("port", 443),
            api_key_file=".api_key",
            secrets_store="local",
        )
        self.fs = self.connection.get_feature_store()

        # test settings
        self.external = self.hopsworks_config.get("external", False)
        self.rows = self.hopsworks_config.get("rows", 1_000_000)
        self.schema_repetitions = self.hopsworks_config.get("schema_repetitions", 1)
        self.recreate_feature_group = self.hopsworks_config.get(
            "recreate_feature_group", False
        )
        self.batch_size = self.hopsworks_config.get("batch_size", 100)

    def get_or_create_fg(self):
        locust_fg = self.fs.get_or_create_feature_group(
            name="locust_fg",
            version=1,
            primary_key=["ip"],
            online_enabled=True,
            stream=True,
        )
        return locust_fg

    def insert_data(self, locust_fg):
        if locust_fg.id is not None and self.recreate_feature_group:
            locust_fg.delete()
            locust_fg = self.get_or_create_fg()
        if locust_fg.id is None:
            df = self.generate_insert_df(self.rows, self.schema_repetitions)
            locust_fg.insert(df, write_options={"internal_kafka": not self.external})
        return locust_fg

    def get_or_create_fv(self, fg=None):
        try:
            return self.fs.get_feature_view("locust_fv", version=1)
        except RestAPIError:
            return self.fs.create_feature_view(
                name="locust_fv",
                query=fg.select_all(),
                version=1,
            )

    def close(self):
        if client._client is not None:
            self.connection.close()

    def generate_insert_df(self, rows, schema_repetitions):
        data = {"ip": range(0, rows)}
        df = pd.DataFrame.from_dict(data)

        for i in range(0, schema_repetitions):
            df["rand_ts_1_" + str(i)] = datetime.datetime.now()
            df["rand_ts_2_" + str(i)] = datetime.datetime.now()
            df["rand_int_1" + str(i)] = np.random.randint(0, 100000)
            df["rand_int_2" + str(i)] = np.random.randint(0, 100000)
            df["rand_float_1" + str(i)] = np.random.uniform(low=0.0, high=1.0)
            df["rand_float_2" + str(i)] = np.random.uniform(low=0.0, high=1.0)
            df["rand_string_1" + str(i)] = "".join(
                random.choices(string.ascii_lowercase, k=5)
            )
            df["rand_string_2" + str(i)] = "".join(
                random.choices(string.ascii_lowercase, k=5)
            )
            df["rand_string_3" + str(i)] = "".join(
                random.choices(string.ascii_lowercase, k=5)
            )
            df["rand_string_4" + str(i)] = "".join(
                random.choices(string.ascii_lowercase, k=5)
            )

        return df

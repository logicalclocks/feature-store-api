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


import humps
import json

from hsfs import util, engine, feature, storage_connector as sc
from hsfs.core import on_demand_feature_group_engine, feature_group_base


class OnDemandFeatureGroup(feature_group_base.FeatureGroupBase):
    ON_DEMAND_FEATURE_GROUP = "ON_DEMAND_FEATURE_GROUP"

    def __init__(
        self,
        query,
        storage_connector,
        name=None,
        version=None,
        description=None,
        featurestore_id=None,
        featurestore_name=None,
        created=None,
        creator=None,
        id=None,
        features=None,
        jobs=None,
        desc_stats_enabled=None,
        feat_corr_enabled=None,
        feat_hist_enabled=None,
        cluster_analysis_enabled=None,
        statistic_columns=None,
        statistics_config=None,
    ):
        super().__init__(featurestore_id)

        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = creator
        self._version = version
        self._name = name
        self._query = query
        self._id = id
        self._jobs = jobs
        self._desc_stats_enabled = desc_stats_enabled
        self._feat_corr_enabled = feat_corr_enabled
        self._feat_hist_enabled = feat_hist_enabled
        self._statistic_columns = statistic_columns

        self._feature_group_engine = on_demand_feature_group_engine.OnDemandFeatureGroupEngine(
            featurestore_id
        )

        if self._id:
            # Got from Hopsworks, deserialize features and storage connector
            self._features = [
                feature.Feature.from_response_json(feat) for feat in features
            ]
        else:
            self._features = features

        if storage_connector is not None and isinstance(storage_connector, dict):
            self._storage_connector = sc.StorageConnector.from_response_json(
                storage_connector
            )
        else:
            self._storage_connector = storage_connector

    def save(self):
        self._feature_group_engine.save(self)

    def read(self, dataframe_type="default"):
        """Get the feature group as a DataFrame."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().read(dataframe_type)

    def show(self, n):
        """Show the first n rows of the feature group."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().show(n)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "query": self._query,
            "storageConnector": self._storage_connector.to_dict(),
            "type": "onDemandFeaturegroupDTO",
        }

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def description(self):
        return self._description

    @property
    def features(self):
        return self._features

    @property
    def query(self):
        return self._query

    @property
    def storage_connector(self):
        return self._storage_connector

    @property
    def creator(self):
        return self._creator

    @property
    def created(self):
        return self._created

    @version.setter
    def version(self, version):
        self._version = version

    @description.setter
    def description(self, new_description):
        self._description = new_description

    @features.setter
    def features(self, new_features):
        self._features = new_features

#
#   Copyright 2023 Logical Clocks AB
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

from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor.join import Join
from hsfs.feature import Feature
from hsfs import client
from hsfs.client.external import Client

class VectorDbClient:

    def __init__(self, query):
        self._opensearch_client = None
        self._query = query
        self._embedding_features = {}
        self._fg_vdb_col_fg_col_map = {}
        self._fg_vdb_col_td_col_map = {}
        self._fg_fg_col_vdb_col_map = {}
        self._fg_embedding_map = {}
        self._embedding_fg_by_join_index = {}
        self.init()

    def init(self):
        self._setup_opensearch_client()
        for fg in self._query.featuregroups:
            if fg.embedding:
                for feat in fg.embedding.get_features():
                    for fgf in fg.features:
                        if fgf.name == feat.name and fgf.feature_group_id == fg.id:
                            self._embedding_features[fgf] = feat
        for q in [self._query] + [j.query for j in self._query.joins]:
            fg = q._left_feature_group
            if fg.embedding:
                vdb_col_fg_col_map = {}
                fg_col_vdb_col_map = {}
                for f in q._left_features:
                    vdb_col_fg_col_map[fg.embedding.col_prefix + f.name] = f
                    fg_col_vdb_col_map[f.name] = fg.embedding.col_prefix + f.name
                # add primary key to the map in case it is not selected as feature
                for pk in q._left_feature_group.primary_key:
                    vdb_col_fg_col_map[fg.embedding.col_prefix + pk] = q._left_feature_group[pk]
                    fg_col_vdb_col_map[pk] = fg.embedding.col_prefix + pk
                self._fg_vdb_col_fg_col_map[fg.id] = vdb_col_fg_col_map
                self._fg_fg_col_vdb_col_map[fg.id] = fg_col_vdb_col_map
                self._fg_embedding_map[fg.id] = fg.embedding

        # create a join for the left fg so that the dict can be constructed in one loop
        fg_joins = [Join(self._query, None, None, None, None, "")
                    ] + self._query.joins
        # join in dex start from 0, 0 means left fg
        for i, join in enumerate(fg_joins):
            join_fg = join.query._left_feature_group
            if join_fg.embedding:
                if join_fg.id in self._fg_vdb_col_td_col_map:
                    # `self._fg_vdb_col_td_col_map` do not support join of same fg
                    raise FeatureStoreException(
                        "Do not support join of same fg multiple times.")
                self._embedding_fg_by_join_index[i] = join_fg
                vdb_col_td_col_map = {}
                for feat in join_fg.features:
                    vdb_col_td_col_map[
                        join_fg.embedding.col_prefix + feat.name] = (
                        (join.prefix or "") + feat.name
                    ) # join.prefix can be None
                self._fg_vdb_col_td_col_map[
                    join_fg.id] = vdb_col_td_col_map

    # TODO: opensearch client should be singleton
    def _setup_opensearch_client(self):
        if not self._opensearch_client:
            try:
                import hopsworks
                from opensearchpy import OpenSearch
            except:
                raise ModuleNotFoundError(
                    "hopsworks or opensearchpy is not installed"
                )
            if not hopsworks._connected_project:
                if isinstance(client.get_instance(), Client):
                    hopsworks.login(
                        host=client.get_instance().host,
                        port=client.get_instance()._port,
                        project=client.get_instance()._project_name,
                        api_key_value=client.get_instance()._auth._token,
                    )
                else:
                    hopsworks.login()
            opensearch_api = hopsworks._connected_project.get_opensearch_api()
            self._opensearch_client = OpenSearch(**opensearch_api.get_default_py_config())

    def _refresh_opensearch_connection(self):
        self._opensearch_client.close()
        self._opensearch_client = None
        self._setup_opensearch_client()

    def _search_opensearch(self, index=None, body=None):
        try:
            return self._opensearch_client.search(
                body=body,
                index=index
            )
        except:
            self._refresh_opensearch_connection()
            return self._opensearch_client.search(
                body=body,
                index=index
            )

    def find_neighbors(self, embedding, feature: Feature = None,
                       index_name=None, k=10, filter=None, min_score=0):
        if not feature:
            if not self._embedding_features:
                raise ValueError("embedding col is not defined.")
            if len(self._embedding_features) > 1:
                raise ValueError(
                    "More than 1 embedding columns but col is not defined")
            embedding_feature = list(self._embedding_features.values())[0]
        else:
            embedding_feature = self._embedding_features.get(feature, None)
            if embedding_feature is None:
                raise ValueError(
                    f"feature: {feature.name} is not an embedding feature."
                )
        col_name = (
            embedding_feature.embedding.col_prefix + embedding_feature.name
        )
        query = {
            "size": k,
            "query": {

                "bool": {
                    "must": [
                        {"knn": {
                                   col_name: {
                                       "vector": embedding,
                                       "k": k
                                   }
                               }
                        },
                        {
                            "exists": {
                                "field": col_name
                            }
                        }
                    ]
        }

            },
            "_source": list(self._fg_vdb_col_fg_col_map.get(
                embedding_feature.feature_group.id).keys())
        }

        if not index_name:
            index_name = embedding_feature.embedding.index_name

        results = self._search_opensearch(
            body=query,
            index=index_name
        )

        # https://opensearch.org/docs/latest/search-plugins/knn/approximate-knn/#spaces
        return [
            (1/item['_score'] - 1,
             self._rewrite_result_key(
                 item['_source'],
                 self._fg_vdb_col_td_col_map[embedding_feature.feature_group.id]
             )
             ) for item in results['hits']['hits']]

    def _rewrite_result_key(self, result_map, key_map):
        new_map = {}
        for key in result_map.keys():
            new_key = key_map.get(key)
            if new_key is None:
                raise FeatureStoreException(
                    f"Feature '{key}' from embedding feature group is not found in the query"
                )
            new_map[new_key] = result_map[key]
        return new_map

    def read(self, fg_id, keys=None, pk=None, index_name=None, n=10):
        if fg_id not in self._fg_vdb_col_fg_col_map:
            raise FeatureStoreException("Provided fg does not have embedding.")
        if not index_name:
            index_name = self._get_vector_db_index_name(fg_id)
        if keys:
            query = {
                    "query": {
                        "match": self._rewrite_result_key(keys, self._fg_fg_col_vdb_col_map[fg_id])
                    },
                }
        else:
            if not pk:
                raise FeatureStoreException("No pk provided.")
            query = {
                "query": {
                    "bool": {
                        "must": {
                            "exists": {
                                "field": pk
                            }
                        }
                    }
                },
                "size": n,
            }

        query["_source"] = list(self._fg_vdb_col_fg_col_map.get(fg_id).keys())
        results = self._search_opensearch(
            body=query,
            index=index_name
        )
        # https://opensearch.org/docs/latest/search-plugins/knn/approximate-knn/#spaces
        return [self._rewrite_result_key(item['_source'], self._fg_vdb_col_td_col_map[fg_id]) for item in results['hits']['hits']]

    def _get_vector_db_index_name(self, fg_id):
        embedding = self._fg_embedding_map.get(fg_id)
        if embedding is None:
            raise ValueError("No embedding fg available.")
        return embedding.index_name

    @property
    def embedding_fg_by_join_index(self):
        return self._embedding_fg_by_join_index

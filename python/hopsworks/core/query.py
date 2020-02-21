import json

from hopsworks import util, engine
from hopsworks.core import join


class Query:
    def __init__(self, query_constructor_api, left_feature_group, left_features):
        self._left_feature_group = left_feature_group
        self._left_features = util.parse_features(left_features)
        self._joins = []
        self._query_constructor_api = query_constructor_api

    def read(self, dataframe_type="default"):
        sql_query = self._query_constructor_api.construct_query(self)["query"]
        return engine.get_instance().sql(sql_query, dataframe_type)

    def show(self, n):
        sql_query = self._query_constructor_api.construct_query(self)["query"]
        return engine.get_instance().show(sql_query, n)

    def join(self, sub_query, on=[], left_on=[], right_on=[], join_type="inner"):
        self._joins.append(
            join.Join(sub_query, on, left_on, right_on, join_type.upper())
        )
        return self

    def json(self):
        return json.dumps(self, cls=util.QueryEncoder)

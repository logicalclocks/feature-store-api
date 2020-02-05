import json

from hopsworks import util, engine


class Query:
    def __init__(
        self, query_constructor_api, left_feature_group, left_features, joins=None
    ):
        self._left_feature_group = left_feature_group
        self._left_features = left_features
        self._joins = joins
        self._query_constructor_api = query_constructor_api

    def read(self):
        sql_query = self._query_constructor_api.construct_query(self)["query"]
        return engine.get_instance().sql(sql_query)

    def show(self, n):
        sql_query = self._query_constructor_api.construct_query(self)["query"]
        return engine.get_instance().show(sql_query, n)

    def json(self):
        return json.dumps(self, cls=util.QueryEncoder)

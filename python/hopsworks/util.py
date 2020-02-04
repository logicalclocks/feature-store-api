import json
import functools


from hopsworks import feature_group, feature, connection
from hopsworks.core import query


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class QueryEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, feature.Feature):
            return {"name": o._name}
        elif isinstance(o, feature_group.FeatureGroup):
            return {"id": o._id}
        elif isinstance(o, query.Query):
            return {
                "leftFeatureGroup": o._left_feature_group,
                "leftFeatures": o._left_features,
                # "joins": o._joins,
            }
        else:
            return super().default(o)


def not_connected(fn):
    @functools.wraps(fn)
    def if_not_connected(inst, *args, **kwargs):
        if inst._connected:
            raise connection.HopsworksConnectionError
        return fn(inst, *args, **kwargs)

    return if_not_connected


def connected(fn):
    @functools.wraps(fn)
    def if_connected(inst, *args, **kwargs):
        if not inst._connected:
            raise connection.NoHopsworksConnectionError
        return fn(inst, *args, **kwargs)

    return if_connected

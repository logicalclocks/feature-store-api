import json


from hopsworks import feature


class FeatureStoreEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            return o.to_dict()
        except AttributeError:
            return super().default(o)


def validate_feature(ft):
    if isinstance(ft, feature.Feature):
        return ft
    elif isinstance(ft, str):
        return feature.Feature(ft)


def parse_features(feature_names):
    if isinstance(feature_names, (str, feature.Feature)):
        return [validate_feature(feature_names)]
    elif isinstance(feature_names, list) and len(feature_names) > 0:
        return [validate_feature(feat) for feat in feature_names]
    else:
        return []

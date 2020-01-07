import json
import humps


class FeatureGroup:
    def __init__(self):
        pass

    def sample(self, num_rows=None):
        pass

    def features(self, feature_list=["*"]):
        pass

    @classmethod
    def from_json(cls, json_str):
        json_dict = json.loads(json_str)
        # Json is coming from Java, convert it from camel case to snake case
        json_dec = humps.decamelize(json_dict)
        return cls(**json_dec)

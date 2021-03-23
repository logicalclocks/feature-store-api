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
#

import json
from datetime import datetime

from hsfs import feature


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
    elif isinstance(ft, dict):
        return feature.Feature(**ft)


def parse_features(feature_names):
    if isinstance(feature_names, (str, feature.Feature)):
        return [validate_feature(feature_names)]
    elif isinstance(feature_names, list) and len(feature_names) > 0:
        return [validate_feature(feat) for feat in feature_names]
    else:
        return []


def feature_group_name(feature_group):
    return feature_group.name + "_" + str(feature_group.version)


def get_hudi_datestr_from_timestamp(timestamp):
    date_obj = datetime.fromtimestamp(timestamp / 1000)
    date_str = date_obj.strftime("%Y%m%d%H%M%S")
    return date_str


class VersionWarning(Warning):
    pass


class StorageWarning(Warning):
    pass

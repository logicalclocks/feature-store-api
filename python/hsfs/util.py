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

import os
import json

from pathlib import Path
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


def parse_features(feature_names):
    if isinstance(feature_names, (str, feature.Feature)):
        return [validate_feature(feature_names)]
    elif isinstance(feature_names, list) and len(feature_names) > 0:
        return [validate_feature(feat) for feat in feature_names]
    else:
        return []


def get_cert_pw():
    """
    Get keystore password from local container

    Returns:
        Certificate password
    """
    hadoop_user_name = "hadoop_user_name"
    crypto_material_password = "material_passwd"
    material_directory = "MATERIAL_DIRECTORY"
    password_suffix = "__cert.key"
    pwd_path = Path(crypto_material_password)
    if not pwd_path.exists():
        username = os.environ[hadoop_user_name]
        material_directory = Path(os.environ[material_directory])
        pwd_path = material_directory.joinpath(username + password_suffix)

    with pwd_path.open() as f:
        return f.read()


class VersionWarning(Warning):
    pass


class StorageWarning(Warning):
    pass

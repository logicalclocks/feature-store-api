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
import re
import json

from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine

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


def get_cert_pw():
    """
    Get keystore password from local container

    Returns:
        Certificate password
    """
    hadoop_user_name = "HADOOP_USER_NAME"
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


def create_mysql_connection(online_conn):
    online_options = online_conn.spark_options()
    # Here we are replacing the first part of the string returned by Hopsworks,
    # jdbc:mysql:// with the sqlalchemy one + username and password
    # useSSL and allowPublicKeyRetrieval are not valid properties for the pymysql driver
    # to use SSL we'll have to something like this:
    # ssl_args = {'ssl_ca': ca_path}
    # engine = create_engine("mysql+pymysql://<user>:<pass>@<addr>/<schema>", connect_args=ssl_args)
    sql_alchemy_conn_str = (
        online_options["url"]
        .replace(
            "jdbc:mysql://",
            "mysql+pymysql://"
            + online_options["user"]
            + ":"
            + online_options["password"]
            + "@",
        )
        .replace("useSSL=false&", "")
        .replace("?allowPublicKeyRetrieval=true", "")
    )

    sql_alchemy_engine = create_engine(sql_alchemy_conn_str, pool_recycle=3600)
    return sql_alchemy_engine.connect()


def get_timestamp_from_date_string(input_date):
    date_format_patterns = {
        r"^([0-9]{4})([0-9]{2})([0-9]{2})$": "%Y%m%d",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M%S",
    }
    input_date = (
        input_date.replace("/", "").replace("-", "").replace(" ", "").replace(":", "")
    )
    date_format = None

    for pattern in date_format_patterns:
        date_format_pattern = re.match(pattern, input_date)
        if date_format_pattern:
            break

    if date_format in None:
        raise ("Unable to identify format of the provided date value : " + input_date)

    return int(float(datetime.strptime(input_date, date_format).timestamp()) * 1000)


class VersionWarning(Warning):
    pass


class StorageWarning(Warning):
    pass

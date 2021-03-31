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

import re
import json

from datetime import datetime
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
            date_format = date_format_patterns[pattern]
            break

    if date_format is None:
        raise ValueError(
            "Unable to identify format of the provided date value : " + input_date
        )

    return int(float(datetime.strptime(input_date, date_format).timestamp()) * 1000)


def get_hudi_datestr_from_timestamp(timestamp):
    date_obj = datetime.fromtimestamp(timestamp / 1000)
    date_str = date_obj.strftime("%Y%m%d%H%M%S")
    return date_str


class VersionWarning(Warning):
    pass


class StorageWarning(Warning):
    pass


class StatisticsWarning(Warning):
    pass


class ValidationWarning(Warning):
    pass

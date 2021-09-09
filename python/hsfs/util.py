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

from hsfs import client, feature


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


def create_mysql_engine(online_conn, external):
    online_options = online_conn.spark_options()
    # Here we are replacing the first part of the string returned by Hopsworks,
    # jdbc:mysql:// with the sqlalchemy one + username and password
    # useSSL and allowPublicKeyRetrieval are not valid properties for the pymysql driver
    # to use SSL we'll have to something like this:
    # ssl_args = {'ssl_ca': ca_path}
    # engine = create_engine("mysql+pymysql://<user>:<pass>@<addr>/<schema>", connect_args=ssl_args)
    if external:
        # This only works with external clients.
        # Hopsworks clients should use the storage connector
        online_options["url"] = re.sub(
            "/[0-9.]+:",
            "/{}:".format(client.get_instance().host),
            online_options["url"],
        )

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

    # default connection pool size kept by engine is 5
    sql_alchemy_engine = create_engine(sql_alchemy_conn_str, pool_recycle=3600)
    return sql_alchemy_engine


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


def setup_pydoop():
    # Import Pydoop only here, so it doesn't trigger if the execution environment
    # does not support Pydoop. E.g. Sagemaker
    from pydoop import hdfs

    # Create a subclass that replaces the check on the hdfs scheme to allow hopsfs as well.
    class _HopsFSPathSplitter(hdfs.path._HdfsPathSplitter):
        @classmethod
        def split(cls, hdfs_path, user):
            if not hdfs_path:
                cls.raise_bad_path(hdfs_path, "empty")
            scheme, netloc, path = cls.parse(hdfs_path)
            if not scheme:
                scheme = "file" if hdfs.fs.default_is_local() else "hdfs"
            if scheme == "hdfs" or scheme == "hopsfs":
                if not path:
                    cls.raise_bad_path(hdfs_path, "path part is empty")
                if ":" in path:
                    cls.raise_bad_path(hdfs_path, "':' not allowed outside netloc part")
                hostname, port = cls.split_netloc(netloc)
                if not path.startswith("/"):
                    path = "/user/%s/%s" % (user, path)
            elif scheme == "file":
                hostname, port, path = "", 0, netloc + path
            else:
                cls.raise_bad_path(hdfs_path, "unsupported scheme %r" % scheme)
            return hostname, port, path

    # Monkey patch the class to use the one defined above.
    hdfs.path._HdfsPathSplitter = _HopsFSPathSplitter


class VersionWarning(Warning):
    pass


class StorageWarning(Warning):
    pass


class StatisticsWarning(Warning):
    pass


class ValidationWarning(Warning):
    pass

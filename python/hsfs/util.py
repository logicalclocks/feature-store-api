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
import pandas as pd

from datetime import datetime, date, timezone
from urllib.parse import urljoin, urlparse

from sqlalchemy import create_engine

from hsfs import client, feature
from hsfs.client import exceptions


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


def rewrite_feature_store_name(name):
    FEATURE_STORE_NAME_SUFFIX = "_featurestore"
    name = name.lower()
    if name.endswith(FEATURE_STORE_NAME_SUFFIX):
        return name
    else:
        return name + FEATURE_STORE_NAME_SUFFIX


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


def check_timestamp_format_from_date_string(input_date):
    date_format_patterns = {
        r"^([0-9]{4})([0-9]{2})([0-9]{2})$": "%Y%m%d",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M%S",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{3})$": "%Y%m%d%H%M%S%f",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{6})Z$": "ISO",
    }
    normalized_date = (
        input_date.replace("/", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(":", "")
        .replace(".", "")
    )

    date_format = None
    for pattern in date_format_patterns:
        date_format_pattern = re.match(pattern, normalized_date)
        if date_format_pattern:
            date_format = date_format_patterns[pattern]
            break

    if date_format is None:
        raise ValueError(
            "Unable to identify format of the provided date value : " + input_date
        )

    return normalized_date, date_format


def get_timestamp_from_date_string(input_date):
    norm_input_date, date_format = check_timestamp_format_from_date_string(input_date)
    try:
        if date_format != "ISO":
            date_time = datetime.strptime(norm_input_date, date_format)
        else:
            date_time = datetime.fromisoformat(input_date[:-1])
    except ValueError:
        raise ValueError(
            "Unable to parse the normalized input date value : "
            + norm_input_date
            + " with format "
            + date_format
        )
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=timezone.utc)
    return int(float(date_time.timestamp()) * 1000)


def get_hudi_datestr_from_timestamp(timestamp):
    return datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y%m%d%H%M%S%f")[:-3]


def convert_event_time_to_timestamp(event_time):
    if not event_time:
        return None
    if isinstance(event_time, str):
        return get_timestamp_from_date_string(event_time)
    elif isinstance(event_time, pd._libs.tslibs.timestamps.Timestamp):
        # convert to unix epoch time in milliseconds.
        event_time = event_time.to_pydatetime()
        # convert to unix epoch time in milliseconds.
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return int(event_time.timestamp() * 1000)
    elif isinstance(event_time, datetime):
        # convert to unix epoch time in milliseconds.
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return int(event_time.timestamp() * 1000)
    elif isinstance(event_time, date):
        # convert to unix epoch time in milliseconds.
        event_time = datetime(*event_time.timetuple()[:7])
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return int(event_time.timestamp() * 1000)
    elif isinstance(event_time, int):
        if event_time == 0:
            raise ValueError("Event time should be greater than 0.")
        # jdbc supports timestamp precision up to second only.
        if len(str(event_time)) <= 10:
            event_time = event_time * 1000
        return event_time
    else:
        raise ValueError(
            "Given event time should be in `datetime`, `date`, `str` or `int` type"
        )


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


def get_hostname_replaced_url(sub_path: str):
    """
    construct and return an url with public hopsworks hostname and sub path
    :param self:
    :param sub_path: url sub-path after base url
    :return: href url
    """
    href = urljoin(client.get_instance()._base_url, sub_path)
    url_parsed = client.get_instance().replace_public_host(urlparse(href))
    return url_parsed.geturl()


def verify_attribute_key_names(feature_group_obj, external_feature_group=False):
    feature_names = set(feat.name for feat in feature_group_obj.features)
    if feature_group_obj.primary_key:
        diff = set(feature_group_obj.primary_key) - feature_names
        if diff:
            raise exceptions.FeatureStoreException(
                f"Provided primary key(s) {','.join(diff)} doesn't exist in feature dataframe"
            )

    if feature_group_obj.event_time:
        if feature_group_obj.event_time not in feature_names:
            raise exceptions.FeatureStoreException(
                f"Provided event_time feature {feature_group_obj.event_time} doesn't exist in feature dataframe"
            )

    if not external_feature_group:
        if feature_group_obj.partition_key:
            diff = set(feature_group_obj.partition_key) - feature_names
            if diff:
                raise exceptions.FeatureStoreException(
                    f"Provided partition key(s) {','.join(diff)} doesn't exist in feature dataframe"
                )

        if feature_group_obj.hudi_precombine_key:
            if feature_group_obj.hudi_precombine_key not in feature_names:
                raise exceptions.FeatureStoreException(
                    f"Provided hudi precombine key {feature_group_obj.hudi_precombine_key} "
                    f"doesn't exist in feature dataframe"
                )


def translate_legacy_spark_type(output_type):
    if output_type == "StringType()":
        return "STRING"
    elif output_type == "BinaryType()":
        return "BINARY"
    elif output_type == "ByteType()":
        return "BYTE"
    elif output_type == "ShortType()":
        return "SHORT"
    elif output_type == "IntegerType()":
        return "INT"
    elif output_type == "LongType()":
        return "LONG"
    elif output_type == "FloatType()":
        return "FLOAT"
    elif output_type == "DoubleType()":
        return "DOUBLE"
    elif output_type == "TimestampType()":
        return "TIMESTAMP"
    elif output_type == "DateType()":
        return "DATE"
    elif output_type == "BooleanType()":
        return "BOOLEAN"
    else:
        return "STRING"  # handle gracefully, and return STRING type, the default for spark udfs


class VersionWarning(Warning):
    pass


class JobWarning(Warning):
    pass


class StorageWarning(Warning):
    pass


class StatisticsWarning(Warning):
    pass


class ValidationWarning(Warning):
    pass


class FeatureGroupWarning(Warning):
    pass

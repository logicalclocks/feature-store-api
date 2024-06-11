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
from __future__ import annotations

import asyncio
import itertools
import json
import re
import sys
import threading
import time
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, Union
from urllib.parse import urljoin, urlparse

import numpy as np
import pandas as pd
from aiomysql.sa import create_engine as async_create_engine
from hsfs import client, feature, feature_group, serving_key
from hsfs.client import exceptions
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor import serving_prepared_statement
from hsfs.core import feature_group_api, variable_api
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url


FEATURE_STORE_NAME_SUFFIX = "_featurestore"


class FeatureStoreEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Dict[str, Any]:
        try:
            return o.to_dict()
        except AttributeError:
            return super().default(o)


def validate_feature(
    ft: Union[str, feature.Feature, Dict[str, Any]],
) -> feature.Feature:
    if isinstance(ft, feature.Feature):
        return ft
    elif isinstance(ft, str):
        return feature.Feature(ft)
    elif isinstance(ft, dict):
        return feature.Feature(**ft)


VALID_EMBEDDING_TYPE = {
    "array<int>",
    "array<bigint>",
    "array<float>",
    "array<double>",
}


def validate_embedding_feature_type(embedding_index, schema):
    if not embedding_index or not schema:
        return
    feature_type_map = dict([(feat.name, feat.type) for feat in schema])
    for embedding in embedding_index.get_embeddings():
        feature_type = feature_type_map.get(embedding.name)
        if feature_type not in VALID_EMBEDDING_TYPE:
            raise FeatureStoreException(
                f"Provide feature `{embedding.name}` has type `{feature_type}`, "
                f"but requires one of the following: {', '.join(VALID_EMBEDDING_TYPE)}"
            )


def parse_features(
    feature_names: Union[
        str, feature.Feature, List[Union[Dict[str, Any], str, feature.Feature]]
    ],
) -> List[feature.Feature]:
    if isinstance(feature_names, (str, feature.Feature)):
        return [validate_feature(feature_names)]
    elif isinstance(feature_names, list) and len(feature_names) > 0:
        return [validate_feature(feat) for feat in feature_names]
    else:
        return []


def autofix_feature_name(name: str) -> str:
    # replace spaces with underscores and enforce lower case
    return name.lower().replace(" ", "_")


def feature_group_name(
    feature_group: Union[
        feature_group.FeatureGroup,
        feature_group.ExternalFeatureGroup,
        feature_group.SpineGroup,
    ],
) -> str:
    return feature_group.name + "_" + str(feature_group.version)


def append_feature_store_suffix(name: str) -> str:
    name = name.lower()
    if name.endswith(FEATURE_STORE_NAME_SUFFIX):
        return name
    else:
        return name + FEATURE_STORE_NAME_SUFFIX


def strip_feature_store_suffix(name: str) -> str:
    name = name.lower()
    if name.endswith(FEATURE_STORE_NAME_SUFFIX):
        return name[: -1 * len(FEATURE_STORE_NAME_SUFFIX)]
    else:
        return name


def create_mysql_engine(
    online_conn: Any, external: bool, options: Optional[Dict[str, Any]] = None
) -> Any:
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
        host = get_host_name()
        online_options["url"] = re.sub(
            "/[0-9.]+:",
            "/{}:".format(host),
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
    if options is not None and not isinstance(options, dict):
        raise TypeError("`options` should be a `dict` type.")
    if not options:
        options = {"pool_recycle": 3600}
    elif "pool_recycle" not in options:
        options["pool_recycle"] = 3600
    # default connection pool size kept by engine is 5
    sql_alchemy_engine = create_engine(sql_alchemy_conn_str, **options)
    return sql_alchemy_engine


def get_host_name() -> str:
    host = variable_api.VariableApi().get_loadbalancer_external_domain()
    if host == "":
        # If the load balancer is not configured, then fall back to
        # use the MySQL node on the head node
        host = client.get_instance().host
    return host


def get_dataset_type(path: str) -> Literal["HIVEDB", "DATASET"]:
    if re.match(r"^(?:hdfs://|)/apps/hive/warehouse/*", path):
        return "HIVEDB"
    else:
        return "DATASET"


async def create_async_engine(
    online_conn: Any,
    external: bool,
    default_min_size: int,
    options: Optional[Dict[str, Any]] = None,
    hostname: Optional[str] = None,
) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as er:
        raise RuntimeError(
            "Event loop is not running. Please invoke this co-routine from a running loop or provide an event loop."
        ) from er

    online_options = online_conn.spark_options()
    # read the keys user, password from online_conn as use them while creating the connection pool
    url = make_url(online_options["url"].replace("jdbc:", ""))
    if hostname is None:
        if external:
            hostname = get_host_name()
        else:
            hostname = url.host

    # create a aiomysql connection pool
    pool = await async_create_engine(
        host=hostname,
        port=3306,
        user=online_options["user"],
        password=online_options["password"],
        db=url.database,
        loop=loop,
        minsize=(
            options.get("minsize", default_min_size) if options else default_min_size
        ),
        maxsize=(
            options.get("maxsize", default_min_size) if options else default_min_size
        ),
        pool_recycle=(options.get("pool_recycle", -1) if options else -1),
    )
    return pool


def check_timestamp_format_from_date_string(input_date: str) -> Tuple[str, str]:
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


def get_timestamp_from_date_string(input_date: str) -> int:
    norm_input_date, date_format = check_timestamp_format_from_date_string(input_date)
    try:
        if date_format != "ISO":
            date_time = datetime.strptime(norm_input_date, date_format)
        else:
            date_time = datetime.fromisoformat(input_date[:-1])
    except ValueError as err:
        raise ValueError(
            "Unable to parse the normalized input date value : "
            + norm_input_date
            + " with format "
            + date_format
        ) from err
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=timezone.utc)
    return int(float(date_time.timestamp()) * 1000)


def get_hudi_datestr_from_timestamp(timestamp: int) -> str:
    return datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y%m%d%H%M%S%f")[:-3]


def get_delta_datestr_from_timestamp(timestamp: int) -> str:
    return datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[
        :-3
    ]


def convert_event_time_to_timestamp(
    event_time: Optional[
        Union[str, pd._libs.tslibs.timestamps.Timestamp, datetime, date, int]
    ],
) -> Optional[int]:
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


def setup_pydoop() -> None:
    # Import Pydoop only here, so it doesn't trigger if the execution environment
    # does not support Pydoop. E.g. Sagemaker
    from pydoop import hdfs

    # Create a subclass that replaces the check on the hdfs scheme to allow hopsfs as well.
    class _HopsFSPathSplitter(hdfs.path._HdfsPathSplitter):
        @classmethod
        def split(
            cls, hdfs_path: Optional[str], user: Optional[str]
        ) -> Tuple[str, int, str]:
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


def get_hostname_replaced_url(sub_path: str) -> str:
    """
    construct and return an url with public hopsworks hostname and sub path
    :param self:
    :param sub_path: url sub-path after base url
    :return: href url
    """
    href = urljoin(client.get_instance()._base_url, sub_path)
    url_parsed = client.get_instance().replace_public_host(urlparse(href))
    return url_parsed.geturl()


def verify_attribute_key_names(
    feature_group_obj: Union[
        feature_group.FeatureGroup,
        feature_group.ExternalFeatureGroup,
        feature_group.SpineGroup,
    ],
    external_feature_group: bool = False,
) -> None:
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


def get_job_url(href: str) -> str:
    """Use the endpoint returned by the API to construct the UI url for jobs

    Args:
        href (str): the endpoint returned by the API
    """
    url = urlparse(href)
    url_splits = url.path.split("/")
    project_id = url_splits[4]
    job_name = url_splits[6]
    ui_url = url._replace(
        path="p/{}/jobs/named/{}/executions".format(project_id, job_name)
    )
    ui_url = client.get_instance().replace_public_host(ui_url)
    return ui_url.geturl()


def translate_legacy_spark_type(
    output_type: str,
) -> Literal[
    "STRING",
    "BINARY",
    "BYTE",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "TIMESTAMP",
    "DATE",
    "BOOLEAN",
]:
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


def _loading_animation(message: str, stop_event: threading.Event) -> None:
    for char in itertools.cycle([".", "..", "...", ""]):
        if stop_event.is_set():
            break
        print(f"{message}{char}   ", end="\r")
        time.sleep(0.5)


def run_with_loading_animation(message: str, func: Callable, *args, **kwargs) -> Any:
    stop_event = threading.Event()
    t = threading.Thread(
        target=_loading_animation,
        args=(
            message,
            stop_event,
        ),
    )
    t.daemon = True
    t.start()
    start = time.time()
    end = None

    try:
        result = func(*args, **kwargs)
        end = time.time()
        return result
    finally:
        # Stop the animation and print the "Finished Querying" message
        stop_event.set()
        t.join()
        if not end:
            print(f"\rError: {message}           ", end="\n")
        else:
            print(f"\rFinished: {message} ({(end-start):.2f}s) ", end="\n")


def get_feature_group_url(feature_store_id: int, feature_group_id: int) -> str:
    sub_path = (
        "/p/"
        + str(client.get_instance()._project_id)
        + "/fs/"
        + str(feature_store_id)
        + "/fg/"
        + str(feature_group_id)
    )
    return get_hostname_replaced_url(sub_path)


def build_serving_keys_from_prepared_statements(
    prepared_statements: List[serving_prepared_statement.ServingPreparedStatement],
    feature_store_id: int,
    ignore_prefix: bool = False,
) -> Set[serving_key.ServingKey]:
    serving_keys = set()
    fg_api = feature_group_api.FeatureGroupApi()
    for statement in prepared_statements:
        fg = fg_api.get_by_id(feature_store_id, statement.feature_group_id)
        for param in statement.prepared_statement_parameters:
            serving_keys.add(
                serving_key.ServingKey(
                    feature_name=param.name,
                    join_index=statement.prepared_statement_index,
                    prefix=statement.prefix,
                    ignore_prefix=ignore_prefix,
                    feature_group=fg,
                )
            )
    return serving_keys


def is_runtime_notebook():
    if "ipykernel" in sys.modules:
        return True
    else:
        return False


class NpDatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        dtypes = (np.datetime64, np.complexfloating)
        if isinstance(obj, (datetime, date)):
            return convert_event_time_to_timestamp(obj)
        elif isinstance(obj, dtypes):
            return str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            if any([np.issubdtype(obj.dtype, i) for i in dtypes]):
                return obj.astype(str).tolist()
            return obj.tolist()
        return super(NpDatetimeEncoder, self).default(obj)


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

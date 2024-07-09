#
#   Copyright 2024 Hopsworks AB
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
import re
from typing import Any, Dict, Optional

from hsfs import util
from hsfs.core.constants import HAS_AIOMYSQL, HAS_SQLALCHEMY


if HAS_SQLALCHEMY:
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import make_url

if HAS_AIOMYSQL:
    from aiomysql.sa import create_engine as async_create_engine


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
        host = util.get_host_name()
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
            hostname = util.get_host_name()
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

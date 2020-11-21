import argparse
import json
import hsfs

from typing import Dict, Any
from pydoop import hdfs
from pyspark.sql import SparkSession


def read_job_conf(path: str) -> Dict[Any, Any]:
    file_content = hdfs.load(path)
    return json.loads(file_content)


def setup_spark() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def get_feature_store_handle(feature_store: str = "") -> hsfs.feature_store:
    connection = hsfs.connection()
    return connection.get_feature_store(feature_store)


def create_fg(spark: SparkSession, job_conf: Dict[Any, Any]) -> None:
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    data_path = job_conf.pop("data_path")
    data_format = job_conf.pop("data_format")
    data_options = job_conf.pop("data_options")

    df = spark.read.format(data_format).options(data_options).load(data_path)

    fg = fs.create_feature_group(**job_conf)
    fg.save(df)


def create_td(job_conf: Dict[Any, Any]) -> None:
    # Extract the feature store handle
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    # Extract the query object
    query = hsfs.Query.from_response_json(job_conf.pop("query"))

    storage_connector = None
    if "storage_connector" in job_conf:
        storage_connector_json = job_conf.pop("storage_connector")
        storage_connector = fs.get_storage_connector(
            storage_connector_json["name"], storage_connector_json["type"]
        )

    td = fs.create_training_dataset(**job_conf, storage_connector=storage_connector)
    td.save(query)


def compute_stats(job_conf: Dict[Any, Any]) -> None:
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HSFS Job Utils")
    parser.add_argument(
        "op",
        type=str,
        choices=["create_fg", "create_td", "compute_stats"],
        help="Operation type",
    )
    parser.add_argument(
        "path",
        type=str,
        help="Location on HopsFS of the JSON containing the full configuration",
    )

    args = parser.parse_args()
    job_conf = read_job_conf(args.path)

    spark = setup_spark()

    if args.op == "create_fg":
        create_fg(spark, job_conf)
    elif args.op == "create_td":
        create_td(job_conf)
    elif args.op == "compute_stats":
        compute_stats(job_conf)

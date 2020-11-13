import argparse
import json
import hsfs

from typing import Dict, Any
from pydoop import hdfs 


def read_job_conf(path: str) -> Dict[Any, Any]:
    file_content = hdfs.load(path)
    return json.loads(file_content)


def get_feature_store_handle(feature_store: str = "") -> hsfs.feature_store:
    connection = hsfs.connection()
    return connection.get_feature_store(feature_store)


def create_td(job_conf: Dict[Any, Any]) -> None:
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)


    fs.create_training_dataset()


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

    if args.op == "create_fg":
        create_fg(job_conf)
    elif args.op == "create_td":
        create_td(job_conf)
    elif args.op == "compute_stats":
        compute_stats(job_conf)

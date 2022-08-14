import pathlib
import shutil

import keras_autodoc

PAGES = {
    "api/connection_api.md": {
        "connection": ["hsfs.connection.Connection"],
        "connection_properties": keras_autodoc.get_properties(
            "hsfs.connection.Connection"
        ),
        "connection_methods": keras_autodoc.get_methods("hsfs.connection.Connection"),
    },
    "api/feature_store_api.md": {
        "fs": ["hsfs.feature_store.FeatureStore"],
        "fs_get": ["hsfs.connection.Connection.get_feature_store"],
        "fs_properties": keras_autodoc.get_properties(
            "hsfs.feature_store.FeatureStore"
        ),
        "fs_methods": keras_autodoc.get_methods("hsfs.feature_store.FeatureStore"),
    },
    "api/feature_group_api.md": {
        "fg": ["hsfs.feature_group.FeatureGroup"],
        "fg_create": [
            "hsfs.feature_store.FeatureStore.create_feature_group",
            "hsfs.feature_store.FeatureStore.get_or_create_feature_group",
        ],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.FeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods("hsfs.feature_group.FeatureGroup"),
    },
    "api/external_feature_group_api.md": {
        "fg": ["hsfs.feature_group.ExternalFeatureGroup"],
        "fg_create": ["hsfs.feature_store.FeatureStore.create_external_feature_group"],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_external_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.ExternalFeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods(
            "hsfs.feature_group.ExternalFeatureGroup"
        ),
    },
    "api/training_dataset_api.md": {
        "td": ["hsfs.training_dataset.TrainingDataset"],
        "td_create": ["hsfs.feature_store.FeatureStore.create_training_dataset"],
        "td_get": ["hsfs.feature_store.FeatureStore.get_training_dataset"],
        "td_properties": keras_autodoc.get_properties(
            "hsfs.training_dataset.TrainingDataset"
        ),
        "td_methods": keras_autodoc.get_methods(
            "hsfs.training_dataset.TrainingDataset"
        ),
    },
    "api/feature_view_api.md": {
        "fv": ["hsfs.feature_view.FeatureView"],
        "fv_create": ["hsfs.feature_store.FeatureStore.create_feature_view"],
        "fv_get": ["hsfs.feature_store.FeatureStore.get_feature_view"],
        "fvs_get": ["hsfs.feature_store.FeatureStore.get_feature_views"],
        "fv_properties": keras_autodoc.get_properties("hsfs.feature_view.FeatureView"),
        "fv_methods": keras_autodoc.get_methods("hsfs.feature_view.FeatureView"),
    },
    "api/feature_api.md": {
        "feature": ["hsfs.feature.Feature"],
        "feature_properties": keras_autodoc.get_properties("hsfs.feature.Feature"),
        "feature_methods": keras_autodoc.get_methods("hsfs.feature.Feature"),
    },
    "api/storage_connector_api.md": {
        "sc_get": [
            "hsfs.feature_store.FeatureStore.get_storage_connector",
            "hsfs.feature_store.FeatureStore.get_online_storage_connector",
        ],
        "hopsfs_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.HopsFSConnector", exclude=["from_response_json"]
        ),
        "hopsfs_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.HopsFSConnector"
        ),
        "s3_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.S3Connector", exclude=["from_response_json"]
        ),
        "s3_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.S3Connector"
        ),
        "redshift_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.RedshiftConnector", exclude=["from_response_json"]
        ),
        "redshift_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.RedshiftConnector"
        ),
        "adls_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.AdlsConnector", exclude=["from_response_json"]
        ),
        "adls_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.AdlsConnector"
        ),
        "snowflake_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.SnowflakeConnector", exclude=["from_response_json"]
        ),
        "snowflake_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.SnowflakeConnector"
        ),
        "jdbc_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.JdbcConnector", exclude=["from_response_json"]
        ),
        "jdbc_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.JdbcConnector"
        ),
        "gcs_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.GcsConnector", exclude=["from_response_json"]
        ),
        "gcs_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.GcsConnector"
        ),
        "bigquery_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.BigQueryConnector", exclude=["from_response_json"]
        ),
        "bigquery_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.BigQueryConnector"
        ),
    },
    "api/statistics_config_api.md": {
        "statistics_config": ["hsfs.statistics_config.StatisticsConfig"],
        "statistics_config_properties": keras_autodoc.get_properties(
            "hsfs.statistics_config.StatisticsConfig"
        ),
    },
    "api/transformation_functions_api.md": {
        "transformation_function": [
            "hsfs.transformation_function.TransformationFunction"
        ],
        "transformation_function_properties": keras_autodoc.get_properties(
            "hsfs.transformation_function.TransformationFunction"
        ),
        "transformation_function_methods": keras_autodoc.get_methods(
            "hsfs.transformation_function.TransformationFunction",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
        "create_transformation_function": [
            "hsfs.feature_store.FeatureStore.create_transformation_function"
        ],
        "get_transformation_function": [
            "hsfs.feature_store.FeatureStore.get_transformation_function"
        ],
        "get_transformation_functions": [
            "hsfs.feature_store.FeatureStore.get_transformation_functions"
        ],
    },
    "api/job_configuration.md": {
        "job_configuration": ["hsfs.core.job_configuration.JobConfiguration"]
    },
    "api/query_api.md": {
        "query_methods": keras_autodoc.get_methods(
            "hsfs.constructor.query.Query",
            exclude=["json", "to_dict"],
        ),
        "query_properties": keras_autodoc.get_properties(
            "hsfs.constructor.query.Query"
        ),
    },
}

hsfs_dir = pathlib.Path(__file__).resolve().parents[0]


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url="https://github.com/logicalclocks/feature-store-api/blob/master/python",
        template_dir="./docs/templates",
        titles_size="###",
        extra_aliases={
            "hsfs.core.query.Query": "hsfs.Query",
            "hsfs.storage_connector.StorageConnector": "hsfs.StorageConnector",
            "hsfs.statistics_config.StatisticsConfig": "hsfs.StatisticsConfig",
            "hsfs.training_dataset_feature.TrainingDatasetFeature": "hsfs.TrainingDatasetFeature",
            "pandas.core.frame.DataFrame": "pandas.DataFrame",
        },
        max_signature_line_length=100,
    )
    shutil.copyfile(hsfs_dir / "CONTRIBUTING.md", dest_dir / "CONTRIBUTING.md")
    shutil.copyfile(hsfs_dir / "README.md", dest_dir / "index.md")

    doc_generator.generate(dest_dir / "generated")


if __name__ == "__main__":
    generate(hsfs_dir / "docs")

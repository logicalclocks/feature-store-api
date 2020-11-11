import pathlib
import shutil

import keras_autodoc

PAGES = {
    "project.md": {
        "connection": ["hsfs.connection.Connection"],
        "connection_methods": keras_autodoc.get_methods(
            "hsfs.connection.Connection", exclude=["connection"]
        ),
    },
    "feature_store.md": {
        "fs_get": ["hsfs.connection.Connection.get_feature_store"],
        "fs_properties": keras_autodoc.get_properties(
            "hsfs.feature_store.FeatureStore"
        ),
        "fs_methods": keras_autodoc.get_methods(
            "hsfs.feature_store.FeatureStore", exclude=["from_response_json"]
        ),
    },
    "feature.md": {
        "feature": ["hsfs.feature.Feature"],
        "feature_properties": keras_autodoc.get_properties("hsfs.feature.Feature"),
        "feature_methods": keras_autodoc.get_methods(
            "hsfs.feature.Feature", exclude=["from_response_json", "to_dict"]
        ),
    },
    "feature_group.md": {
        "fg_create": ["hsfs.feature_store.FeatureStore.create_feature_group"],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.FeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods(
            "hsfs.feature_group.FeatureGroup",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
    },
    "training_dataset.md": {
        "td_create": ["hsfs.feature_store.FeatureStore.create_training_dataset"],
        "td_get": ["hsfs.feature_store.FeatureStore.get_training_dataset"],
        "td_properties": keras_autodoc.get_properties(
            "hsfs.training_dataset.TrainingDataset"
        ),
        "td_methods": keras_autodoc.get_methods(
            "hsfs.training_dataset.TrainingDataset",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
        "tf_record_dataset": ["hsfs.core.tfdata_engine.TFDataEngine.tf_record_dataset"],
        "tf_csv_dataset": ["hsfs.core.tfdata_engine.TFDataEngine.tf_csv_dataset"],
    },
    "storage_connector.md": {
        "sc_get": [
            "hsfs.feature_store.FeatureStore.get_storage_connector",
            "hsfs.feature_store.FeatureStore.get_online_storage_connector",
        ],
        "sc_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.StorageConnector", exclude=["from_response_json"]
        ),
        "sc_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.StorageConnector"
        ),
    },
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
        "fg_create": ["hsfs.feature_store.FeatureStore.create_feature_group"],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.FeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods("hsfs.feature_group.FeatureGroup"),
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
        "sc_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.StorageConnector"
        ),
        "sc_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.StorageConnector"
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

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
    "query_vs_dataframe.md": {
        "query_methods": keras_autodoc.get_methods("hsfs.constructor.query.Query"),
        "query_properties": keras_autodoc.get_properties(
            "hsfs.constructor.query.Query"
        ),
    },
    "statistics.md": {
        "statistics_config": ["hsfs.statistics_config.StatisticsConfig"],
        "statistics_config_properties": keras_autodoc.get_properties(
            "hsfs.statistics_config.StatisticsConfig"
        ),
    },
    "feature_validation.md": {
        "rule": ["hsfs.rule.Rule"],
        "rule_properties": keras_autodoc.get_properties("hsfs.rule.Rule"),
        "ruledefinition": ["hsfs.ruledefinition.RuleDefinition"],
        "ruledefinition_getall": ["hsfs.connection.Connection.get_rules"],
        "ruledefinition_get": ["hsfs.connection.Connection.get_rule"],
        "ruledefinition_properties": keras_autodoc.get_properties(
            "hsfs.ruledefinition.RuleDefinition"
        ),
        "expectation": ["hsfs.expectation.Expectation"],
        "expectation_properties": keras_autodoc.get_properties(
            "hsfs.expectation.Expectation"
        ),
        "expectation_methods": keras_autodoc.get_methods(
            "hsfs.expectation.Expectation",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
        "expectation_create": ["hsfs.feature_store.FeatureStore.create_expectation"],
        "expectation_get": ["hsfs.feature_store.FeatureStore.get_expectation"],
        "expectation_getall": ["hsfs.feature_store.FeatureStore.get_expectations"],
        "validation_result": ["hsfs.validation_result.ValidationResult"],
        "validation_result_properties": keras_autodoc.get_properties(
            "hsfs.validation_result.ValidationResult"
        ),
        "validate": ["hsfs.feature_group.FeatureGroup.validate"],
        "validation_result_get": ["hsfs.feature_group.FeatureGroup.get_validations"],
    },
    "tags.md": {
        "fg_tag_add": ["hsfs.feature_group.FeatureGroupBase.add_tag"],
        "fg_tag_get": ["hsfs.feature_group.FeatureGroupBase.get_tag"],
        "fg_tag_get_all": ["hsfs.feature_group.FeatureGroupBase.get_tags"],
        "fg_tag_delete": ["hsfs.feature_group.FeatureGroupBase.delete_tag"],
        "td_tag_add": ["hsfs.training_dataset.TrainingDataset.add_tag"],
        "td_tag_get": ["hsfs.training_dataset.TrainingDataset.get_tag"],
        "td_tag_get_all": ["hsfs.training_dataset.TrainingDataset.get_tags"],
        "td_tag_delete": ["hsfs.training_dataset.TrainingDataset.delete_tag"],
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
    "api/statistics_config_api.md": {
        "statistics_config": ["hsfs.statistics_config.StatisticsConfig"],
        "statistics_config_properties": keras_autodoc.get_properties(
            "hsfs.statistics_config.StatisticsConfig"
        ),
    },
    "api/rule_api.md": {
        "rule": ["hsfs.rule.Rule"],
        "rule_properties": keras_autodoc.get_properties("hsfs.rule.Rule"),
    },
    "api/rule_definition_api.md": {
        "ruledefinition": ["hsfs.ruledefinition.RuleDefinition"],
        "ruledefinition_getall": ["hsfs.connection.Connection.get_rules"],
        "ruledefinition_get": ["hsfs.connection.Connection.get_rule"],
        "ruledefinition_properties": keras_autodoc.get_properties(
            "hsfs.ruledefinition.RuleDefinition"
        ),
    },
    "api/expectation_api.md": {
        "expectation": ["hsfs.expectation.Expectation"],
        "expectation_properties": keras_autodoc.get_properties(
            "hsfs.expectation.Expectation"
        ),
        "expectation_methods": keras_autodoc.get_methods(
            "hsfs.expectation.Expectation",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
        "expectation_create": ["hsfs.feature_store.FeatureStore.create_expectation"],
        "expectation_get": ["hsfs.feature_store.FeatureStore.get_expectation"],
        "expectation_getall": ["hsfs.feature_store.FeatureStore.get_expectations"],
    },
    "api/validation_api.md": {
        "validation_result": ["hsfs.validation_result.ValidationResult"],
        "validation_result_properties": keras_autodoc.get_properties(
            "hsfs.validation_result.ValidationResult"
        ),
        "validate": ["hsfs.feature_group.FeatureGroup.validate"],
        "validation_result_get": ["hsfs.feature_group.FeatureGroup.get_validations"],
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

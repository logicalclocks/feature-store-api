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
        "fs": ["hsfs.feature_store.FeatureStore"],
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
    "connection_api.md": ["hsfs.connection.Connection"]
    + keras_autodoc.get_methods("hsfs.connection.Connection"),
    "feature_group_api.md": {
        "fg": ["hsfs.feature_group.FeatureGroup"],
        "fg_create": ["hsfs.feature_store.FeatureStore.create_feature_group"],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.FeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods("hsfs.feature_group.FeatureGroup"),
    },
}

hsfs_dir = pathlib.Path(__file__).resolve().parents[0]


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url="https://github.com/logicalclocks/feature-store-api/blob/master/python",
        template_dir="./docs/templates",
        titles_size="###",
    )
    shutil.copyfile(hsfs_dir / "CONTRIBUTING.md", dest_dir / "CONTRIBUTING.md")
    shutil.copyfile(hsfs_dir / "README.md", dest_dir / "index.md")

    doc_generator.generate(dest_dir / "generated")


if __name__ == "__main__":
    generate(hsfs_dir / "docs")

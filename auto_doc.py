import pathlib
import shutil

import keras_autodoc

PAGES = {
    "connection.md": [
        "hsfs.connection.Connection.connection",
        "hsfs.connection.Connection.setup_databricks",
    ]
}

hsfs_dir = pathlib.Path(__file__).resolve().parents[0]


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url="https://github.com/logicalclocks/feature-store-api/blob/master/python",
        template_dir="./docs/templates",
    )
    shutil.copyfile(hsfs_dir / "CONTRIBUTING.md", dest_dir / "contributing.md")
    shutil.copyfile(hsfs_dir / "README.md", dest_dir / "index.md")

    doc_generator.generate(dest_dir / "generated")


if __name__ == "__main__":
    generate(hsfs_dir / "docs")

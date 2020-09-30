import keras_autodoc

PAGES = {
    "connection.md": [
        "hsfs.connection.Connection.connection",
        "hsfs.connection.Connection.setup_databricks",
    ]
}


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url="https://github.com/logicalclocks/feature-store-api/blob/master/python",
        template_dir="./docs/templates",
    )
    doc_generator.generate(dest_dir)


if __name__ == "__main__":
    generate("./docs/generated")

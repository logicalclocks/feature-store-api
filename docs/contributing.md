## Python development setup
---

- Fork and clone the repository

- Create a new Python environment with your favourite environment manager, e.g. virtualenv or conda

- Install repository in editable mode with development dependencies:

    ```bash
    cd python
    pip install -e ".[dev]"
    ```

- Install [pre-commit](https://pre-commit.com/) and then activate its hooks. pre-commit is a framework for managing and maintaining multi-language pre-commit hooks. The Feature Store uses pre-commit to ensure code-style and code formatting through [black](https://github.com/psf/black) and [flake8](https://gitlab.com/pycqa/flake8):

    ```bash
    cd python
    pip install --user pre-commit
    pre-commit install
    ```

  Afterwards, pre-commit will run whenever you commit.

- To run formatting and code-style separately, you can configure your IDE, such as VSCode, to use black and flake8, or run them via the command line:

    ```bash
    cd python
    flake8 hopsworks
    black hopsworks
    ```

### Python documentation

We follow a few best practices for writing the Python documentation:

1. Use the google docstring style:

    ```python
    """[One Line Summary]

    [Extended Summary]

    [!!! example
        import xyz
    ]

    # Arguments
        arg1: Type[, optional]. Description[, defaults to `default`]
        arg2: Type[, optional]. Description[, defaults to `default`]

    # Returns
        Type. Description.

    # Raises
        Exception. Description.
    """
    ```

    If Python 3 type annotations are used, they are inserted automatically.


2. Feature store entity engine methods (e.g. FeatureGroupEngine etc.) only require a single line docstring.
3. REST Api implementations (e.g. FeatureGroupApi etc.) should be fully documented with docstrings without defaults.
4. Public Api such as metadata objects should be fully documented with defaults.

#### Setup and Build Documentation

We use `mkdocs` to build the documentation and a plugin called `keras-autodoc` to auto generate Python API documentation from docstrings.

1. Currently we are using our own version of `keras-autodoc`

    ```bash
    pip install git+https://github.com/moritzmeister/keras-autodoc@split-tags
    ```

2. Install HSFS with `docs` extras:

    ```bash
    pip install -e .[dev,docs]
    ```

3. To build the docs, first run the auto doc script:

    ```bash
    python auto_doc.py
    ```

4. Either build the docs, or serve them dynamically:

    ```bash
    mkdocs build
    # or
    mkdocs serve
    ```

#### Adding new API documentation

To add new documentation for APIs, you need to add information about the method/class to document to the `auto_doc.py` script:

```python
PAGES = {
    "connection.md": [
        "hsfs.connection.Connection.connection",
        "hsfs.connection.Connection.setup_databricks",
    ]
    "new_template.md": [
            "module",
            "xyz.asd"
    ]
}
```

Now you can add a template markdown file to the `docs/templates` directory with the name you specified in the auto-doc script. The `new_template.md` file should contain a tag to identify the place at which the API documentation should be inserted:

```
## The XYZ package

{{module}}

Some extra content here.

!!! example
    ```python
    import xyz
    ```

{{xyz.asd}}
```

Finally, run the `auto_doc.py` script, as decribed above, to update the documentation.

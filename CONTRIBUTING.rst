Python development setup
~~~~~~~~~~~~~~~~
- Fork and clone the repository

- Create a new Python environment with your favourite environment manager, e.g. virtualenv or conda::

- Install repository in editable mode with development dependencies::

        pip install -e ".[dev]"

- Install pre-commit_ and then activate its hooks. pre-commit is a framework for managing and maintaining multi-language pre-commit hooks. The Feature Store uses pre-commit to ensure code-style and code formatting through black_ and flake8_::

    $ pip install --user pre-commit
    $ pre-commit install

  Afterwards, pre-commit will run whenever you commit.

.. _pre-commit: https://pre-commit.com/
.. _flake8: https://gitlab.com/pycqa/flake8
.. _black: https://github.com/psf/black

- To run formatting and code-style separately, you can configure your IDE, such as VSCode, to use black_ and flake8_, or run them via the command line::

    $ flake8 hopsworks
    $ black hopsworks


Python documentation
~~~~~~~~~~~~~~~~
We follow a few best practices for generating the Python documentation:

1. Use the sphinx docstring style.

        """[single line summary]

        [extended summary]

        :param [param1]: [description]
        :type [param1]: [type]
        :param [param2]: [description]
        :type [param2]: [type]
        :return: [description]
        :rtype: [type]
        """

2. Feature store entity engine methods (e.g. FeatureGroupEngine etc.) only require a single line docstring.
3. REST Api implementations (e.g. FeatureGroupApi etc.) should be fully documented with docstrings without defaults.
4. Public Api such as metadata objects should be fully documented with defaults.

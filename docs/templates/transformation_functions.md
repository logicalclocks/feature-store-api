# Transformation Functions

HSFS provides functionality to attach transformation functions to [training datasets](training_dataset.md).

To be able to attach transformation function to training datasets it has to be either part of the library
[installed](https://hopsworks.readthedocs.io/en/stable/user_guide/hopsworks/python.html?highlight=install#installing-libraries) in Hopsworks
or attached when starting a [Jupyter notebook](https://hopsworks.readthedocs.io/en/stable/user_guide/hopsworks/jupyter.html?highlight=jupyter)
or [Hopsworks job](https://hopsworks.readthedocs.io/en/stable/user_guide/hopsworks/jobs.html).

!!! warning "Pyspark decorators."
    Don't decorate transformation function with Pyspark `@udf` or `@pandas_udf`, as well as don't use any Pyspark dependencies.
    HSFS will decorate transformation function only if it is used inside Pyspark application.

## Examples

=== "Python"

    !!! example "Register transformation function  `plus_one` in the Hopsworks feature store."
        ```python
        from hsfs_transformers import transformers
        plus_one_meta = fs.create_transformation_function(
                    transformation_function=transformers.plus_one,
                    output_type=int,
                    version=1)
        plus_one_meta.save()
        ```

To retrieve all transformation functions from the feature store use `get_transformation_functions` that will return list of `TransformatioFunction` objects.
Specific transformation function can be retrieved by `get_transformation_function` method where user can provide name and version of the transformation function.
If only name is provided then it will default to version 1.

=== "Python"

    !!! example "Retrieving transformation functions from the feature store"
        ```python
        # get all transformation functions
        fs.get_transformation_functions()

        # get transformation function by name. This will default to version 1
        fs.get_transformation_function(name="plus_one")

        # get transformation function by name and version.
        fs.get_transformation_function(name="plus_one", version=2)
        ```

To attach transformation function to training dataset provide transformation functions as dict, where key is feature name and value is online transformation function name.
Also training dataset must be created from the [Query](query_vs_dataframe.md) object. Once attached transformation function will be applied on whenever `save`, `insert` and `get_serving_vector`
methods are called on training dataset object.

=== "Python"

    !!! example "Attaching transformation functions to the training dataset"
        ```python
        plus_one_meta = fs.get_transformation_function(name="plus_one", version=1)
        fs.create_training_dataset(name="td_demo",
                                   description="Dataset to train the demo model",
                                   data_format="csv",
                                   transformation_functions={"feature_name":plus_one_meta}
                                   statistics_config=None,
                                   version=1)
        td.save(join_query)
        ```

!!! warning "Scala support"
    Creating and attaching Transformation functions to training datasets are not supported for hsfs scala client.
    If training dataset with transformation function was created using python client and later `insert` or `getServingVector`
    methods are called on this training dataset from scala client hsfs will throw an exception.


# Transfromation Function

{{transformation_function}}

## Properties

{{transformation_function_properties}}

## Methods

{{transformation_function_methods}}

## Creation
{{create_transformation_function}}

## Retrieval

{{get_transformation_function}}

{{get_transformation_functions}}

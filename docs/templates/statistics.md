# Statistics

HSFS provides functionality to compute statistics for [training datasets](training_dataset.md) and [feature groups](feature_group.md) and save these along with their other metadata in the [feature store](feature_store.md).
These statistics are meant to be helpful for Data Scientists to perform explorative data analysis and then recognize suitable [features](feature.md) or [training datasets](training_dataset.md) for models.

Statistics are configured on a training dataset or feature group level using a `StatisticsConfig` object.
This object can be passed at creation time of the dataset or group or it can later on be updated through the API.

{{statistics_config}}

For example, to enable all statistics (descriptive, histograms and correlations) for a training dataset:

=== "Python"
    ```python
    from hsfs.statistics_config import StatisticsConfig

    td = fs.create_training_dataset("rain_dataset",
                            version=1,
                            label=”weekly_rain”,
                            data_format=”tfrecords”,
                            statistics_config=StatisticsConfig(true, true, true))

    ```
=== "Scala"
    ```scala
    val td = (fs.createTrainingDataset()
                            .name("rain_dataset")
                            .version(1)
                            .label(”weekly_rain”)
                            .dataFormat(”tfrecords”)
                            .statisticsConfig(new StatisticsConfig(true, true, true))
                            .build())
    ```

And similarly for feature groups.

!!! note "Default StatisticsConfig"
    By default all training datasets and feature groups will be configured such that only descriptive statistics
    are computed. However, you can also enable `histograms` and `correlations` or limit the features for which
    statistics are computed.

## Properties

{{statistics_config_properties}}

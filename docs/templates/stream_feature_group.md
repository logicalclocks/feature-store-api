# Stream Feature Group

Stream Feature Groups are Feature Groups that have unified single API for writing streaming features transparently to both online/offline storage.
It also compute framework independent, for example other types of Feature Groups support only Apache Spark and/or Python while with Stream Feature Groups other frameworks,
such as Apache Flink can be also used.

From an API perspective, Stream Feature Groups can be used in the same way as regular feature groups. Users can pick
features create training datasets. Stream Feature Groups can be also used as data source to create derived features,
meaning features on which additional feature engineering is applied.


=== "Python"

    !!! example "Define a stream feature group"
        ```python
        stream_agg_fg = fs.create_stream_feature_group("stream_agg",
                                                        version = 1,
                                                        primary_key=["id"])
        stream_agg_fg.save(stream_agg_df)
        ```

=== "Scala"

    !!! example "Define a stream feature group"
        ```scala
        val streamAggFg = (fs.createOnDemandFeatureGroup()
                            .name("stream_agg")
                            .version(1)
                            .primaryKeys(Collections.singletonList("id"))
                            .build());
         streamAggFg.save()
        ```

## Use case

The use case in which a user can benefit from a stream feature groups:

- **Real-Time feature engineering on streaming source**: Feature engineering pipeline on streaming source updates
  online feature store and fresh features are  immediately available for model serving. Offline feature store
  will be backfilled later on (depending on configuration). When Stream Feature Group is created using  `save`
  method it will  also configure periodic job for backfilling data to offline storage. Jobs will be run periodically
  and this will not interfere or compete for resources with the streaming application. But this also means that updated
  data in offline store will be available depending on cadence of the backfilling job.

## Creation

{{fg_create}}

## Retrieval

{{fg_get}}

## Properties

{{fg_properties}}

## Methods

{{fg_methods}}

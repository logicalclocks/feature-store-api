# Stream Feature Group

Stream Feature Groups are Feature Groups that have unified single API for writing streaming features transparently
to both online and offline store. It is also compute framework independent, for example other types of Feature
Groups support only Apache Spark and/or Python while with Stream Feature Group other frameworks, such as Apache Flink
can be also used.

From an API perspective, Stream Feature Groups are an extension of regular Feature Groups. can be used in the same way.
For example users can pick features create and training datasets.

## Limitations

Stream Feature Group does not support validations.

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

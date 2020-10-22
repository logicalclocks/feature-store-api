# Feature Group

A Feature Groups is a logical grouping of features, and experience has shown, that this grouping generally originates from the features being derived from the same data source.
The Feature Group lets you save metadata along features, which defines how the Feature Store interprets them, combines them and reproduces training datasets created from them.

Generally, the features in a feature froup are engineered together in an ingestion job. However, it is possible to have additional jobs to append features to an existing feature group. Furthermore, feature groups provide a way of defining a namespace for features, such that you can define features with the same name multiple times, but uniquely identified by the group they are contained in.

!!! info "Combine features from any number of feature groups"
    Feature groups are logical groupings of features, usually based on the data source and ingestion job from which they originate.
    It is important to note that feature groups are **not** groupings of features for immediate training of Machine Learning models.
    Instead, to ensure reusability of features, it is possible to combine features from any number of groups into training datasets.

## Versioning

Feature groups can be versioned. Data Engineers should use the version to indicate to a Data Scientist that the schema or the feature engineering logic of the features in this group has changed.

!!! danger "Breaking feature group schema changes"
    In order to guarantee reproducability, the schema of a feature group should be immutable, because deleting features could lead to failing model pipelines downstream. Hence, in order to modify a schema, a new version of a feature group has to be created.

    In contrary, appending features to feature groups is considered a non-breaking change, since the feature store makes all selections explicit and because the namespace within a feature group is flat, it is not possible to append a new feature with an already existing name to a feature group.

## Creation

{{fg_create}}

## Retrieval

{{fg_get}}

## Properties

{{fg_properties}}

## Methods

{{fg_methods}}

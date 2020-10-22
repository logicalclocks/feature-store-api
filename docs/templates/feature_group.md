# Feature Group

Feature Groups are a logical grouping of features, and our experience has shown, that this grouping generally originates from the features being derived from the same source.
The Feature Group lets you define metadata along features, which defines how the Feature Store interprets them, combines them and reproduces training datasets created from them.

Generally, the features in a feature froup are engineered together with a one to one mapping from ingestion job to feature group. Furthermore, feature groups provide a way of defining a namespace for features, such that you can define features with the same name multiple times, but uniquely identified by the group they are contained in.

!!! info "Combine features from any number of feature groups"
    Feature groups are logical groupings of features, usually based on the data source and ingestion job from which they originate.
    It is important to note that feature groups are **not** groupings of features for immediate training of Machine Learning models.
    Instead, to ensure reusability of features, it is possible to combine features from any number of groups into training datasets.

{{feature_group}}

## Methods

{{feature_group_methods}}

## Attributed

{{fg_attributes}}

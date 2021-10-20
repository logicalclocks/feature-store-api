# Versioning

The concept of versioning in Hopsworks works on two dimensions: metadata versioning (e.g. schemas) and data versioning.

## Metadata versioning

Every entity in the Hopsworks feature store has a version number. As an example, every feature group is uniquely identified within the platform based on the feature store (Project) it belongs to, its name and version.

The version allows users to identify breaking changes in the schema or computation of features. For example, if a user decides to remove a feature or change the way that feature is computed, that is considered a breaking change and require a version increase.

Increasing the version number will allow existing pipelines and models to keep using the old version of the feature(s) up until the pipeline is adapted to the new feature group version or the model is re-trained. This allow users to progressively rollout and test new features.

### Creating a new version

When creating a feature group or training dataset using the `create_feature_group()` or `create_training_dataset()` methods of the [FeatureStore](../feature_store) object, it is possible to provide a version number using the `version` parameter.
The `version` parameter is not mandatory. If not provided and no feature group (or training dataset) with that name exists, then the version is going to be set to 1. If the `version` parameter is not provided, and a feature group (or training dataset) already exist, then the version number will be increased by one.

### Appending features to an existing version

For feature groups, it is possible to append new features to existing feature groups. This is not considered a breaking change. To append new features users can either use the UI or the following `append_features` method:

{{fg_append}}

Appended features can also define a default value as placeholder for the feature data that is already present in the feature group. When setting a default value, the value is going to be attached as metadata, the existing data does not need to be rewritten. The default value is going to be used by the [query APIs](../query_vs_dataframe/#the-query-abstraction).


### Retrieving a specific version

When retrieving a feature group from the feature store, the `get_feature_group()` has an optional `version` parameter. If the `version` is not provided, the version defaults to 1. This is done explicitly to guarantee a safe behavior for pipelines and models that use the feature group.

Setting the default version to 1 will make sure that, even when the user does not specify the version number, Hopsworks can still guarantee a degree of safety for pipelines and models. This is because, even if new breaking changes are introduced (and so new versions are created), existing pipelines will still use the same version they have been build or trained with.

### Retrieving all the versions

It is also possible to retrieve the metadata of all the versions of a feature group or training dataset based on its name using the following methods:

{{fg_get_all}}
{{td_get_all}}

## Data versioning

Data versioning captures the different commits of data that are inserted into a feature group.
The data, as it belongs to the same schema version, is homogeneous in terms of schema structure and feature definition. Hopsworks provides data versioning capabilities only for the offline feature groups. The online version of the feature groups only store the most recent values for any given primary key.
Data versioning is also not critical for training dataset, which are point in time snapshots of a set of features.

Data stored on the offline feature store is stored as [Apache Hudi](http://hudi.apache.org/) files. Apache Hudi provides the Upsert and Time Travel capabilities that powers the Hopsworks offline feature store.

Using Apache Hudi users on Hopsworks are able to track what data was inserted at which commit. Information regarding the commits made on a feature group, the amount of new rows written, updated and deleted is available in the Activity UI of a feature group.

<p align="center">
  <img src="../../assets/images/activities.png" width="600" alt="Feature group activities">
</p>

Users can also use the APIs to read the feature group data at a specific point in time using the `as_of` method of the query object:

{{as_of}}

Data versioning is critical for reproducibility and debugging. As an example, if a data scientist is debugging why a new model is performing poorly compared to the same model trained six months ago, they can leverage the time travel capabilities of the Hopsworks feature store to build a training dataset with the data as it was six months ago. From there, using [statistics](../statistics) and [data validation](../feature_validation), further debugging can be made to determine what is the root cause of the new model degraded performances.

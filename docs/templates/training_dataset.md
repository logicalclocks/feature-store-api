# Training Dataset

The training dataset abstraction in Hopsworks Feature Store allows users to group a set of features (potentially from
multiple different feature groups) with labels for training a model to do a particular prediction task. The training
dataset is a versioned and managed dataset and is stored in HopsFS as `tfrecords`, `parquet`, `csv`, or `tsv` files.

## Versioning

Training Dataset can be versioned. Data Scientist should use the version to indicate to the model, as well as to the
schema or the feature engineering logic of the features associated to this training dataset.

## Creation

To create training dataset, the user supplies a Pandas, Numpy or Spark dataframe with features and labels
together with metadata. Once the training dataset has been created, the dataset is discoverable in the feature registry
and users can use it to train models.

{{td_create}}

## Tagging Training Datasets
The feature store enables users to attach tags to training dataset in order to make them discoverable across feature
stores.  A tag is a simple {key: value} association, providing additional information about the data, such as for
example geographic origin. This is useful in an organization as it makes easier to discover for data scientists, reduces
duplicated work in terms of for example data preparation. The tagging feature is only available in the enterprise version.

#### Define tags that can be attached
The first step is to define a set of tags that can be attached. Such as for example “Country” to tag data as being from
a certain geographic location and “Sport” to further associate a type of Sport with the data.

![Define tags that can be attached](../../assets/images/creating_tags.gif)

#### Attach tags using the UI
Tags can then be attached using the feature store UI or programmatically using the API.
Attaching tags to feature group.

![Attach tags using the UI](../../assets/images/attach_tags.gif)

## Retrieval

{{td_get}}

## Properties

{{td_properties}}

## Methods

{{td_methods}}

## TFData engine

{{tf_record_dataset}}

{{tf_csv_dataset}}

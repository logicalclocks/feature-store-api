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

## Retrieval

{{td_get}}

## Properties

{{td_properties}}

## Methods

{{td_methods}}

## TFData engine

{{tf_record_dataset}}

{{tf_csv_dataset}}

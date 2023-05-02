# Feature Monitoring

Feature Monitoring complements the Hopsworks Data Validation capabilities by allowing
you to monitor your data once they have been ingested into the Feature Store. Hopsworks
feature monitoring user interface is centered around two functionalities:

- Scheduled Statistics Monitoring : The user defines a detection over its data for
which Hopsworks will compute the statistics on a regular basis. The results are stored
in Hopsworks and enable the user to visualise the temporal evolution of statistical
metrics on its data. This can be enabled for a whole Feature Group or Feature View,
or for a particular Feature.

- Individual Feature Monitoring: Enabled only for individual features, this variant allows
the user to schedule the statistics computation on both a detection and a reference
monitoring window. By providing information about how to compare those statistics,
you can setup alerts to quickly detect critical change in the data.

## Enabling Scheduled Statistics or Individual Feature Monitoring

Enabling feature monitoring for Feature Group or Feature View with a unified API.

{{feature_monitoring_enable}}

{{feature_monitoring_config_fetch}}

## Feature Monitoring Configuration and Results

{{feature_monitoring_config}}

{{feature_monitoring_config_properties}}

{{feature_monitoring_config_methods}}

{{feature_monitoring_result}}

{{feature_monitoring_result_properties}}

{{feature_monitoring_result_methods}}

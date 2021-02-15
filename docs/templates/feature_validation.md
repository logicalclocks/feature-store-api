# Feature Validation with the Hopsworks Feature Store

Correct feature data is essential for developing accurate machine learning models.
Raw data being ingested into the feature store maybe suffer from incorrect or corrupt values, may need to be validated
against certain features depending on the domain. For example, a feature representing the customer's age should not be a
negative number and should always have a value set.

HSFS provides an API to define expectations on data being inserted into feature groups and also view results over time
of these expectations in the form of feature validations.

Feature validations can therefore be easily integrated with existing feature ingestion pipelines.
HSFS utilizes the [Deequ](https://github.com/awslabs/deequ) open source library and support is currently being added for
working with the [Great Expectations](https://github.com/great-expectations/great_expectations).

Below we describe the different API components of the hsfs feature validation API and we walk you through Feature
validation is part of the HSFS Java/Scala and Python API for working with Feature Groups. Users work with the abstractions:


# Rule definitions
A set of pre-defined and immutable rule definitions (RuleDefiniton) that are unique by name and are used for creating
validation rules (Rule) and expectations (Expectation) applied on a dataframe that is inserted into a Feature Group.

## Retrieval

{{ruledefinition_getall}}

{{ruledefinition_get}}

For example, to get all available rule definitions in hsfs:

=== "Python"
    ```python
    import hsfs
    connection = hsfs.connection()
    rules = connection.get_rules()
    ```

=== "Scala"
    ```scala
    import com.logicalclocks.hsfs._
    val connection = HopsworksConnection.builder().build();
    val rules = connection.getRules()
    ```

and to get a rule definition by name:

=== "Python"
    ```python
    import hsfs
    connection = hsfs.connection()
    rules = connection.get_rules()
    ```

=== "Scala"
    ```scala
    import com.logicalclocks.hsfs._
    val connection = HopsworksConnection.builder().build();
    val rules = connection.getRules()
    ```

## Properties
{{ruledefinition}}

{{ruledefinition_properties}}


# Rules

Used as part of expectations that are applied on ingested features. Rule names correspond to the names of the
rule definitions (see section above) and you can set the severity level and the actual values that the feature should
respect.

## Setting rules values

Rules applied on numerical features except for a specific value they can also accept a range of values,
for example if you need a feature to be ingested if its minimum value is below zero, then you can set
`min(0)` and `max(0)` but if you want the minimum to fall within a range of `0` and `1000` then you need to set
`min(0)` and `max(1000)`. See section `Expectations` below for a detailed example.

## Properties
{{rule}}

{{rule_properties}}


# Expectations

A set of rule instances that are applied on a set of features. Expectations are created at the feature store level
and can be attached to multiple feature groups. An expectation contains a list of featues it is applied to.
If the feature group the expectation is attached to, does not contain all the expectations features, the expectation
will not be met.

## Creation

Create an expectation with two rules for ensuring the min and max of the features are valid:

=== "Python"
    ```python
    expectation_sales = fs.create_expectation("sales",
                                               description="min and max sales limits",
                                               features=["salary", "commission"],
                                               rules=[Rule(name="HAS_MIN", level="WARNING", min=0), Rule(name="HAS_MAX", level="ERROR", max=1000000)])
    expectation_sales.save()
    ```

=== "Scala"
    ```scala
    // Create an expectation for the "salary" and "commissio" features so that their min value is "10" and their max is "100"
    val expectationSales = (fs.createExpectation()
                              .rules(Seq(
                                  Rule.createRule(RuleName.HAS_MIN).min(0).level(Level.WARNING).build(), //Set rule by name
                                  Rule.createRule(ruleMax).max(1000000).level(Level.ERROR).build())) //Set rule by passing the RuleDefinition metadata
                              .name("sales")
                              .description("min and max sales limits")
                              .features(Seq("salary", "commission"))
                              .build())
    expectationSales.save()
    ```

{{expectation_create}}

## Retrieval

{{expectation_get}}
{{expectation_getall}}

## Properties
{{expectation}}

{{expectation_properties}}

## Methods
{{expectation_methods}}

# Validations
The results of expectations against the ingested dataframe are assigned a validation time and are persisted within the
Feature Store. Users can then retrieve validation results by validation time and by commit time for time-travel enabled
feature groups.

## Validation Type
Feature Validation is disabled by default, by having the validation_type feature group attribute set to NONE.
The list of allowed validation types are:

- STRICT: Data validation is performed and feature group is updated only if validation status is "Success"
- WARNING: Data validation is performed and feature group is updated only if validation status is "Warning" or lower
- ALL: Data validation is performed and feature group is updated only if validation status is "Failure" or lower
- NONE: Data validation not performed on feature group

For example, to update the validation type to all:

=== "Python"
    ```python
    fg.validation_type = "ALL"
    ```

=== "Scala"
    ```scala
    import com.logicalclocks.hsfs.metadata.validation._
    fg.updateValidationType(ValidationType.ALL)
    ```

## Validate
You can also apply the expectations on a dataframe without inserting data, that can be helpful for debugging.
{{validate}}

## Retrieval
{{validation_result_get}}

## Properties
{{validation_result}}

{{validation_result_properties}}

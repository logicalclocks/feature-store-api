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
Rule definitions is a set of pre-defined and immutable rules  (`RuleDefiniton`) that are unique by name and are used for
creating validation rules (`Rule`) and expectations (`Expectation`) applied on a dataframe that is ingested into a
Feature Group.

The following table describes all the supported rule definitions (code examples are shown in the section below).

- Name: The name of the rule.
- Predicate: The type of value this rule accepts. For example, when using `HAS_MIN` you need to set the value with the `value` parameter.
- Accepted type: The data type of the value users set for this rule. For example, the value of the `HAS_MIN` predicate must be a fractional number.
- Feature type: The data type of the feature this rule can be applied to. For example, the `HAS_MIN` rule can only be applied on quantitative features.
  If a rule is to be applied to an incompatible feature type, an error will be thrown when the expectation is attached on the feature group.
  If a rule does not have a feature type, then it can be applied on both quantitative and categorical features.
- Description: A short description of what the rule validates.

If an expectation contains a rule that can be applied

| Name                          | Predicate     | Accepted type | Feature type | Description                                                                             |
| ------------------------------|---------------|---------------|--------------|---------------------------------------------------------------------------------------- |
| HAS_APPROX_COUNT_DISTINCT     | VALUE         | Fractional    | NULL         | Assert on the approximate count distinct of a feature.                                  |
| HAS_APPROX_QUANTILE           | VALUE         | Fractional    | Quantitative | Assert on the approximate quantile of a feature.                                        |
| HAS_COMPLETENESS              | VALUE         | Fractional    | NULL         | Assert on the uniqueness of a single or combined set of features.                       |
| HAS_CORRELATION               | VALUE         | Fractional    | Quantitative | Assert on the pearson correleation between two features.                                |
| HAS_DATATYPE                  | ACCEPTED_TYPE | String        | NULL         | Assert on the fraction of rows that conform to the given data type.                     |
| HAS_DISTINCTNESS              | VALUE         | Fractional    | NULL         | Assert on the distincness of a single or combined set of features.                      |
| HAS_ENTROPY                   | VALUE         | Fractional    | NULL         | Assert on the entropy of a feature.                                                     |
| HAS_MAX                       | VALUE         | Fractional    | Quantitative | Assert on the max of a feature.                                                         |
| HAS_MAX_LENGTH                | VALUE         | String        | Categorical  | Assert on the maximum length of the feature value.                                      |
| HAS_MEAN                      | VALUE         | Fractional    | Quantitative | Assert on the mean of a feature.                                                        |
| HAS_MIN                       | VALUE         | Fractional    | Quantitative | Assert on the min of a feature.                                                         |
| HAS_MIN_LENGTH                | VALUE         | String        | Categorical  | Assert on the minimum length of the feature value.                                      |
| HAS_MUTUAL_INFORMATION        | VALUE         | Fractional    | NULL         | Assert on the mutual information between two features.                                  |
| HAS_NUMBER_OF_DISTINCT_VALUES | VALUE         | Integral      | NULL         | Assert on the number of distinct values of a feature.                                   |
| HAS_PATTERN                   | PATTERN       | String        | Categorical  | Assert on the average compliance of the feature to the regular expression.              |
| HAS_SIZE                      | VALUE         | Integral      | NULL         | Assert on the number of rows of the dataframe.                                          |
| HAS_STANDARD_DEVIATION        | VALUE         | Fractional    | Quantitative | Assert on the standard deviation of a feature.                                          |
| HAS_SUM                       | VALUE         | Fractional    | Quantitative | Assert on the sum of a feature.                                                         |
| HAS_UNIQUENESS                | VALUE         | Fractional    | NULL         | Assert on the uniqueness of a single or combined set of features.                       |
| HAS_UNIQUE_VALUE_RATIO        | VALUE         | Fractional    | NULL         | Assert on the unique value ratio of of a single or combined set of features.            |
| IS_CONTAINED_IN               | LEGAL_VALUES  | String        | NULL         | Assert that every non-null value of feature is contained in a set of predefined values. |
| IS_GREATER_THAN               | VALUE         | Fractional    | NULL         | Assert on feature A values being greater than feature B.                                |
| IS_GREATER_THAN_OR_EQUAL_TO   | VALUE         | Fractional    | NULL         | Assert on feature A values being greater than or equal to those of feature B.           |
| IS_LESS_THAN                  | VALUE         | Fractional    | NULL         | Assert on feature A values being less that feature B.                                   |
| IS_LESS_THAN_OR_EQUAL_TO      | VALUE         | Fractional    | NULL         | Assert on feature A values being less or equal to those of feature B.                   |
| IS_NON_NEGATIVE               | VALUE         | Fractional    | NULL         | Assert on feature containing non negative values.                                       |
| IS_POSITIVE                   | VALUE         | Boolean       | NULL         | Assert on a feature containing non negative values.                                     |


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

## Defining expectation rules

In general, rule values can be an exact value or a range of values. For example, if you need a feature to be ingested
if its minimum value is below zero, then you can set `min(0)` and `max(0)` but if you want the minimum to fall within
a range of `0` and `1000` then you need to set `min(0)` and `max(1000)`. See section `Expectations` below for a detailed example.

Rules that operate on tuples of features, for example `HAS_CORRELEATION`, are applied on the first two features as
defined in the expectation (as ordered within the expectation).

## Examples

=== "Python"
```python
rules=[Rule(name="HAS_MIN", level="WARNING", min=10)] # the mininum value of the feature needs to be at least 10
rules=[Rule(name="HAS_MIN", level="WARNING", max=10)] # the minimum value of the feature needs to be at most 10
rules=[Rule(name="HAS_MIN", level="WARNING", min=0, max=10)] # the minimum value of the feature needs to be between 0 and 10

rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="String", min=0.1)] # At least 10% of all instances of the feature need to be of type String
rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="String", min=0.1, max=0.5)] # 10-50% of all instances of the feature need to of type String
rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="String", min=0.1, max=0.1)] # Exactly 10% of all instances of the feature need to be of type String

rules=[Rule(name="IS_CONTAINED_IN", level="ERROR", legal_values=["a", "b"], min=0.1, max=0.1)] # Exactly 10% of all instances of the feature need to be contained in the legal_values list

rules=[Rule(name="HAS_PATTERN", level="ERROR", pattern="a+", min=0.1, max=0.1)] # Exactly 10% of all instances of the feature need to match the given pattern
```

=== "Scala"
```scala
Rule.createRule(RuleName.HAS_MIN).min(10).level(Level.WARNING).build() // the mininum value of the feature needs to be at least 10
Rule.createRule(RuleName.HAS_MIN).max(10).level(Level.WARNING).build() // the minimum value of the feature needs to be at most 10
Rule.createRule(RuleName.HAS_MIN).min(10).max(10).level(Level.WARNING).build() // the minimum value of the feature needs to be between 0 and 10

Rule.createRule(RuleName.HAS_DATATYPE).acceptedType(AcceptedType.String).min(10).level(Level.ERROR).build() // At least 10% of all instances of the feature need to be of type String
Rule.createRule(RuleName.HAS_DATATYPE).acceptedType(AcceptedType.String).max(10).level(Level.ERROR).build() // At most 10% of all instances of the feature need to be of type String
Rule.createRule(RuleName.HAS_DATATYPE).acceptedType(AcceptedType.String).min(10).max(10).level(Level.ERROR).build() // Exactly 10% of all instances of the feature need to be of type String

Rule.createRule(RuleName.IS_CONTAINED_IN).legalValues(Seq("a", "b")).min(0.1).max(0.1).level(Level.ERROR).build() // # Exactly 10% of all instances of the feature need to be contained in the legal_values list

Rule.createRule(RuleName.HAS_PATTERN).pattern("a+").min(10).max(10).level(Level.ERROR).build() // Exactly 10% of all instances of the feature need to match the given pattern
```

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

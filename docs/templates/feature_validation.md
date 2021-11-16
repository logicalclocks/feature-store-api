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


All rules are asserted based on a value provided by the users which can be set using the `min` and `max` rule properties. 
That means users must provide an exact value, or a range of values, that the rule's computed value needs to match in order for the rule to be asserted as successful. For example:
- `min(10)` and `max(10)` for the `HAS_MIN` rule means the rule will be successful only if the minimum value of a feature is exactly `10`.
- `min(10)` for the `HAS_MIN` rule means the rule will be successful if the minimum value of a feature is at least `10`.
- `max(10)` for the `HAS_MIN` rule means the rule will be successful if the maximum value of a feature is at most `10`.

Default value for min/max is `0.0`, except for compliance rules which is `1.0`.

The following table describes all the supported rule definitions along with their properties (code examples are shown in the section below).
- Name: The name of the rule.
- Predicate: The type of the predicate type of value this rule expects. 
    - Example: 
      - `IS_CONTAINED_IN` expects the `LEGAL_VALUES` property to be set.
      - Compliance rules such as `IS_LESS_THAN` expect the `feature` property to be set. That means all the expectation's features will be evaluated against the provided feature. 
- Accepted type: The data type of value set for the rules predicate or min/max properties.
    - Examples: 
      - `HAS_MIN` uses min() and max() with fractional values like min(2.5) max (5.0).
      - `HAS_SIZE` uses min() and max() with integral values like min(10) max(10)
      - `HAS_DATATYPE` expects `String` values to be provided as input to the `ACCEPTED_TYPE` predicate, `ACCEPTED_TYPE="Boolean"`.
- Feature type: The data type of the feature this rule is evaluated against. If a rule is to be applied to an incompatible feature type, an error will be thrown when the expectation is attached on the feature group.
    - Example, `HAS_MIN` can only be applied on numerical features but `HAS_DISTINCTNESS` can be applied on both numerical and categorical features.
- Description: A short description of what the rule validates.


| Name                          | Predicate     | Accepted type | Feature type | Description                                                                             |
| ------------------------------|---------------|---------------|--------------|---------------------------------------------------------------------------------------- |
| HAS_APPROX_COUNT_DISTINCT     |               | Fractional    |              | Assert on the approximate count distinct of a feature.                                  |
| HAS_APPROX_QUANTILE           |               | Fractional    | Numerical    | Assert on the approximate quantile of a feature.                                        |
| HAS_COMPLETENESS              |               | Fractional    |              | Assert on the uniqueness of a single or combined set of features.                       |
| HAS_CORRELATION               |               | Fractional    | Numerical    | Assert on the pearson correlation between two features.                                 |
| HAS_DATATYPE                  | ACCEPTED_TYPE | String        |              | Assert on the fraction of rows that conform to the given data type.                     |
| HAS_DISTINCTNESS              |               | Fractional    |              | Assert on the distinctness of a single or combined set of features.                     |
| HAS_ENTROPY                   |               | Fractional    |              | Assert on the entropy of a feature.                                                     |
| HAS_MAX                       |               | Fractional    | Numerical    | Assert on the max of a feature.                                                         |
| HAS_MAX_LENGTH                |               | String        | Categorical  | Assert on the maximum length of the feature value.                                      |
| HAS_MEAN                      |               | Fractional    | Numerical    | Assert on the mean of a feature.                                                        |
| HAS_MIN                       |               | Fractional    | Numerical    | Assert on the min of a feature.                                                         |
| HAS_MIN_LENGTH                |               | String        | Categorical  | Assert on the minimum length of the feature value.                                      |
| HAS_MUTUAL_INFORMATION        |               | Fractional    |              | Assert on the mutual information between two features.                                  |
| HAS_NUMBER_OF_DISTINCT_VALUES |               | Integral      |              | Assert on the number of distinct values of a feature.                                   |
| HAS_PATTERN                   |               | String        | Categorical  | Assert on the average compliance of the feature to the regular expression.              |
| HAS_SIZE                      |               | Integral      |              | Assert on the number of rows of the dataframe.                                          |
| HAS_STANDARD_DEVIATION        |               | Fractional    | Numerical    | Assert on the standard deviation of a feature.                                          |
| HAS_SUM                       |               | Fractional    | Numerical    | Assert on the sum of a feature.                                                         |
| HAS_UNIQUENESS                |               | Fractional    |              | Assert on the uniqueness of a feature, that is the fraction of unique values over the number of all its values.              |
| HAS_UNIQUE_VALUE_RATIO        |               | Fractional    |              | Assert on the unique value ratio of a feature, that is the fraction of unique values over the number of all distinct values. |
| IS_CONTAINED_IN               | LEGAL_VALUES  | String        |              | Assert that every non-null value of feature is contained in a set of predefined values. |
| IS_GREATER_THAN               | feature       | Fractional    |              | Assert on feature A values being greater than feature B.                                |
| IS_GREATER_THAN_OR_EQUAL_TO   | feature       | Fractional    |              | Assert on feature A values being greater than or equal to those of feature B.           |
| IS_LESS_THAN                  | feature       | Fractional    |              | Assert on feature A values being less that feature B.                                   |
| IS_LESS_THAN_OR_EQUAL_TO      | feature       | Fractional    |              | Assert on feature A values being less or equal to those of feature B.                   |
| IS_NON_NEGATIVE               |               | Fractional    | Numerical    | Assert on feature containing non negative values.                                       |
| IS_POSITIVE                   |               | Fractional    | Numerical    | Assert on a feature containing non negative values.                                     |


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
rules=[Rule(name="HAS_MIN", level="WARNING", min=10)] # the minimum value of the feature needs to be at least 10
rules=[Rule(name="HAS_MIN", level="WARNING", max=10)] # the minimum value of the feature needs to be at most 10
rules=[Rule(name="HAS_MIN", level="WARNING", min=0, max=10)] # the minimum value of the feature needs to be between 0 and 10

rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="String", min=0.1)] # At least 10% of all instances of the feature need to be of type String
rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="String", min=0.1, max=0.5)] # 10-50% of all instances of the feature need to of type String
rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="String", min=0.1, max=0.1)] # Exactly 10% of all instances of the feature need to be of type String

rules=[Rule(name="IS_CONTAINED_IN", level="ERROR", legal_values=["a", "b"], min=0.1, max=0.1)] # Exactly 10% of all instances of the feature need to be contained in the legal_values list

rules=[Rule(name="HAS_PATTERN", level="ERROR", pattern="a+", min=0.1, max=0.1)] # Exactly 10% of all instances of the feature need to match the given pattern

rules=[Rule(name="IS_LESS_THAN", level="ERROR", feature="graduation_date")] # All values of the expectation's features must be less than the graduation_date feature values (comparison is done on a per-row basis) 
rules=[Rule(name="IS_LESS_THAN", level="ERROR", feature="graduation_date", min=0.5, max=0.5)] # Same as above but only 50% of the expectation's feature values need to be less that the graduation_date feature values
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

Rule.createRule(RuleName.IS_LESS_THAN).feature("graduation_date").level(Level.ERROR).build() // All values of the expectation's features must be less than the graduation_date feature values (comparison is done on a per-row basis)
Rule.createRule(RuleName.IS_LESS_THAN).feature("graduation_date").min(0.5).max(0.5).level(Level.ERROR).build() // Same as above but only 50% of the expectation's feature values need to be less that the graduation_date feature values
```

## Properties
{{rule}}

{{rule_properties}}


# Expectations

A set of rule instances that are applied on a set of features. Expectations are created at the feature store level
and can be attached to multiple feature groups. If an expectation contains no features, it will be applied to all the
features of the feature group when the validation is done An expectation contains a list of features it is applied to.
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

Create an expectation with a rule to assert that no feature has a null value:

=== "Python"
    ```python
    expectation_notnull = fs.create_expectation("not_null",
                                                description="Assert no feature is null",
                                                rules=[Rule(name="HAS_DATATYPE", level="ERROR", accepted_type="Null", max=0)])
    expectation_notnull.save()
    ```

=== "Scala"
    ```scala
    val expectationNotNull = (fs.createExpectation()
                                .rules(Seq(
                                    Rule.createRule(RuleName.HAS_DATATYPE).max(0).acceptedType(AcceptedType.Null).level(Level.ERROR).build()))
                                .name("not_null")
                                .description("Assert no feature is null")
                                .build())
    expectationNotNull.save()
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

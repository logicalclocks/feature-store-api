# Expectation Suite

{{expectation_suite}}

## Creation with Great Expectations

```python3
import great_expectations as ge

expectation_suite = ge.core.ExpectationSuite(
    "new_expectation_suite",
    expectations=[
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_max_to_be_between",
            kwargs={
                "column": "feature",
                "min_value": -1,
                "max_value": 1
            }
        )
    ]
)
```

## Attach to Feature Group

{{expectation_suite_attach}}

## Single Expectation API

An API to edit the expectation list based on Great Expectations API.

{{single_expectation_api}}

## Properties

{{expectation_suite_properties}}

## Methods

{{expectation_suite_methods}}

@GroupBy
Feature: LookUp - Verify GroupBy Plugin Error scenarios

  @GCS_CSV_GROUPBY_TEST
  Scenario:Verify GroupBy plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Group By" from the plugins list as: "Analytics"
    Then Navigate to the properties page of plugin: "Group By"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | groupByFields |
      | aggregates    |

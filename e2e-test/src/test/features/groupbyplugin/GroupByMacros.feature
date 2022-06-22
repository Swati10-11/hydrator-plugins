@GroupBy
Feature:GroupBy - Verification of GroupBy pipeline with GCS as source and File as target using macros

  @GCS_CSV_GROUPBY_TEST @File_Sink @FILE_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to File successfully with GroupBy plugin properties as macro arguments
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Group By" from the plugins list as: "Analytics"
    Then Connect plugins: "GCS" and "Group By" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "GCSReferenceName"
    Then Enter input plugin property: "path" with value: "gcsSourcePath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Capture the generated Output Schema
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Group By"
    Then Click on the Macro button of Property: "groupByFields" and set the value to: "groupByFields"
    Then Click on the Macro button of Property: "Aggregates" and set the value to: "groupByAggregates"
    Then Click on the Macro button of Property: "numberOfPartitions" and set the value to: "groupByNumberOfPartitions"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "filePluginOutputFolder"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "<FileFormat>"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "groupByFields" for key "groupByFields"
    Then Enter runtime argument value "Aggregates" for key "groupByAggregates"
    Then Enter runtime argument value "numberOfPartitions" for key "groupByNumberOfPartitions"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on the Preview Data link on the Sink plugin node: "BigQueryTable"
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "groupByFields" for key "groupByFields"
    Then Enter runtime argument value "Aggregates" for key "groupByAggregates"
    Then Enter runtime argument value "numberOfPartitions" for key "groupByNumberOfPartitions"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

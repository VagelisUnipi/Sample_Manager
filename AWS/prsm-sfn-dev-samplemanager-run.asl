{
  "Comment": "Metadata-Driven LIMS Ingestion Pipeline",
  "StartAt": "DiscoverAndConfigLambda",
  "States": {
    "DiscoverAndConfigLambda": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "lims-config-router-lambda",
        "Payload": {}
      },
      "OutputPath": "$.Payload",
      "Next": "ProcessFilesMap"
    },
    "ProcessFilesMap": {
      "Type": "Map",
      "ItemsPath": "$.Files",
      "MaxConcurrency": 10,
      "Iterator": {
        "StartAt": "ParallelProcessing",
        "States": {
          "ParallelProcessing": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
              {
                "StartAt": "CopyToReplicatedVault",
                "States": {
                  "CopyToReplicatedVault": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:s3:copyObject",
                    "Parameters": {
                      "Bucket.$": "$.ReplicatedCopyParams.Bucket",
                      "Key.$": "$.ReplicatedCopyParams.Key",
                      "CopySource.$": "$.ReplicatedCopyParams.CopySource"
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "ExecuteGlueTransformation",
                "States": {
                  "ExecuteGlueTransformation": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "lims_ingestion_job",
                      "Arguments.$": "$.GlueArguments"
                    },
                    "End": true
                  }
                }
              }
            ]
          }
        }
      },
      "Next": "LoadRedshiftDw"
    },
    "LoadRedshiftDw": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "lims-redshift-loader-lambda",
        "Payload": {
          "table_name": "all"
        }
      },
      "OutputPath": "$.Payload",
      "Next": "CheckDwLoadResult"
    },
    "CheckDwLoadResult": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.statusCode",
          "NumericEquals": 200,
          "Next": "DwLoadSucceeded"
        }
      ],
      "Default": "DwLoadFailed"
    },
    "DwLoadSucceeded": {
      "Type": "Succeed"
    },
    "DwLoadFailed": {
      "Type": "Fail",
      "Error": "DwLoadError",
      "Cause": "Redshift DW load returned a non-200 status; see the loader Lambda logs."
    }
  }
}

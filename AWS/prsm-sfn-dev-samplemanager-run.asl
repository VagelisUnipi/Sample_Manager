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
      "End": true
    }
  }
}
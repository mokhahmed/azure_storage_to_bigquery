#!/bin/sh

TEMPLATE=$1
AZ_STORAGE_ACCOUNT=$2
AZ_CONTAINER_NAME=$3
AZ_INPUT_LOCATION=$4
AZ_SAS_TOKEN=$4
INPUT_LOCATION="wasbs://$AZ_CONTAINER_NAME@$AZ_STORAGE_ACCOUNT.blob.core.windows.net/$AZ_INPUT_LOCATION"
INPUT_FORMAT=$5
WRITE_MODE=$6
OUTPUT_TABLE=$7
TEMP_BUCKET=$8

gcloud beta dataproc batches submit \
--project ma-sabre-sandbox-01 \
--region us-central1 pyspark \
--batch batch-d1bf19 $TEMPLATE \
--version 1.0 \
--jars gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.1-preview.jar,gs://{bucket_jars}/delta-core_2.12-1.1.0.jar \
--subnet default \
--  --input.location $INPUT_LOCATION \
    --bigquery.input.format $INPUT_FORMAT \
    --bigquery.temp.bucket.name $TEMP_BUCKET \
    --bigquery.output.mode $WRITE_MODE \
    --bigquery.output.table $OUTPUT_TABLE \
    --azure.storage.account $AZ_STORAGE_ACCOUNT \
    --azure.container.name $AZ_CONTAINER_NAME \
    --azure.sas $AZ_SAS_TOKEN
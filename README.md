# Azure Storage to BigQuery

Azure Blob Storage is Microsoft's object storage solution for the cloud. Blob Storage is optimized for storing massive amounts of unstructured data. Clients usually use Azure Data Lake Storage (Gen1, Gen2) built on top of Azure Blob Storage  to store enterprise-wide hyper-scale repositories for big data and data warehouse analytic workloads. On the other hand BigQuery is Google Cloud's fully managed, petabyte scale, and cost effective analytics data warehouse. In this document we will explore different techniques for data migration from Azure Data Lake Storage into BigQuery. 


## Prerequisites
1. Configure Access by creating or using an existing Microsoft Azure Storage user to access the storage account for your Microsoft Azure Storage Blob container.

2. Create a shared access signatures (SAS) token at the container level save the sas token which will be used later to access 
Store SAS token at  Secret Manger <br />

   * Enable Secret Manger API.<br />
   ``` gcloud services enable secretmanager.googleapis.com ``` <br />

   * Create SAS Token secret. <br />
   ``` gcloud secrets create SAS-TOKEN --replication-policy="automatic"``` <br />

   * Store the SAS Token generated form step 2 at GCP secret Manager.<br />
   ``` gcloud secrets versions add SAS-TOKEN --data-file="/path/to/sas-token.txt" ``` <br />


## 1. Storage Transfer Service (STS) 
Storage Transfer Service enables you to quickly and securely transfer data to, from, and between object and file storage systems, including Google Cloud Storage, Amazon S3, Azure Storage, on-premises data, and more. Depending on your source type, you can easily create and run Google-managed transfers or configure self-hosted transfers that give you full control over network routing and bandwidth usage. It makes it easy to perform large-scale online data transfers to migrate data to GCP, archive cold data to GCS, replicate data for business continuity, or transfer data for analytics and machine learning in the cloud. <br/><br/>

![alt text](https://github.com/mokhahmed/azure_storage_to_bigquery/blob/main/storage_transfer_service/reference_architecture.png?raw=true)


1. Create PubSub topic to get a notification when the transfer completed <br />
    ```gcloud pubsub topics create az-to-gcs-sts-notifications```


2. Create Storage Transfer Service Scheduled Jobs
    
    ```
      name=az_2_bq_sts_job
      source=https://{storage_account}.blob.core.windows.net/{source_folder}
      sink=gs://{landing_bucket}/{target_folder}
      sas_token= $(gcloud secrets versions access latest --secret=<SAS-TOKEN>)
      notification_topic=projects/$project_id/topics/az-to-gcs-sts-notifications 
      
      echo "{\"sasToken\": \"$sas_token\"}" > creds.json 

      gcloud transfer jobs create $source $sink \
      --name=$name \
      --source-creds-file='creds.json' \
      --overwrite-when='different' \
      --include-modified-after-relative=1d \
      --notification-pubsub-topic=$notification_topic \
      --notification-event-types='failed','aborted','success' \
      --notification-payload-format='json' \
      --schedule-repeats-every=1d
    ```
    
    Or, execute the shell script to create the sts job 
    
    ```
      project_id= <PROJECT_ID>
      name=az_2_bq_sts_job
      source=https://{storage_account}.blob.core.windows.net/{source_folder}
      sink=gs://{landing_bucket}/{target_folder}
      sas_token= $(gcloud secrets versions access latest --secret=<SAS-TOKEN>)
      notification_topic=az-to-gcs-sts-notifications 

      sh az_storage_to_bq_transfer.sh az_2_bq_sts_job $name $source $sink $sas_token $project_id $notification_topic
      
    ```
    
    Or,  trigger cloud build to create the sts job 
    
    ```
    gcloud builds submit --config=cloudbuild.yaml --substitutions=_NAME=$name, $_SOURCE=$source, $_SINK=$sink, $_JOB_PROJECT_ID=$project_id, $_NOTIFICATION_JOB=$notification_topic
    ```
3. Once the STS job is completed it will push a status notification to az-to-gcs-sts-notifications pubsub topic.
Cloud Function will be triggered to 
    * Copy all the files in the landing bucker folder into a temp _processing files directory 
   
        ```gsutil mv  gs://{landing_bucket}/{target_folder}/*format gs://{landing_bucket}/{target_folder}/_processing/ ``` 

   * At this point there are many options that could be used here based on the use case, customer requirement “ masking data before loading it into BQ”,  file formats “ supported , ELT vs ETL , Batch vs Streaming load and so on. We will list here the main 3 approaches for loading data from GCS into BQ 
        * BigQuery batch load using [BQ Load](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table)
        
              ``` 
                bq load 
                --source_format=CSV 
                mydataset.mytable  
                gs://{landing_bucket}/{target_folder}/_processing/*.csv 
                ./myschema.json
              ``` 

        * [BigQuery Client Libraries](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table)
           ex. the below python code could be used to load file into BQ 
          
          ```
            from google.cloud import bigquery

            client = bigquery.Client()
            # table_id = "<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>"

            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("name", "STRING"),
                    ....
                ],
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
            )
            uri = "gs://path/to/input/files"

            load_job = client.load_table_from_uri( uri, table_id, job_config=job_config )

            print("Job Status {} .".format(  load_job.result()  )) 

          ```
        
        * Dataflow Batch template.<br/> 
              ex. if the input format is Text/CSV files you can use [GCS_Text_to_BigQuery](https://github.com/GoogleCloudPlatform/dataproc-templates/tree/e4774330c27bdf26b34871f59e8d04123ed28468/python/dataproc_templates/gcs#gcs-to-bigquery). You can find the list of all avaliable dataflow templates [here](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
              
              ```
              
                JOB_NAME= GCS_TEXT_TO_BQ_JOB
                REGION_NAME = us-central1
                JAVASCRIPT_FUNCTION= parse_events
                PATH_TO_JAVASCRIPT_UDF_FILE = gs://path/to/transformer.js
                BIGQUERY_TABLE= <PROJECT_ID>.<DATASET>.<TABLE_ID>
                PATH_TO_BIGQUERY_SCHEMA_JSON= gs://path/to/schema.json
                PATH_TO_TEXT_DATA= gs://path/to/input/files
                PATH_TO_TEMP_DIR_ON_GCS=gs://path/to/temp_df_bucket
                
                gcloud dataflow jobs run JOB_NAME 
                    --gcs-location gs://dataflow-templates/latest/GCS_Text_to_BigQuery 
                    --region REGION_NAME 
                    --parameters 
                javascriptTextTransformFunctionName=JAVASCRIPT_FUNCTION,
                JSONPath=PATH_TO_BIGQUERY_SCHEMA_JSON,
                javascriptTextTransformGcsPath=PATH_TO_JAVASCRIPT_UDF_FILE,
                inputFilePattern=PATH_TO_TEXT_DATA,
                outputTable=BIGQUERY_TABLE,
                bigQueryLoadingTemporaryDirectory=PATH_TO_TEMP_DIR_ON_GCS
                
              ```

      * Dataproc serverless batch template.<br/>
        
          you can use dataproc [GCS_TO_BIGQUERY](https://github.com/GoogleCloudPlatform/dataproc-templates/tree/e4774330c27bdf26b34871f59e8d04123ed28468/python/dataproc_templates/gcs#gcs-to-bigquery) batch template to load files from gcs bucket into bigquery
          
          ```
            export GCP_PROJECT=<project_id>
            export REGION=<region>
            export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
            export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

            ./bin/start.sh \
            -- --template=GCSTOBIGQUERY \
                --gcs.bigquery.input.format="<json|csv|parquet|avro>" \
                --gcs.bigquery.input.location="<gs://bucket/path>" \
                --gcs.bigquery.output.dataset="<dataset>" \
                --gcs.bigquery.output.table="<table>" \
                --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists>\
                --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
          ```
          
      * Copy all files from processing into an archiving bucket. <br/>
        ```gsutil mv gs://{landing_bucket}/{target_folder}/_processing/*.format  gs://{archive_bucket}/{target_folder}/{day_of_year}/_processed/  ``` 

  
<br/><br/>
## 2. Dataproc ( Spark Serverless Batch Jobs) 

Dataproc is a managed Spark and Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning. Dataproc automation helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them. With less time and money spent on administration, you can focus on your jobs and your data. <br/><br/>

![alt text](https://github.com/mokhahmed/azure_storage_to_bigquery/blob/main/dataproc_template/reference_architecture.png?raw=true)

Run Azure storage to bigquery dataproc template to read directly from azure and write bigquery you only need to configure sas token that we created from previous step 

***Note: in case of delta.io input format you need to provide delta dependencies `delta-core.jar` and upload it to gs://{bucket_jars} for the demo i used dataproc serverless (PySpark) Runtime version 1.0 (Spark 3.2, Java 11, Scala 2.12)  which require delta version 1.1.0 you can check the delta and spark dependencies [here](https://docs.delta.io/latest/releases.html)***
  
``` 

  TEMPLATE= AZURE_STORAGE_TO_BQ
  AZ_STORAGE_ACCOUNT= <AZURE_STORAGE_ACCOUNT>
  AZ_CONTAINER_NAME= <AZURE_STORAGE_ACCOUNT> 
  AZ_INPUT_LOCATION= <AZURE_STORAGE_ACCOUNT> 
  AZ_SAS_TOKEN= $(gcloud secrets versions access latest --secret=<SAS-TOKEN>)
  INPUT_FORMAT=delta|csv|parquet|orc|avro
  WRITE_MODE=append|overwrite
  OUTPUT_TABLE=<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>
  TEMP_BUCKET=<TEMP_BUCKET>

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

``` 

Or run the template script

``` 
  
  TEMPLATE= AZURE_STORAGE_TO_BQ
  AZ_STORAGE_ACCOUNT= <AZURE_STORAGE_ACCOUNT>
  AZ_CONTAINER_NAME= <AZURE_STORAGE_ACCOUNT> 
  AZ_INPUT_LOCATION= <AZURE_STORAGE_ACCOUNT> 
  AZ_SAS_TOKEN= $(gcloud secrets versions access latest --secret=<SAS-TOKEN>)
  INPUT_FORMAT=delta|csv|parquet|orc|avro
  WRITE_MODE=append|overwrite
  OUTPUT_TABLE=<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>
  TEMP_BUCKET=<TEMP_BUCKET>
  
  sh dataproc_template/run_template.sh  $TEMPLATE $AZ_STORAGE_ACCOUNT $AZ_CONTAINER_NAME $AZ_INPUT_LOCATION $INPUT_FORMAT $WRITE_MODE $OUTPUT_TABLE $TEMP_BUCKET
  
 ```

<br/><br/>

## Quick Summary 

||Storage Transfer Service|Dataproc|
| :---        |    :----:   |          ---: |
|Transfer Time | 3 mins for 15GB | 15 mins for 15GB (cluster of: 1 driver, 4 workers) |
|Cost | No charges incurred  | ~ 20 USD monthly  Cost could be dropped by for dataproc serverless batch jobs |
|Scheduling |  Scheduled jobs or Triggered by API calls | Scheduled batch jobs or Triggered by API calls |
|Security | Data encrypted at transit |  Data encrypted at transit and you can apply data masking or encrption at transit|
|Team Skill  | Fully managed service.  | Managed Hadoop environment. <br/>DataProc serverless is fully managed spark for batch jobs | 
|Technology | Storage transfer service “ file replication”  | Open source apache spark | 
|Data Format | No restrictions since it’s a file replication tool  | Limited to Spark supported file formats | 



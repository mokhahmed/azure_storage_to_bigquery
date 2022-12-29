# Azure Storage to BigQuery

Azure Blob Storage is Microsoft's object storage solution for the cloud. Blob Storage is optimized for storing massive amounts of unstructured data. Clients usually use Azure Data Lake Storage (Gen1, Gen2) built on top of Azure Blob Storage  to store enterprise-wide hyper-scale repositories for big data and data warehouse analytic workloads. On the other hand BigQuery is Google Cloud's fully managed, petabyte scale, and cost effective analytics data warehouse. In this document we will explore different techniques for data migration from Azure Data Lake Storage into BigQuery. 


## Prerequisites
1. Configure Access by creating or using an existing Microsoft Azure Storage user to access the storage account for your Microsoft Azure Storage Blob container.

2. Create a shared access signatures (SAS) token at the container level save the sas token which will be used later to access 
Store SAS token at  Secret Manger 
  a. Enable Secret Manger 
  ``` gcloud services enable secretmanager.googleapis.com ``` 


  b. Create SAS-TOKEN secret 
  ``` gcloud secrets create SAS-TOKEN --replication-policy="automatic"``` 


  c. Store the SAS-TOKEN generated form step 2 
  ``` gcloud secrets versions add SAS-TOKEN --data-file="/path/to/sas-token.txt" ``` 




## 1. Storage Transfer Service ( STS) 
Storage Transfer Service enables you to quickly and securely transfer data to, from, and between object and file storage systems, including Google Cloud Storage, Amazon S3, Azure Storage, on-premises data, and more. Depending on your source type, you can easily create and run Google-managed transfers or configure self-hosted transfers that give you full control over network routing and bandwidth usage. It makes it easy to perform large-scale online data transfers to migrate data to GCP, archive cold data to GCS, replicate data for business continuity, or transfer data for analytics and machine learning in the cloud.
![alt text](https://github.com/mokhahmed/azure_storage_to_bigquery/blob/main/storage_transfer_service/reference_architecture.png?raw=true)


## 2. Dataproc ( Spark Serverless Batch Jobs) 

Dataproc is a managed Spark and Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning. Dataproc automation helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them. With less time and money spent on administration, you can focus on your jobs and your data. 

![alt text](https://github.com/mokhahmed/azure_storage_to_bigquery/blob/main/dataproc_template/reference_architecture.png?raw=true)


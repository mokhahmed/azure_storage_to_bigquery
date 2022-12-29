# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint
from pyspark.sql import SparkSession, DataFrame


# Output mode
OUTPUT_MODE_OVERWRITE = "overwrite"
OUTPUT_MODE_APPEND = "append"
OUTPUT_MODE_IGNORE = "ignore"
OUTPUT_MODE_ERROR_IF_EXISTS = "errorifexists"
INPUT_LOCATION = "input.location"
BQ_OUTPUT_TABLE = "bigquery.output.table"
BQ_OUTPUT_MODE = "bigquery.output.mode"
BQ_INPUT_FORMAT = "bigquery.input.format"
BQ_LD_TEMP_BUCKET_NAME = "bigquery.temp.bucket.name"
AZ_STORAGE_ACCOUNT= "azure.storage.account"
AZ_CONTAINER_NAME=  "azure.container.name"
AZ_SAS_TOKEN="azure.sas"
BQ_TEMP_BUCKET = "temporaryGcsBucket"
FORMAT_JSON = "json"
FORMAT_CSV = "csv"
FORMAT_DELTA_IO = "delta"
FORMAT_TXT = "txt"
FORMAT_AVRO = "avro"
FORMAT_PARQUET = "parquet"
FORMAT_DELTA = "delta"
FORMAT_AVRO_EXTD = "com.databricks.spark.avro"
HEADER = "header"
INFER_SCHEMA = "inferSchema"
FORMAT_BIGQUERY = "bigquery"
TABLE = "table"


class AZStorageToBigQueryTemplate:
    """
    Dataproc template implementing loads from Azure Storage into BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{AZ_STORAGE_ACCOUNT}',
            dest=AZ_STORAGE_ACCOUNT,
            required=True,
            help='Azure Storage Account'
        )

        parser.add_argument(
            f'--{AZ_CONTAINER_NAME}',
            dest=AZ_CONTAINER_NAME,
            required=True,
            help='Azure Account Name'
        )

        parser.add_argument(
            f'--{AZ_SAS_TOKEN}',
            dest=AZ_SAS_TOKEN,
            required=True,
            help='Azure SAS TOKEN'
        )

        parser.add_argument(
            f'--{INPUT_LOCATION}',
            dest=INPUT_LOCATION,
            required=True,
            help='Location of the input files'
        )

        parser.add_argument(
            f'--{BQ_OUTPUT_TABLE}',
            dest=BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )

        parser.add_argument(
            f'--{BQ_INPUT_FORMAT}',
            dest=BQ_INPUT_FORMAT,
            required=True,
            help='Input file format (one of: avro, parquet, csv, json, delta)',
            choices=[
                FORMAT_AVRO,
                FORMAT_PARQUET,
                FORMAT_CSV,
                FORMAT_JSON,
                FORMAT_DELTA
            ]
        )

        parser.add_argument(
            f'--{BQ_LD_TEMP_BUCKET_NAME}',
            dest=BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )

        parser.add_argument(
            f'--{BQ_OUTPUT_MODE}',
            dest=BQ_OUTPUT_MODE,
            required=False,
            default=OUTPUT_MODE_APPEND,
            help=(
                'Output write mode '
                '(one of: append,overwrite,ignore,errorifexists) '
                '(Defaults to append)'
            ),
            choices=[
                OUTPUT_MODE_OVERWRITE,
                OUTPUT_MODE_APPEND,
                OUTPUT_MODE_IGNORE,
                OUTPUT_MODE_ERROR_IF_EXISTS
            ]
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)


    def __read_from_gcs(self, spark, input_file_format, input_file_location):

        if input_file_format == FORMAT_PARQUET:
            return spark.read.parquet(input_file_location)

        elif input_file_format == FORMAT_AVRO:
            return spark.read.format(FORMAT_AVRO_EXTD).load(input_file_location)

        elif input_file_format == FORMAT_CSV:
            return (spark.read
                    .format(FORMAT_CSV)
                    .option(HEADER, True)
                    .option(INFER_SCHEMA, True)
                    .load(input_file_location))


        elif input_file_format == FORMAT_JSON:
            return spark.read.json(input_file_location)

        elif input_file_format == FORMAT_DELTA:
            return spark.read.format(FORMAT_DELTA).load(input_file_location)


    def __write_to_bq(self, input_data, output_mode, bq_table, bq_temp_bucket):
        (   input_data.write
            .format(FORMAT_BIGQUERY)
            .option(TABLE,bq_table)
            .option(BQ_TEMP_BUCKET, bq_temp_bucket)
            .mode(output_mode)
            .save()
            )


    def run(self, spark, args: Dict[str, Any]) -> None:

        log_4j_logger = spark.sparkContext._jvm.org.apache.log4j
        logger: Logger = log_4j_logger.LogManager.getLogger(__name__)

        # Arguments
        input_file_location: str = args[INPUT_LOCATION]
        big_query_table: str = args[BQ_OUTPUT_TABLE]
        input_file_format: str = args[BQ_INPUT_FORMAT]
        bq_temp_bucket: str = args[BQ_LD_TEMP_BUCKET_NAME]
        output_mode: str = args[BQ_OUTPUT_MODE]
        storage_account: str = args[AZ_STORAGE_ACCOUNT]
        container_name: str = args[AZ_CONTAINER_NAME]
        sas_token: str = args[AZ_SAS_TOKEN]

        spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net", sas_token)
        logger.info(
            "Starting Azure to Bigquery spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        # Read
        logger.info(f"Starting Reading from input path:\n {input_file_location}")
        input_data: DataFrame = self.__read_from_gcs(spark, input_file_format, input_file_location)

        # Write
        self.__write_to_bq(input_data, output_mode, big_query_table, bq_temp_bucket)



if __name__ == '__main__':
    import uuid
    app_name = "AZ_STORAGE_TO_BQ_" + str(uuid.uuid4())[:6]
    template = AZStorageToBigQueryTemplate()
    args: Dict[str, Any] = template.parse_args()
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    template.run(spark, args)
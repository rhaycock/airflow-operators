# -*- coding: utf-8 -*-
import sys

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import textwrap
import csv
import os

import logging

log = logging.getLogger()

class PostgresToGCSOperator(BaseOperator):
    '''
    UNLOAD POSTGRESQL DATA TO A GCS BUCKET

    :param table: source (PostgreSQL) table
    :type table: string
    :param schema: source (PostgreSQL) schema
    :type schema: string
    :param database: source (PostgreSQL) database
    :type database: string
    :param filename: suffix to add to the final filename (do not include extensions such as '.csv')
    :type filename: string
    :param bucket: name of destination GCS bucket (do not include 'gs://')
    :type bucket: string
    :param sql_statement: the SQL statement to execute which will create the resulting data to be moved into GCS (can be a file path to a sql file)
    :type sql_statement: string
    :param max_file_size_bytes: the maximum csv file size that will be written to GCS. If the resulting file is larger, a new file will be created
    :param max_file_size_bytes: int
    :param google_cloud_storage_conn_id: the Google cloud storage connection ID to connect to GCS
    :type google_cloud_storage_conn_id: string
    :param postgres_conn_id: the conncetion ID to connect to PostgreSQL
    :type postgres_conn_id: string
    :param cadence: the frequency of the dag
    :type cadence: string
    :param dt: the execution date of the dag
    :type dt: string
    :param hour: the execution hour of the dag
    :type hour: string
    :param compression: whether to compress (gzip) the destination csv files in GCS or not
    :type compression: boolean
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param field_delimiter: The delimiter to be used for CSV files.
    :type field_delimiter: string
    :param is_date_partitioned: does the source PostgreSQL table have a date column which we are using as base line for querying
    :type is_date_partitioned: boolean
    :param params: a dictionary of parameters to be passed into templated fields, such as a SQL query
    :type params: dict
    '''

    template_fields = ('table','bucket', 'sql_statement', 'hour','dt','params')
    template_ext = ('.sql',)
    ui_color = '#22bef2'

    @apply_defaults
    def __init__(
            self,
            table,
            schema,
            database,
            filename,
            bucket,
            sql_statement,
            max_file_size_bytes=1900000000,
            google_cloud_storage_conn_id='gcp_default',
            postgres_conn_id='postgres_default',
            cadence='@daily',
            dt='',
            hour='',
            compression=False,
            delegate_to=None,
            field_delimiter=',',
            is_date_partitioned=True,
            params={},
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.bucket = bucket
        self.sql_statement = sql_statement
        self.max_file_size_bytes = max_file_size_bytes
        self.table = table
        self.schema = schema
        self.cadence = cadence
        self.filename = filename
        self.database = database
        self.dt = dt
        self.hour = hour
        self.compression = compression
        self.delegate_to = delegate_to
        self.field_delimiter = field_delimiter
        self.is_date_partitioned = is_date_partitioned
        self.params = params

    def execute(self, context):
        
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()
        # Get Table Column metadata
        log.info(f'Fetching schema fields')
        _sql_columns = f'select column_name from {self.database}.information_schema.columns where table_catalog = \'{self.database}\' AND table_schema = \'{self.schema}\' AND table_name = \'{self.table}\''
        log.info(f'Running schema field query: {_sql_columns}')
        _cursor_schema = conn.cursor()
        _cursor_schema.execute(_sql_columns)
        tmp_schema_fields = _cursor_schema.fetchall()
        log.info(f'Columns: {tmp_schema_fields}')
        schema_fields = []
        for x in tmp_schema_fields:
            schema_fields.append(x[0])

        sql = self.sql_statement
        sql = textwrap.dedent(sql.replace('    ','')).strip()

        file_no = 0
        # Create file handle (filename):
        if self.is_date_partitioned:
            tmp_file_handle = f'/tmp/{self.dt}_{self.table}_raw_{file_no}.csv'
            filename = f'{self.dt}_{self.table}_{self.filename}_{file_no}.csv'
        else:
            tmp_file_handle = f'/tmp/{self.table}_raw_{file_no}.csv'
            filename = f'{self.table}_{self.filename}_{file_no}.csv'

        _cursor = conn.cursor()
        file_mime_type = 'text/csv'
        files_to_upload = [{
            'file_name': filename,
            'file_handle': tmp_file_handle,
            'file_mime_type': file_mime_type
        }]
        log.info(f'executing query: \n"{sql}"')
        i = 0
        try:
            _cursor.execute(sql)
            with open(tmp_file_handle, 'w', newline='', encoding='utf-8') as f:
                csv_writer = csv.writer(f,
                                    delimiter=self.field_delimiter)
                csv_writer.writerow(schema_fields)
                for row in _cursor:
                    csv_writer.writerow(row)
                    # Stop if the file exceeds the file size limit.
                    # if os.path.getsize(tmp_file_handle) >= self.max_file_size_bytes:
                    #     log.info(f'file too large, current file: {file_no}')
                    #     file_no += 1
                    #     # Rename the file handle and filename with the new number, since we are now adding more files
                    #     tmp_file_handle = f'/tmp/{self.dt}_{self.table}_raw_{file_no}.csv'
                    #     filename = f'{self.dt}_{self.table}_{self.filename}_{file_no}.csv'
                    #     log.info(f'renamed {tmp_file_handle} and {filename}')
                    #     files_to_upload.append({
                    #         'file_name': filename,
                    #         'file_handle': tmp_file_handle,
                    #         'file_mime_type': file_mime_type
                    #     })

                    #     # Write the header to the new file
                    #     log.info(f'writing schema fields to CSV file now')
                    #     # csv_writer = csv.writer(f,
                    #     #                 delimiter=self.field_delimiter)
                    #     with open(tmp_file_handle, 'w', newline='', encoding='utf-8') as f:
                    #         csv_writer = csv.writer(f,
                    #                 delimiter=self.field_delimiter)
                    #         csv_writer.writerow(schema_fields)
                    #         log.info(f'schema fields written')
        except:
            self.log.error(f"Unexpected error:{sys.exc_info()[0]}")
            raise
        finally:
            _cursor.close()

        # Create a GCS hook so we can delete old objects in the event of a backfill
        gcs = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        
        log.info(f"Deleting existing files in bucket {self.bucket}")
        # Determine the folder path in GCS to delete files from (that is, this is the entire filename, except we remove the filename at the end)
        if self.is_date_partitioned:
            if self.cadence == '@daily':
                prefix = f"{self.database}/{self.schema}/{self.table}/{self.dt}"
            elif self.cadence == '@hourly':
                prefix = f"{self.database}/{self.schema}/{self.table}/{self.dt}/{self.hour}"
        else:
            prefix = f"{self.database}/{self.schema}/{self.table}"
        # List current objects in the bucket blob and delete them
        old_object_list = gcs.list(self.bucket, prefix=prefix)
        for old_object in old_object_list:
            gcs.delete(self.bucket, old_object)

        # self._upload_to_gcs(f'{prefix}/{files_to_upload}')
        log.info(f'Uploading files to bucket {self.bucket}')
        self._upload_to_gcs(f'{prefix}', files_to_upload)

    def getFirstColumnListAsString(self,data):
        return ', \n'.join([f'{x[0]}' for x in data])

    def _upload_to_gcs(self, prefix, files_to_upload):
        """
        Upload all of the file splits to Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for tmp_file in files_to_upload:
            file_name = tmp_file.get('file_name')
            object_name = f'{prefix}/{file_name}'
            hook.upload(# bucket name without 'gs://'
                bucket=self.bucket,
                # filepath without the bucketname, including the target filename
                object=object_name,
                filename=tmp_file.get('file_handle'),
                mime_type=tmp_file.get('file_mime_type'),
                gzip=self.compression)
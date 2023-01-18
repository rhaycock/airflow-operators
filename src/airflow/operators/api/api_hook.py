# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime, timedelta
from random import random
from time import sleep
import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

log = logging.getLogger()


class ApiHook(HttpHook):

    def __init__(
            self,
            endpoint='',
            method='GET',
            bucket='',
            prefix='',
            dt='',
            hour=None,
            isDatePartitioned=False,
            overwrite=False,
            params={},
            http_conn_id='http_default',
            google_cloud_storage_conn_id = 'gcp_default'
    ) -> None:
        super().__init__()
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.params = params
        self.method = method.upper()
        self.bucket = bucket
        self.prefix = prefix
        self.dt = dt
        self.hour = hour
        self.isDatePartitioned = isDatePartitioned
        self.overwrite = overwrite
        http_connection = self.get_connection(http_conn_id)
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id

    def call_endpoint(self, endpoint, params):

        try:
            response = super().run(endpoint=endpoint,
                                    data=params,
                                    headers={"Content-Type": "application/json"})
        except AirflowException as exc:
            if str(exc).startswith("404"):
                return None
            elif str(exc).startswith("429"):  # several parallel requests
                log.warning('waiting to avoid parallel API requests')
                sleep(random() * 5)
                response = super().run(endpoint=endpoint,
                                        data=params,
                                        headers={"Content-Type": "application/json"})
            else:
                raise exc

        if self.isDatePartitioned:
            if self.hour is None:
                self.prefix = f'{self.prefix}/{self.dt}'
            elif self.hour is not None:
                self.prefix = f'{self.prefix}/{self.dt}/{self.hour}'
        else:
            pass

        json_data = response.json()

        with open('json_data.json', 'w') as file:
            for report in json_data['data']:
                for key in report:
                        json.dump(report[key], file)
                        file.write('\n')

        self.upload_file('json_data.json', self.bucket, self.prefix, google_cloud_storage_conn_id=self.google_cloud_storage_conn_id, overwrite=self.overwrite)

    def upload_file(self,
                    filename,
                    bucket,
                    prefix,
                    google_cloud_storage_conn_id='gcp_default',
                    overwrite=False):
        
        gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=google_cloud_storage_conn_id)

        object_key = f'{prefix}/{filename}'

        path_from_file = 'json_data.json'

        if overwrite:
            if gcs_hook.exists(bucket=bucket,
                                object=object_key):
                gcs_hook.delete(bucket=bucket,
                                object=object_key)

        gcs_hook.upload(bucket=bucket,
                        object=object_key,
                        filename=path_from_file)
        if os.path.exists(path_from_file):
            log.info('Deleting path_from_file: %s', path_from_file)
            os.remove(path_from_file)

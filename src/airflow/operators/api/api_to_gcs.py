# -*- coding: utf-8 -*-
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from api.api_hook import ApiHook

log = logging.getLogger()

class ApiToGCSOperator(BaseOperator):

    template_fields = ('dt','hour',)

    @apply_defaults
    def __init__(
            self,
            endpoint='',
            isDatePartitioned=False,
            overwrite=False,
            dt='',
            hour=None,
            params={},
            bucket='bucket_here',
            prefix='prefix_here',
            google_cloud_storage_conn_id='gcp_default',
            *args, **kwargs):
        super(ApiToGCSOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.isDatePartitioned = isDatePartitioned
        self.overwrite = overwrite
        self.dt = dt
        self.hour = hour
        self.bucket = bucket
        self.prefix = prefix
        self.params = params
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id

    def execute(self, context):

        api_hook = ApiHook(endpoint=self.endpoint,
                                    bucket=self.bucket,
                                    prefix=self.prefix,
                                    dt=self.dt,
                                    hour=self.hour,
                                    isDatePartitioned=self.isDatePartitioned,
                                    overwrite=self.overwrite,
                                    params=self.params,
                                    google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)

        response_data = api_hook.call_endpoint(endpoint=self.endpoint,
                                    params=self.params)

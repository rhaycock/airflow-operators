# -*- coding: utf-8 -*-

from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
import os
import logging

log = logging.getLogger()

class GcloudBashOperator(BashOperator):
    """Run a gcloud operator with the possibility of transferring data between GCP and AWS.

    :param bash_command: The bash command you wish to execute, but excluding gcloud auth commands
    :type bash_command: str
    :param gcloud_install_path: path where gcloud was installed in your Dockerfile, excluding 'google-cloud-sdk'
    :type bucket_name: str
    :param gcp_service_account_email: the full email address of the GCP service account you are using to authenticate with GCP
    :type gcp_service_account_email: str
    :param gcp_key_file: the path and filename (json format) of the credentials for GCP
    :type gcp_key_file: str
    :param aws_boto_file: the path and filename (.boto) of the credentials for AWS
    :type aws_boto_file: str
    """

    template_fields = ('bash_command', 'env')
    emplate_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command,
            xcom_push=False,
            gcloud_install_path='',
            gcp_service_account_email='',
            gcp_key_file='',
            aws_boto_file='',
            output_encoding='utf-8',
            *args, **kwargs) -> None:
        super(BashOperator, self).__init__(*args, **kwargs)
        self.xcom_push_flag = xcom_push
        self.gcloud_install_path = gcloud_install_path
        self.gcp_service_account_email = gcp_service_account_email
        self.gcp_key_file = gcp_key_file
        self.aws_boto_file = aws_boto_file
        self.output_encoding = output_encoding

        # Add shell command completion and command line tools to path
        shell_completion_bash = f'source \'{self.gcloud_install_path}/google-cloud-sdk/path.bash.inc\''
        command_line_tools_bash = f'source \'{self.gcloud_install_path}/google-cloud-sdk/completion.bash.inc\''

        # Add gcloud authentication and combine commands together into one
        gcloud_auth_bash = f'gcloud auth activate-service-account {self.gcp_service_account_email} --key-file={self.gcp_key_file}'
        self.bash_command = shell_completion_bash + ' && ' + command_line_tools_bash + ' && ' + gcloud_auth_bash + ' && ' + bash_command

        # Add BOTO_PATH for AWS boto file
        log.info("Adding .boto file to environment variables for AWS authentication")
        os.environ["BOTO_PATH"]=self.aws_boto_file
        self.env = os.environ.copy()

    def execute(self, context):
        log.info("Enabling shell command completion for gcloud")
        log.info("Adding the Google Cloud SDK command line tools to your $PATH")
        log.info(f"Final bash command which will be executed: {self.bash_command}")
        super().execute(context)
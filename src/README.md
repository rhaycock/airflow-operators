# Overview

This folder contains Airflow operators which relate to GCP/Google cloud.

# Operators
## gcloud_bash_operator

This is an Airflow operator which can be used to run gcloud (e.g., gsutil) commands through (Dockerized, if desired) Airflow. This operator makes it easy as it covers all of the prerequisites which are required to run such commands. A use case of this class is to copy data from an S3 bucket to a GCS bucket using rsync (instead of batch loading objects). This is useful for cases where you do not know the cadence of objects being loaded into the source S3 bucket.

To run `gcloud_bash_operator` in Docker, simply place the class in your project, import the package in your dag file, and add the following lines to your Dockerfile:
```
RUN apk add --update \
 python \
 curl \
 which \
 bash
RUN curl -sSL https://sdk.cloud.google.com > /tmp/gcl && bash /tmp/gcl --install-dir=/<gcloud_install_path>/gcloud --disable-prompts
```
Choose your own `<gcloud_install_path>` and then place a GCP credential json file and a .boto file (for AWS credentials) in your project. Then, reference these files and folder paths when you create the operator instance in your dag file.
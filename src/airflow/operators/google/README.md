# Overview

This folder contains Airflow operators which relate to GCP/Google cloud.

# Operators
## gcloud_bash_operator

This is an Airflow operator which can be used to run gcloud (e.g., gsutil) commands through (Dockerized, if desired) Airflow. This operator makes it easy as it covers all of the prerequisites which are required to run such commands. Also included is a subclass called S3ToGCSSync which synchronizes an S3 bucket with a GCS bucket using rsync (instead of batch loading objects). This is useful for cases where you do not know the cadence of objects being loaded into the source S3 bucket.
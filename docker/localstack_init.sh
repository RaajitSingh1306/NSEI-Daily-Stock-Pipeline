#!/bin/bash
set -euo pipefail

echo "[localstack-init] Creating S3 bucket: nsei-datalake"

awslocal s3 mb s3://nsei-datalake --region ap-south-1

awslocal s3api put-object --bucket nsei-datalake --key raw/nsei/daily/.keep
awslocal s3api put-object --bucket nsei-datalake --key processed/nsei/daily/.keep
awslocal s3api put-object --bucket nsei-datalake --key quarantine/.keep

echo "[localstack-init] s3://nsei-datalake ready"
awslocal s3 ls s3://nsei-datalake/
project=$1
staging_bucket=$2
gcloud dataproc clusters create btc-project \
    --region us-west4 \
    --optional-components=JUPYTER \
    --image-version=2.0.24-ubuntu18 --enable-component-gateway \
    --metadata 'PIP_PACKAGES=requests_oauthlib google-cloud-bigquery' \
    --metadata gcs-connector-version=1.9.16 \
    --metadata 'bigquery-connector-version=1.0.0' \
    --project  $project \
    --bucket $staging_bucket \
    --worker-boot-disk-size=400g \
    --initialization-actions=gs://dataproc-initialization-actions/python/pip-install.sh,gs://dataproc-initialization-actions/connectors/connectors.sh \
    --num-workers=5

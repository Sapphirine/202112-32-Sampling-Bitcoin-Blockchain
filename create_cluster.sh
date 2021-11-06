project=$1
staging_bucket=$2
gcloud beta dataproc clusters create btc-project \
    --region us-west4 \
    --optional-components=JUPYTER \
    --image-version=preview --enable-component-gateway \
    --metadata 'PIP_PACKAGES=requests_oauthlib google-cloud-bigquery  tweepy==3.10.0' \
    --metadata gcs-connector-version=1.9.16 \
    --metadata 'bigquery-connector-version=1.0.0' \
    --project  $project \
    --bucket $staging_bucket \
    --initialization-actions=gs://dataproc-initialization-actions/python/pip-install.sh,gs://dataproc-initialization-actions/connectors/connectors.sh \
    --single-node 

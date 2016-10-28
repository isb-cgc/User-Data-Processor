/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/udp-uat.env" ./.env
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/ssl/ISB-CGC-uat-client-cert.pem" ./client-cert.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/ssl/ISB-CGC-uat-client-key.pem" ./client-key.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/ssl/ISB-CGC-uat-server-ca.pem" ./server-ca.pem

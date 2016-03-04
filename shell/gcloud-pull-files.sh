/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/udp-prod.env" ./.env
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-ssl/demo01-client-cert.pem" ./client-cert.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-ssl/demo01-client-key.pem" ./client-key.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-ssh/demo01-server-ca.pem" ./server-ca.pem

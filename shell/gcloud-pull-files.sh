/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-files/udp-prod.env" ./.env
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-files/prod-ssl/prod-main-client-cert.pem" ./client-cert.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-files/prod-ssl/prod-main-client-key.pem" ./client-key.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/prod-files/prod-ssl/prod-main-server-ca.pem" ./server-ca.pem

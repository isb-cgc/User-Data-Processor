/usr/local/bin/gsutil cp "gs://${GCLOUD_BUCKET}/udp-test.env" ./.env
/usr/local/bin/gsutil cp "gs://${GCLOUD_BUCKET}/test-ssl/ISB-CGC-test-main-client-cert.pem" ./client-cert.pem
/usr/local/bin/gsutil cp "gs://${GCLOUD_BUCKET}/test-ssl/ISB-CGC-test-main-client-key.pem" ./client-key.pem
/usr/local/bin/gsutil cp "gs://${GCLOUD_BUCKET}/test-ssl/ISB-CGC-test-main-server-ca.pem" ./server-ca.pem

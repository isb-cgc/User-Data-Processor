/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/udp-test.env" ./.env
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/test-ssl/ISB-CGC-test-main-client-cert.pem" ./client-cert.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/test-ssl/ISB-CGC-test-main-client-key.pem" ./client-key.pem
/usr/bin/gsutil cp "gs://${GCLOUD_BUCKET}/test-ssl/ISB-CGC-test-main-server-ca.pem" ./server-ca.pem

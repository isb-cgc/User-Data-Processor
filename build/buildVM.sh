#!/usr/bin/env bash

INTERNAL_IP_ADDRESS=XXX.XXX.XXX.XXX
MACHINE_NAME=user-data-upload-dev
MACHINE_TAG=udu-processor
MACHINE_DESC="user data upload server for dev"
PROJECT=isb-cgc
UDU_USER=uduproc
USER_AND_MACHINE=${UDU_USER}@${MACHINE_NAME}
ZONE=us-central1-c
BUCK_SUFF=
TARGET_BRANCH=dev
UDU_DEPLOY_BUCKET=your-deployment-bucket-name${BUCK_SUF}/${TARGET_BRANCH}

#
# Spin up the VM:
#

gcloud compute instances create "${MACHINE_NAME}" --description "${MACHINE_DESC}" --zone "${ZONE}" --machine-type "n1-standard-1" --image-project "debian-cloud" --image-family "debian-8" --project "${PROJECT}" --private-network-ip "${INTERNAL_IP_ADDRESS}"

#
# Add tag to machine:
#

sleep 10
gcloud compute instances add-tags "${MACHINE_NAME}" --tags "${MACHINE_TAG}" --project "${PROJECT}" --zone "${ZONE}"

#
# This is what you get after you log in:
#

sleep 10
echo "AFTER LOGIN: gsutil cp gs://${UDU_DEPLOY_BUCKET}/install-deps.sh .; chmod u+x install-deps.sh; ./install-deps.sh"

#
# By logging in as a user, the machine will create an account for that user:
#

sleep 10
gcloud compute ssh --project "${PROJECT}" --zone ${ZONE} "${USER_AND_MACHINE}"

#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/var/local/udu/pubSubServiceAcct.json

# This has to be run in the directory with tasks_for_psq.py.

python /usr/local/lib/python2.7/dist-packages/psq/psqworker.py app_udu_server.q

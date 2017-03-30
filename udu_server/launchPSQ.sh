#!/usr/bin/env bash

# Copyright 2017, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is run by supervisord, so we need to set working directory:

UDU_HOME=/var/local/udu
PYTH_DIST=/usr/local/lib/python2.7/dist-packages

cd $UDU_HOME/User-Data-Processor

export GOOGLE_APPLICATION_CREDENTIALS=$UDU_HOME/credentials/pubSubServiceAcct.json
export PYTHONPATH=$UDU_HOME/User-Data-Processor

# This has to be run in the directory with tasks_for_psq.py.

python $PYTH_DIST/psq/psqworker.py app_udu_server.q

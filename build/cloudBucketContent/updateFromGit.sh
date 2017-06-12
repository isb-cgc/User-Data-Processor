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

# run as ./updateFromGit.sh. Sudo is handled case-by-case

UDU_OWN=uduproc
UDU_HOME=/var/local/udu
UDU_GIT_HOME=${UDU_HOME}/User-Data-Processor
UDU_GIT_HOME_TMP=${UDU_HOME}/User-Data-Processor-Old

#
# Stop the udu daemons for the duration
#

sudo supervisorctl stop all
sleep 10

#
# Move the current config and credentials to the side:
#

mv ${UDU_GIT_HOME} ${UDU_GIT_HOME_TMP}

#
# Off to github to get the code!
#

cd ${UDU_HOME}
git clone https://github.com/isb-cgc/User-Data-Processor.git
cd ${UDU_GIT_HOME}
#needed for development:
#git checkout my-not-master-branch

#
# Toss the generic credentials and config from the Git repo and replace
# with the versions from the hold location. Change permissions on executables:
#

rm ${UDU_GIT_HOME}/config/*
mv ${UDU_GIT_HOME_TMP}/config/* ${UDU_GIT_HOME}/config
rm ${UDU_GIT_HOME}/credentials/*
mv ${UDU_GIT_HOME_TMP}/credentials/* ${UDU_GIT_HOME}/credentials

cd ${UDU_GIT_HOME}/udu_server
chmod u+x worker_runner.py app_udu_server.py launchPSQ.sh launchUDU.sh runSQLProxy.sh

#
# Move CloudSQL Proxy over from temp
#

mv ${UDU_GIT_HOME_TMP}/udu_server/cloud_sql_proxy ${UDU_GIT_HOME}/udu_server/cloud_sql_proxy

#
# Start everybody up
#

sudo supervisorctl start all

#
# Toss the old stuff
#

rm -rf ${UDU_GIT_HOME_TMP}

echo "done!"
echo "Manage services with: sudo supervisorctl"
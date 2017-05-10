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

# run as ./install-deps.sh. Sudo is handled case-by-case

UDU_OWN=uduproc
UDU_HOME=/var/local/udu
BUCK_SUFF=
STAGE=dev
UCSTAGE=Dev
# point this at your own storage bucket for config files:
UDU_DEPLOY_BUCKET=udu-deployment-files${BUCK_SUFF}/${STAGE}


# Update the VM:

sudo apt-get update

# Build the target location

sudo mkdir -p ${UDU_HOME}
sudo chown ${UDU_OWN} ${UDU_HOME}
chmod o+w ${UDU_HOME}
mkdir ${UDU_HOME}/uploads
chmod o+w ${UDU_HOME}/uploads
mkdir ${UDU_HOME}/log
chmod o+w ${UDU_HOME}/log
cd ${UDU_HOME}

# Get deployment files from our bucket:

gsutil cp gs://${UDU_DEPLOY_BUCKET}/* ${UDU_HOME}

# Getting this key from the public server seems to fail a lot:

sudo apt-key add MySQLReleasePubKey.txt || exit 1
rm MySQLReleasePubKey.txt

# Install mysql-server-5.7

MYSQL_MAJOR=5.7
MYSQL_VERSION=`curl http://repo.mysql.com/apt/debian/dists/jessie/mysql-5.7/binary-amd64/Packages 2>/dev/null | grep Version: | uniq | sed 's/Version: //'`

NEW_PASS=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
echo $NEW_PASS > holdPass.txt
chmod 400 holdPass.txt
sudo echo "deb http://repo.mysql.com/apt/debian/ jessie mysql-${MYSQL_MAJOR}" | sudo tee /etc/apt/sources.list.d/mysql.list > /dev/null
sudo echo "mysql-community-server mysql-community-server/data-dir select ''" | sudo debconf-set-selections
sudo echo "mysql-community-server mysql-community-server/root-pass password "$NEW_PASS | sudo debconf-set-selections
sudo echo "mysql-community-server mysql-community-server/re-root-pass password "$NEW_PASS | sudo debconf-set-selections
sudo echo "mysql-community-server mysql-community-server/remove-test-db select true" | sudo debconf-set-selections

sudo apt-get update && sudo apt-get install -y mysql-server="${MYSQL_VERSION}"

#
# Seems we gotta install the server to get all the pieces we need to make
# python SQL happy. But we want to shut that sucker down and keep it down
# (It gets in the way of our cloud SQL proxy, and would be an unused security hole):
#

sudo mysqladmin --user=root --password=$NEW_PASS shutdown
sudo /usr/sbin/update-rc.d mysql disable

# Python environment. Previous UDU used pip installs for numpy and pandas. These are older,
# but some pieces get installed anyway by later apt-get:

sudo apt-get install -y	git
sudo apt-get install -y python-pip
sudo apt-get install -y python-dev
sudo apt-get install -y python-mysqldb
sudo apt-get install -y python-numpy
sudo apt-get install -y python-pandas

#
# Off to github to get the code!
#

rm -f ${UDU_HOME}/User-Data-Processor
git clone https://github.com/isb-cgc/User-Data-Processor.git
cd ${UDU_HOME}/User-Data-Processor
#needed for development:
#git checkout my-not-master-branch
sudo pip install -r config/requirements.txt

# Not using PSQ anymore, but need to get the google cloud stuff it used:

sudo pip install google-cloud

# Need to get Flask, with simple ability to handle passwords:

sudo pip install Flask
sudo pip install Flask-BasicAuth

# Get environment and configuration into position from their original pull
# from the storage bucket:

mv ${UDU_HOME}/startupConfig.sh ${UDU_HOME}/User-Data-Processor/config/startupConfig.sh
mv ${UDU_HOME}/udu_config.txt ${UDU_HOME}/User-Data-Processor/config/udu_config.txt
mv ${UDU_HOME}/udu_secrets.txt ${UDU_HOME}/User-Data-Processor/config/udu_secrets.txt
chmod 400 ${UDU_HOME}/User-Data-Processor/config/udu_secrets.txt
mv ${UDU_HOME}/pubSubServiceAcct${UCSTAGE}.json ${UDU_HOME}/User-Data-Processor/credentials/pubSubServiceAcct.json
mv ${UDU_HOME}/sqlServiceAcct${UCSTAGE}.json ${UDU_HOME}/User-Data-Processor/credentials/sqlServiceAcct.json
mv ${UDU_HOME}/bucketAndBiqQueryServiceAcct${UCSTAGE}.json ${UDU_HOME}/User-Data-Processor/credentials/bucketAndBiqQueryServiceAcct.json
chmod 400 ${UDU_HOME}/User-Data-Processor/credentials/*.json

cd ${UDU_HOME}/User-Data-Processor/udu_server
chmod u+x worker_runner.py app_udu_server.py launchPSQ.sh launchUDU.sh runSQLProxy.sh
mv ${UDU_HOME}/flask-server.crt ${UDU_HOME}/User-Data-Processor/config
mv ${UDU_HOME}/flask-server.key ${UDU_HOME}/User-Data-Processor/config
chmod 400 ${UDU_HOME}/User-Data-Processor/config/flask-server.*

#
# Stackdriver agent:
#

cd ${UDU_HOME}
curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh
SHA_RES=`sha256sum install-logging-agent.sh`
if [ "$SHA_RES" != "07ca6e522885b9696013aaddde48bf2675429e57081c70080a9a1364a411b395  install-logging-agent.sh" ]; then
  echo "Logging Agent is Invalid"
  exit 1
fi
sudo bash install-logging-agent.sh
rm install-logging-agent.sh

#
# Supervisor. Apparently, the apt-get gets us the system init.d install, while we
# need to do the pip upgrade to get ourselves to 3.3.1:
#

sudo apt-get install -y supervisor
sudo pip install --upgrade supervisor

#
# CloudSQL Proxy:
#

cd ${UDU_HOME}/User-Data-Processor/udu_server
wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
mv cloud_sql_proxy.linux.amd64 cloud_sql_proxy
chmod u+x cloud_sql_proxy

#
# Supervisor config files:
#

sudo mv ${UDU_HOME}/User-Data-Processor/supervisorConf/*.conf /etc/supervisor/conf.d/
sudo supervisorctl reread
sudo supervisorctl update

echo "done!"
echo "Manage services with: sudo supervisorctl"
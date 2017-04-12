#!/usr/bin/env python

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

import time
from datetime import datetime
from gcloud import pubsub
from not_psq.queue import Queue
from not_psq.worker import Worker
from not_psq.task import Task
from isb_cgc_user_data.utils.build_config import read_dict
from google.gax.errors import RetryError
import isb_cgc_user_data.uduprocessor

#
# Here we read the config and secret file
#

my_secrets = read_dict('../config/udu_secrets.txt')
my_config = read_dict('../config/udu_config.txt')
my_config.update(my_secrets)

PROJECT_ID = my_config['UDU_PSQ_PROJECT_ID']
PSQ_TOPIC_NAME = my_config['UDU_PSQ_TOPIC_NAME']


def main():
    pubsub_client = pubsub.Client(project=PROJECT_ID)
    q = Queue(pubsub_client, name=PSQ_TOPIC_NAME)
    worker = Worker(q)
    while True:
        listening = True
        try_count = 10
        while listening and try_count > 0:
            try:
                tasks = worker.listen() # will block until exception or we have a task
                listening = False
            except RetryError:
                if try_count <= 0:
                    raise
                time.sleep(2)
                pubsub_client = pubsub.Client(project=PROJECT_ID)
                q = Queue(pubsub_client, name=PSQ_TOPIC_NAME)
                worker = Worker(q)
                try_count -= 1

        for task in tasks:
            to_do = task.getMsg()
            if to_do.method is 'buildWithParameters':
                isb_cgc_user_data.uduprocessor.process_upload(to_do.file_name, to_do.success_url, to_do.failure_url)
            elif to_do.method is 'ping':
                print ('Pipe pinged at: {0}'.format(str(datetime.now())))
            else:
                print ('unexpected method call: {0}'.format(to_do.method))

if __name__ == '__main__':
    main()



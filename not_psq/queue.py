#
# Original implementation:
#
# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Additional Material:
#
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

from gcloud import pubsub
#import google.cloud.exceptions
from .task import Task
import sys

PUBSUB_OBJECT_PREFIX = 'psq'

class Queue(object):
    def __init__(self, pubsub, name='default'):
        self.name = name
        self._check_for_thread_safety(pubsub)
        self.pubsub = pubsub
        self.topic = self._get_or_create_topic()
        self.subscription = None

    def _check_for_thread_safety(self, client):
        try:
            # Is this client's module using grpc/gax?
            client_module_name = client.__module__
            client_module = sys.modules[client_module_name]
            if getattr(client_module, '_USE_GAX', True):
                return

            connection_module_name = client.connection.__module__
            connection_module = sys.modules[connection_module_name]

            if getattr(connection_module, '_USE_GRPC', True):
                return
           # Is the connection is using httplib2shim?
            if client.connection.http.__class__.__module__ == 'httplib2shim':
                return

            raise ValueError(
                'Client object {} is not threadsafe. psq requires clients to be '
                'threadsafe. You can either ensure grpc is installed or use '
                'httplib2shim.'.format(client))

        except (KeyError, AttributeError):
            pass


    def _get_or_create_topic(self):
        topic_name = '{}-{}'.format(PUBSUB_OBJECT_PREFIX, self.name)

        topic = self.pubsub.topic(topic_name)

        if not topic.exists():
            try:
                topic.create()
            except google.cloud.exceptions.Conflict:
                # Another process created the topic before us, ignore.
                pass

        return topic

    def _get_or_create_subscription(self):
        """Workers all share the same subscription so that tasks are
        distributed across all workers."""
        subscription_name = '{}-{}-shared'.format(
            PUBSUB_OBJECT_PREFIX, self.name)

        subscription = pubsub.Subscription(
            subscription_name, topic=self.topic)

        if not subscription.exists():
            try:
                subscription.create()
            except google.cloud.exceptions.Conflict:
                # Another worker created the subscription before us, ignore.
                pass

        return subscription

    def enqueue(self, task):
        self.topic.publish(task.getMsg())

    def dequeue(self):
        if not self.subscription:
            self.subscription = self._get_or_create_subscription()

        messages = self.subscription.pull(return_immediately=False, max_messages=1)

        if not messages:
            return None

        ack_ids = [x[0] for x in messages]

        tasks = []
        for x in messages:
            task = Task(x[1].data, x[1].message_id)
            tasks.append(task)

        self.subscription.acknowledge(ack_ids)

        return tasks


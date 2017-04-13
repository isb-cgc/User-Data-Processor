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

from retrying import retry

class Worker(object):
    def __init__(self, queue):
        self.queue = queue
        self.max_sequential_errors = 5

    def _safe_dequeue(self):
        """Dequeues tasks while dealing with transient errors."""
        @retry(
            stop_max_attempt_number=self.max_sequential_errors,
            # Wait 2^n * 1 seconds between retries, up to 10 seconds.
            wait_exponential_multiplier=1000, wait_exponential_max=10000,
            retry_on_exception=lambda e: not isinstance(e, KeyboardInterrupt))
        def inner():
            return self.queue.dequeue()
        return inner()

    def listen(self):
        try:
            while True:

                tasks = self._safe_dequeue()

                if not tasks:
                    continue

                return (tasks);


        except KeyboardInterrupt:
            pass



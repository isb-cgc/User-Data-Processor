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
from google.cloud import logging

'''
So we are seeing cases on the UDU sever where the Google default credentials expire every
hour, and are useless for about five minutes. If you try to log something, you get a nasty
RetryError. If we get one, try to recover:
'''

class Safe_Logger(object):
    def __init__(self, log_point):
        self.log_point = log_point
        self.logging_client = logging.Client()
        self.logger = self.logging_client.logger(log_point)

    def log_text(self, text, severity='INFO'):
        sending = True
        try_count = 10
        while sending and try_count > 0:
            try:
                self.logger.log_text(text, severity=severity)
                sending = False
            except RetryError:
                time.sleep(2)
                self.logging_client = logging.Client()
                self.logger = self.logging_client.logger(self.log_point)
                try_count -= 1
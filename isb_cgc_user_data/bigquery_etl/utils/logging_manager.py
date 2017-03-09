#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import logging.config
import inspect
from colorlog import ColoredFormatter

def configure_logging(name, log_filename, level='DEBUG'):

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                #'format':  '%(levelname)s %(asctime)s %(name)s %(module)s.%(funcName)s.%(lineno)d %(processName)s -  %(message)s',
                #'format':  '[%(levelname)s %(asctime)s %(name)s %(module)s %(processName)-8s] %(message)s',
                'format':  '[%(levelname)s %(asctime)s %(name)s %(processName)s] %(message)s',
                'datefmt': "%Y-%m-%d-%H:%M:%S"
            },
            'colored': {
                    '()': 'colorlog.ColoredFormatter',
#                    'format':  '%(log_color)s[%(levelname)s %(name)s %(module)s.%(funcName)s.%(lineno)d %(processName)-8s] %(reset)s %(message)s',
                    'format':  '%(log_color)s[%(levelname)s %(asctime)s %(name)s] %(reset)s %(message)s',
                    'log_colors': {
                         'DEBUG':    'cyan',
                         'INFO':     'green',
                         'WARNING':  'yellow',
                         'ERROR':    'red',
                         'CRITICAL': 'red,bg_white',
                  },
                  'reset': 'True'
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'formatter': 'colored',
                'class': 'logging.StreamHandler',
            },
            'file': {
                'level': 'DEBUG',
                'formatter': 'standard',
                'class': 'logging.FileHandler',
                'filename': log_filename,
                'mode': 'w'
            }
        },
        'loggers': {
            '': {
                'handlers': ['console', 'file'],
                'level': level,
            },
        }
   }
    logging.config.dictConfig(LOGGING)
    logging.captureWarnings(True)
    logger = logging.getLogger(name)

    return logger


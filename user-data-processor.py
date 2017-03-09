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

"""Legacy entry point. Use isb_cgc_user_data.uduprocessor for future development
"""


import argparse
import json
import os

import isb_cgc_user_data.uduprocessor


def main(user_data_config):
    isb_cgc_user_data.uduprocessor.process_upload(user_data_config)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'config_file',
        help='Path to the config file for the job'
    )

    args = parser.parse_args()

    # log_filename = 'etl_{0}.log'.format(args.datatype)
    # log_name = 'etl_{0}'.format(args.datatype)
    # log = configure_logging(log_name, log_filename)

    # print args.config_file

    main(
        args.config_file
    )

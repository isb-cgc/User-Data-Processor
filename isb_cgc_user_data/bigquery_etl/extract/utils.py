# -*- coding: utf-8 -*-
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

"""Extract Utils
"""

import json
import pandas as pd
import logging

log = logging.getLogger(__name__)

def convert_file_to_dataframe(filepath_or_buffer, sep="\t", skiprows=0, rollover=False, nrows=None, header=None):
    """does some required data cleaning and
      then converts into a dataframe
    """

    log.info("Converting  file to a dataframe")

    try:
        # items to change to NaN/NULL
        # when you change something here, remember to change in clean_up_dataframe too.
        na_values = ['none', 'None', 'NONE', 'null', 'Null', 'NULL', ' ', 'NA', '__UNKNOWN__', '?']

        # read the table/file
        data_df = pd.read_table(filepath_or_buffer, sep=sep, skiprows=skiprows, lineterminator='\n',
                                comment='#', na_values=na_values, dtype='object', nrows=nrows, header=header)

    except Exception as exp:
        log.error(msg=exp.message)
        raise

    filepath_or_buffer.close() # close  StringIO

    return data_df


#----------------------------------------
# Convert newline-delimited JSON string to dataframe
#  -- should work for a small to medium files
# we are not loading into string, but into a temp file
# works only in a single bucket
#----------------------------------------
def convert_njson_file_to_df(filebuffer):
    """Converting new-line delimited JSON file into dataframe"""
    log.info("Converting new-line delimited JSON file into dataframe")

    # convert the file into a dataframe
    lines = [json.loads(l) for l in filebuffer.splitlines()]
    data_df = pd.DataFrame(lines)

    # delete the temp file
    filebuffer.close()

    return data_df

# Copyright 2015-2017, Institute for Systems Biology.
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

import csv
from isb_cgc_user_data.utils.error_handling import UduException


#
# Do column checking before pandas gets a chance, since it appends _x to
# duplicate column headings:
#

def reject_dup_col_pre_dataframe(buffer, logger, name):

    tsvin = csv.reader(buffer, delimiter='\t')
    first = next(tsvin)
    buffer.seek(0)

    if not first:
        if logger:
            logger.log_text('uduprocessor: file empty', severity='INFO')
        user_message = "File was empty "
        raise UduException(user_message)

    barcode_counts = {}

    for i in first:
        if not i.strip():
            if logger:
                logger.log_text('uduprocessor: empty {0}'.format(name), severity='INFO')
            user_message = "Empty {0} detected in columns, processing cannot continue".format(name)
            raise UduException(user_message)

        cur_count = barcode_counts.get(i, 0)
        if cur_count > 0:
            if logger:
                logger.log_text('uduprocessor: duplicated {0}'.format(name), severity='INFO')
            user_message = "Duplicated {0} detected in columns, processing cannot continue: {1}".format(name, str(i))
            raise UduException(user_message)
        barcode_counts[i] = cur_count + 1

#
# Find the ID column:
#

def find_key_column(data_df, column_map, logger, key):

    count = 0
    for i, j in data_df.iteritems():
        if (i in column_map.keys()) and (column_map[i] == key):
            return count
        count += 1
    if logger:
        logger.log_text('uduprocessor: no key column found', severity='INFO')
    for tkey, value in column_map.iteritems():
        if value == key:
            needed = tkey
            break
    user_message = 'No key column named {0} found, processing cannot continue'.format(needed)
    raise UduException(user_message)

#
# Reject duplicate features:
#

def reject_row_duplicate_or_blank(data_df, logger, name, id_col):

    feature_counts = {}

    for row_index, row in data_df.iterrows():
        feat = row[id_col]
        if isinstance(feat, float):
            if logger:
                logger.log_text('uduprocessor: float key implies trailing empty line {0}'.format(name), severity='INFO')
            user_message = "Trailing empty lines detected, processing cannot continue"
            raise UduException(user_message)
        if not feat.strip():
            if logger:
                logger.log_text('uduprocessor: empty {0}'.format(name), severity='INFO')
            user_message = "Empty {0} detected in rows, processing cannot continue".format(name)
            raise UduException(user_message)

        cur_count = feature_counts.get(feat, 0)
        if cur_count > 0:
            if logger:
                logger.log_text('uduprocessor: duplicated {0}'.format(name), severity='INFO')
            user_message = "Duplicated {0} detected in rows, processing cannot continue: {1}".format(name, str(feat))
            raise UduException(user_message)
        feature_counts[feat] = cur_count + 1

#
# Reject duplicate barcodes:
#

def reject_col_duplicate_or_blank(data_df, logger, name):

    barcode_counts = {}

    for i, j in data_df.iteritems():

        if not i.strip():
            if logger:
                logger.log_text('uduprocessor: empty {0}'.format(name), severity='INFO')
            user_message = "Empty {0} detected in columns, processing cannot continue".format(name);
            raise UduException(user_message)

        cur_count = barcode_counts.get(i, 0)
        if cur_count > 0:
            if logger:
                logger.log_text('uduprocessor: duplicated {0}'.format(name), severity='INFO')
            user_message = "Duplicated {0} detected in columns, processing cannot continue: {1}".format(name, str(i));
            raise UduException(user_message)
        barcode_counts[i] = cur_count + 1

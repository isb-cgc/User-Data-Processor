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

from isb_cgc_user_data.utils.error_handling import UduException

#
# Reject duplicate features:
#

def reject_row_duplicate_features(data_df, logger, name):

    feature_counts = {}

    for row_index, row in data_df.iterrows():
        feat = row[0]
        cur_count = feature_counts.get(feat, 0)
        if cur_count > 0:
            if logger:
                logger.log_text('uduprocessor: duplicated {0}'.format(name), severity='INFO')
            user_message = "Upload stopped due to duplicated {0}: {1}".format(name, str(feat));
            raise UduException(user_message)
        feature_counts[feat] = cur_count + 1

#
# Reject duplicate barcodes:
#

def reject_col_duplicate_barcodes(data_df, logger, name):

    barcode_counts = {}

    for i, j in data_df.iteritems():

        cur_count = barcode_counts.get(i, 0)
        if cur_count > 0:
            if logger:
                logger.log_text('uduprocessor: duplicated {0}'.format(name), severity='INFO')
            user_message = "Upload stopped due to duplicated {0}: {1}".format(name, str(i));
            raise UduException(user_message)
        barcode_counts[i] = cur_count + 1

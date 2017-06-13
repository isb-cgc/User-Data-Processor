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

# -*- coding: utf-8 -*-
import re
from collections import OrderedDict
import pandas as pd
import numpy as np
from StringIO import StringIO
import chardet
from isb_cgc_user_data.utils.error_handling import UduException


#--------------------------------------
# Clean up the dataframe
# Throws a UduException if the dataframe is empty (only has the header row)
#--------------------------------------
def cleanup_dataframe(df, logger=None):
    """Cleans the dataframe
        - strips new lines, double, single quotes; None -> nan, etc
        - formats the column names for Bigquery input
    """

    if logger:
        logger.log_text("Cleaning up the dataframe", severity='INFO')

    if df.empty:
        logger.log_text("Encountered empty dataframe", severity='ERROR')
        raise UduException("Uploaded file only contained header row")

    # why again, we are doing it in convert_to_dataframe, right?
    # because we can call this function separately
    na_values = ['none', 'None', 'NONE', 'null', 'Null', 'NULL', ' ', 'NA', '__UNKNOWN__', '?']
    for rep in na_values:
        df = df.replace(rep, np.nan)

    # remove empty spaces(this removes more than 1 space)
    df = df.applymap(lambda x: np.nan if isinstance(x, basestring) and x.isspace() else x)

    #we dont want to play with nan(numpy.nan) or empty values
    df = df.fillna("__missing__value__")

    # convert to utf-8
    df = df.applymap(lambda x: convert_encoding(x, logger))

    # strip every value(this should get rid of ^M too)
    df = df.applymap(lambda x: str(x).strip())

    # strip every value of single quotes
    df = df.applymap(lambda x: str(x).strip("'"))

    # strip every value of double quotes
    df = df.applymap(lambda x: str(x).strip('"'))

    # strip column names too
    df.columns = df.columns.map(lambda x: x.strip())

    # replace back with np.nan
    df = df.replace(r'__missing__value__', np.nan)

    #------------
    # Fix columns
    #------------
    replace_column_strings = (
        (" ", "_"), # blank space to underscore
        ("-", "_"), # dashes to underscore (BQ doesnt allow dashes)
        (")", ""),  # delete closing bracket
        ("(", "_"), # opening bracket to underscore
        ("+", "_"), # plus to underscore
        (".", "_"), # dot to underscore
        ("__", "_") # double underscore to just underscore
    )
    replace_column_strings = OrderedDict(replace_column_strings)

    # strip column names too
    df.columns = df.columns.map(lambda x: x.strip())

    # replace all patterns from above
    for repl in replace_column_strings:
        df.columns = df.columns.map(lambda x: x.replace(repl, replace_column_strings[repl]))
    return df

#----------------------------------------
# Convert a dataframe into newline-delimited JSON string
#  -- should work for a small to medium files
# if rollover_file is true
# works only in a single bucket
#----------------------------------------
def convert_df_to_njson(df, logger=None):
    """Converting dataframe into a new-line delimited JSON
    """
    if logger:
        logger.log_text("Converting dataframe into a new-line delimited JSON file", severity='INFO')

    file_to_upload = StringIO()

    for _, rec in df.iterrows():
        file_to_upload.write(rec.convert_objects(convert_numeric=False).to_json() + "\n")

    file_to_upload.seek(0)

    return file_to_upload.getvalue()


def remove_duplicates(df, unique_key, logger=None):
    """Removes duplicates in a dataframe based on the unique combination of key
        unique_key accepts a list
    """
    df['duplicated'] = df.duplicated(unique_key)
    if not df[df.duplicated(unique_key)].empty:
        if logger:
            logger.log_text("Found duplicate rows", severity='DEBUG')
            logger.log_text(df[df.duplicated(unique_key)].to_csv(sep="\t", index=False), severity='DEBUG')
        df['duplicated'] = df.duplicated(unique_key)
        # drop duplicates
        df = df.drop_duplicates(unique_key)
        if logger:
            logger.log_text("Deleted", severity='DEBUG')
    return df

def mangle_dupe_cols(columns):
    """remove/mangle any duplicate columns (we are naming line a, a.1, a.2 etc if duplicates)
    """
    counts = {}
    for i, col in enumerate(columns):
        cur_count = counts.get(col, 0)
        if cur_count > 0:
            columns[i] = '%s.%d' % (col, cur_count)
            print ('mangle_dupe_col: Duplicate column name: ' + str(col))
        counts[col] = cur_count + 1
    return columns


#----------------------------------------------
# format any duplicate values ( we are naming line dup1.a, dup2.a, etc if duplicates)
#----------------------------------------------
def format_dupe_values(values):
    counts = {}
    for i, val in enumerate(values):
        cur_count = counts.get(val, 0)
        if cur_count > 0:
            values[i] = 'dup%d.%s' % (cur_count, val)
            print ("format_dupe_values: Duplicate column name: " + str(val))
        counts[val] = cur_count + 1
    return values

def assert_notnull_property(df, columns_list):
    """
    checks if a dataframe column values are NULL/NaN;
    param columns_list accepts a list
    returns NULL valuecolumns

    Example:
        Input Dataframe
                  a   b
            0  True   2
            1     3 NaN

        Returns column that has NULL value:
                  a   b
               1  3 NaN
    """
    null_row_count = len(df[columns_list].isnull().any(axis=1))
    if null_row_count > 0:
        null_rows_df = df[df.isnull().any(axis=1)]
        print null_rows_df
        raise Exception('Assert Property failed: Column values are null')


def convert_encoding(text, new_coding='UTF-8', logger=None):
    """UTF-8 encode all strings
    """
    if isinstance(text, (int, float)):
        text = str(text)

    if all([True if ord(i) < 128 else False for i in text]):
        return text

    try:
        encoding = chardet.detect(text)['encoding']
        if logger:
            logger.log_text('Found {0} encoded string - {1}'.format(encoding, text), severity='DEBUG')
        if new_coding.upper() != encoding.upper():
            text = text.decode(encoding, text).encode(new_coding)
            if logger:
                logger.log_text('New {0} encoded string - {1}'.format(new_coding, text), severity='DEBUG')
        return text
    except Exception as e:
        if logger:
            logger.log_text("Could not be decoded. Encoding failed because {0}".format(str(e)), severity='ERROR')
        pass

    # decode to ascii-ignore and then encode to utf-8( default if others fail)
    # remove the non-ascii characters if the above steps fail
    try:
        text = text.decode('ascii', 'ignore').encode(new_coding)
    except Exception as e:
        if logger:
            logger.log_text("Could not be :ascii ignored:. Encoding failed because {0}".format(str(e)), severity='ERROR')
        text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    return text


def split_df_column_values_into_multiple_rows(data_df, column_name, splitby=','):
    """Split pandas dataframe string entry to separate rows
    """
    concat_dict = []
    data_df['__split_column__'] = data_df[column_name].str.split(splitby)
    for _, row_df in data_df.iterrows():
        row_dict = row_df.to_dict()
        elements = row_dict['__split_column__']
        if not isinstance(elements, float) and  pd.notnull(elements).all() and len(elements) > 1:
            for item in elements:
                row_dict = row_df.to_dict()
                row_dict[column_name] = item
                concat_dict.append(row_dict)
                row_dict = {}
        else:
            concat_dict.append(row_dict)
    concat_df = pd.DataFrame(concat_dict)
    concat_df = concat_df.drop(['__split_column__'], axis=1)

    return concat_df

def normalize_json(y):
    """Converts a nested json string into a flat dict
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name +  a + '_')
        elif type(x) is list:
            for a in x:
                flatten(a, name)
        else:
            out[str(name[:-1])] = convert_encoding(x)
    flatten(y)
    return out


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

import sys
import pandas as pd
import numpy as np
import json
from pandas.io import gbq
from gcloud import storage
from cStringIO import StringIO

#------------------------------------------
# This script reads the new line demilited JSON file 
#  and tries to generate the schema based on the column values
#------------------------------------------
filename = sys.argv[1]
filehandle = open(filename, "r")

# convert new line JSON into dict; 
json_string = '[%s]' % ','.join(filehandle.readlines())

# a little time consuming, but is worth converting into dataframe
df = pd.read_json(json_string)  # this can be replaced by read_csv for tab-demilited files

filehandle.close()

# use gbq generate bq schema
schema = gbq.generate_bq_schema(df, default_type='STRING')["fields"]

print json.dumps(schema, indent=4)


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
"""Convert results from BigQuery to a dataframe
"""

from bigquery_etl.utils import sync_query
import pandas as pd
import numpy as np

def run(project_id, query):
    """Runs a sync query and onverts the results to a dataframe
    """
    # query a table
    results = sync_query.main(project_id, query, timeout=1, num_retries=5)
    data_df = pd.DataFrame(results)
    data_df = data_df.fillna(value=np.nan)
    return data_df


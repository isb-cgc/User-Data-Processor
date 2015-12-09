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

"""Script to download parse hgnc approved symbols file
>Download latest file
>curl -H"Accept:application/json" http://rest.genenames.org/fetch/status/Approved
"""

import json
import pandas as pd
from urllib2 import Request, urlopen

# Download latest file
print 'Downloading the HGNC annotations file'
req = Request('http://rest.genenames.org/fetch/status/Approved')
req.add_header('Accept', 'application/json')
response = urlopen(req)
elevations = response.read()
data = json.loads(elevations)


row_list = []
for hgnc in data["response"]['docs']:

    try:
        row_list.append(
            {
                'symbol': hgnc['symbol'],
                'entrez_id': hgnc['entrez_id']
            }
        )
    except Exception as exp:
        print exp
        pass
df = pd.DataFrame(row_list)
print df.to_csv(sep='|', index=False) # feed this file into the algorithm

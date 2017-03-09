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

import json
import  sys
import httplib2
import pprint
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
import time
import os
from os.path import basename
import argparse
from oauth2client import tools

try:
   parser = argparse.ArgumentParser(parents=[tools.argparser])
   flags = parser.parse_args()
   
   
   FLOW = flow_from_clientsecrets('client_secrets.json',scope='https://www.googleapis.com/auth/bigquery')
   
   storage = Storage('bigquery_credentials.dat')
   credentials = storage.get()
   
   if credentials is None or credentials.invalid:
     # Run oauth2 flow with default arguments.
     credentials = tools.run_flow(FLOW, storage, flags)
   
   http = httplib2.Http()
   http = credentials.authorize(http)
   
   bigquery_service = build('bigquery', 'v2', http=http)
   jobCollection = bigquery_service.jobs()
   print jobCollection

except TypeError, error:
  # Handle errors in constructing a query.
  print ('There was an error in constructing your query : %s' % error)

except HttpError, error:
  # Handle API errors.
  print ('Arg, there was an API error : %s : %s' %
         (error.resp.status, error._get_reason()))

except AccessTokenRefreshError:
  # Handle Auth errors.
  print ('The credentials have been revoked or expired, please re-run '
         'the application to re-authorize')
#
# except client.AccessTokenRefreshError:
#   print ('The credentials have been revoked or expired, please re-run'
#     'the application to re-authorize.')

except Exception as error:
   print (error)



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
import os
import tempfile
import time
from StringIO import StringIO

import pandas as pd
from gcloud import storage
from retrying import retry

class GcsConnector(object):
    """Google Cloud Storage Connector
    """
    def __init__(self, project, bucket_name, config, tempdir='/tmp', logger=None):
        # connect to the cloud bucket
        current_cred = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        if logger:
            logger.log_text("Existing Google Credential: {0}".format(current_cred), severity='INFO')
        new_cred = config['privatekey_path']
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = new_cred
        if logger:
            logger.log_text("New Google Credential: {0}".format(new_cred), severity='INFO')
        self.client = storage.Client(project)
        self.bucket = self.client.get_bucket(bucket_name)
        self.tempdir = tempdir
        self.logger = logger
        self.config = config
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = current_cred
        if logger:
            logger.log_text("Restoring Google Credential: {0}".format(current_cred), severity='INFO')

    #--------------------------------------
    # uploads a file to the bucket
    # this is just a wrapper around the main gcloud package
    # deprecated : gcloud 7 now has a direct function to create the blob
    # Note this is different from upload_from_file, which accepts a file-like object
    # Check if the blob exits
    #--------------------------------------
    def upload_from_filename(self, local_filename, dest_filename):
        if self.logger:
            self.logger.log_text("Uploading file: {0} to bucket".format(local_filename), severity='INFO')
        try:
            self.bucket.upload_file(local_filename, dest_filename)
        except Exception as e:
            if self.logger:
                self.logger.log_text("error on upload: {0}".format(str(e)), severity='ERROR')
            raise
        if self.logger:
            self.logger.log_text("Uploaded file: {0} to bucket".format(dest_filename), severity='INFO')
        return True

    #-------------------------------------------
    # Download the file to disk or file-like object
    # if rollover is true , it writes to the disk
    #-------------------------------------------
    def download_blob_to_file(self, blobname, rollover=False):
        blobname = '/'.join(blobname)
        blob = self.bucket.get_blob(blobname)

        if not blob  :
           raise Exception ('No blob found for the key:' + str(blobname))

        # if rollover is true, write to temp file on disk 
        # if false, write to StringIO buffer
        if rollover:
           temp_data_file = self.create_tempfile()
           blob.download_to_filename(temp_data_file.name)
        else:
           if self.logger:
               self.logger.log_text("StringIO", severity='INFO')
           temp_data_file = StringIO() # string buffer
           blob.download_to_file(temp_data_file)

        temp_data_file.seek(0)

        return temp_data_file 


    #-------------------------------------------
    # create a temp file.
    # this create a Named temp file
    #-------------------------------------------      
    def create_tempfile(self):
         temp_file = tempfile.NamedTemporaryFile(bufsize=0, suffix=str(time.time()), dir = self.tempdir)
         return temp_file

    #------------------------------------------
    # search files in the bucket by pattern
    #------------------------------------------
    def search_files(self, search_patterns, regex_search_pattern, prefixes=["tcga/"]):
        files_info = []
        for prefix in prefixes:
            if self.logger:
                self.logger.log_text("Searching for files with search patters - {0}, {1}, and prefix - {2}".format(search_patterns, regex_search_pattern.pattern, prefix), severity='INFO')
            for blob in self.bucket.list_blobs(prefix=prefix):
                filename = blob.name
                size = blob.size
                timestamp = blob.updated

                # search pattern
                if not all(x in filename for x in search_patterns):
                   continue
        
                if not regex_search_pattern.match(filename):
                   continue
        
                files_info.append({
                    'filename': filename, 
                    'size': size,
                    'timestamp': timestamp
                    
                })

        df = pd.DataFrame(files_info)
        if self.logger:
            self.logger.log_text('Found {0} files in the bucket matching the pattern'.format(len(df.index)), severity='INFO')
        return df

    def retry_if_result_none(result):
        """Return True if we should retry (in this case when result is None), False otherwise"""
        return result is not True

    @retry(retry_on_result=retry_if_result_none, wait_exponential_multiplier=2000, wait_exponential_max=10000, stop_max_delay=60000, stop_max_attempt_number=3)
    def upload_blob_from_string(self, blobname, df_stringIO, metadata={}):
        # upload the file
        upload_blob = storage.blob.Blob(blobname, bucket=self.bucket)
        upload_blob.upload_from_string(df_stringIO)
        # set blob metadata
        if metadata:
            if self.logger:
                self.logger.log_text("Setting object metadata", severity='INFO')
            upload_blob.metadata = metadata
            upload_blob.patch()

        # check if the uploaded blob exists. Just a sanity check
        if upload_blob.exists():
            if self.logger:
                self.logger.log_text("The uploaded file {0} has size {1} bytes.".format(blobname, upload_blob.size), severity='INFO')
            return True
        else:
            raise Exception('File upload failed - {0}.'.format(blobname))

  
    #----------------------------------------
    # Convert a dataframe into newline-delimited JSON string
    #  -- should work for a small to medium files
    # if rollover_file is true
    # works only in a single bucket
    # set the object metadata
    #----------------------------------------
    @retry(retry_on_result=retry_if_result_none, wait_exponential_multiplier=2000, wait_exponential_max=10000, stop_max_delay=60000, stop_max_attempt_number=3)
    def convert_df_to_njson_and_upload(self, df, destination_blobname, metadata={}, tmp_bucket='isb-cgc-dev'):

        if self.logger:
            self.logger.log_text("Converting dataframe into a new-line delimited JSON file", severity='INFO')

        file_to_upload = StringIO()

        for i, rec in df.iterrows():
            file_to_upload.write(rec.convert_objects(convert_numeric=False).to_json() + "\n")
        file_to_upload.seek(0)

        bucket = self.client.get_bucket(tmp_bucket)
        upload_blob = storage.blob.Blob(destination_blobname, bucket=bucket)

        upload_blob.upload_from_string(file_to_upload.getvalue())
        # set blob metadata
        if metadata:
            if self.logger:
                self.logger.log_text("Setting object metadata", severity='INFO')
            upload_blob.metadata = metadata
            upload_blob.patch()
        file_to_upload.close()

        # check if the uploaded blob exists. Just a sanity check
        if upload_blob.exists():
            if self.logger:
                self.logger.log_text("The uploaded file {0} has size {1} bytes.".format(destination_blobname, upload_blob.size), severity='INFO')
            return True
        else:
            raise Exception('File upload failed - {0}.'.format(destination_blobname)) 
     
    def check_blob_exists(self, blob):
        """
        Checks if a blob exists
        Returns True or False
        """
        return self.bucket.get_blob(blob).exists()

    def delete_blob(self, blob):
        return self.bucket.get_blob(blob).delete()

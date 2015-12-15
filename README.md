ISB-CGC-User-Data-Processor
================

This is a repository for the user data processor. It was originally from the isb-cgc-data-proc repository, but seemed different enough to require its own repository.

The bigquery_etl module has been copied from the isb-cgc-data-proc repository.

This code is deployed on a Jenkins slave node and run by Jenkins. 

###General process of the processor:

1. Read through the config file, pull out relevant bits of information and separate all the user_gen data files from the rest of the molecular datatypes.
2. Process all user_gen files together as one
  2.1 Download file from cloud storage.
  2.2 Get column mappings, renaming columns to the mapping provided.
  2.3 Insert data of each file into metadata_data table for the study.
  2.4 Merge all files into one dataframe on SampleBarcode. NOTE: This assumes that all user_gen files provided will have a mapping to SampleBarcode.
  2.5 Insert the table into the user's metadata_samples table for the study.
  2.6 Create and Update BigQuery table by writing to a temporary file and uploading that file to BigQuery.
  2.7 Generate new feature definitions for each column in metadata_samples table except SampleBarcode.
  2.8 Delete temporary file.
3. Process each molecular datatype file individually
  3.1 Download file from cloud storage.
  3.2 Convert file to dataframe.
  3.3 Get column mappings that map the columns in the file to the correct columns in the BigQuery Schema. NOTE: Each molecular file is to have this format:
  
  Symbol|Feature ID|Tab|Sample ID 1|Sample ID 2|Sample ID 3
  ------|----------|---|-----------|-----------|-----------
  BRCA|BRCA ID|Optional Information|Value|Value|Value
  EGFR|EGFR ID|Optional Information|Value|Value|Value
  TP53|TP53 ID|Optional Information|Value|Value|Value
  
  3.4 Convert matrix into denormalized rows based on sample id to store in BigQuery 
  3.5 Generate metadata_data rows from samples in file and insert into metadata_data table for the study.
  3.6 Update metadata_samples table for samples that exist and insert new samples that don't exist.
  3.7 Generate new feature definitions for datatype based on unique symbols.
  3.8 Create and Update BigQuery table by writing ot a temporary file and uploading that file to BigQuery.
  3.9 Delete temporary file.
  
###Big Query Schemas:

Molecular Data Type Schema (mrna, mirna, protein, meth)

Name|Type|Description
----|----|-----------
SampleBarcode|String|Sample barcode
Project|INTEGER|User's Project ID this value is associated with. This refers to the in-app Project model.
Study|INTEGER|User's Study ID this value is associated with. This refers to the in-app Study model.
Platform|STRING|Platform used to generate this value.
Pipeline|STRING|Pipeline used to generate this value.
Symbol|STRING|Can represent the gene symbol, mirna name. This column is mainly used for filtering depending on the datatype. 
ID|STRING|Can represent the gene ID, mirna ID, probe ID. This column is mainly used for filtering depending on the datatype. 
Tab|STRING|Can represent extra information such as protein name. This is an additional column that can be used for storing extra information.
Level|FLOAT|Actual values associated to the sample and datatype. This represents beta levels, expression levels, or counts.

User Generated Data Schema (user_gen)

Name|Type|Description
----|----|-----------
SampleBarcode|String|Sample barcode
Project|INTEGER|User's Project ID this value is associated with. This refers to the in-app Project model.
Study|INTEGER|User's Study ID this value is associated with. This refers to the in-app Study model.

These are the only columns that are required in this schema. All other columns are generated when the data is provided and customized for the data processed.


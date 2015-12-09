ISB-CGC-data-proc
================
data_upload: The data upload code uploads the contents of the TCGA DCC into GCS.

cghub: used by the data_upload code to obtain metadata on the bam and fasta files 
that reside in CGHub

tcga_etl_pipeline: ETL Pipeline to load various datasets into BigQuery.  Uses the metadata output of data_upload
and uses bigquery_etl to do thee actual creation of the BigQuery tables

bigquery_etl: This module provides tools for data extraction, transformation, and loading to Google BigQuery.

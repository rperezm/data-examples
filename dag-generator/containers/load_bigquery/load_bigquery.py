import logging
import os
import sys
from google.cloud import bigquery
from google.cloud import storage


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "my-service-account.json"

# Select max date from file pattern and return the final file
def get_file():
    storage_client = storage.Client()
    print(f'Bucket Name : {bucket_name}')
    print(f'File Prefix : {file_prefix}')

    blobs = storage_client.list_blobs(bucket_name, prefix=file_prefix)
    timestamp = max([blob.name.split('_')[-1].split('.')[0] for blob in blobs])
    print(f'Max Timestamp : {timestamp}')

    file_name = f'{file_prefix}_{timestamp}'+extension_file
    return file_name

# Load file to bigquery from specific file
def load_file():
    print(f'Project : {project}')
    print(f'Dataset : {dataset}')
    print(f'Table : {table}')
    print(f'File : {file_name}')

    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if wrt_disposition == 'WRITE_APPEND' else bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1
    job_config.field_delimiter = '|'
    job_config.allow_quoted_newlines = True
    job_config.project = project

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = f"gs://{bucket_name}/{file_name}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))
    
    uri = f"gs://{bucket_name}/{file_name}"
   
if __name__ == '__main__':
    project = sys.argv[1]
    dataset = sys.argv[2]
    table = sys.argv[3]
    wrt_disposition = sys.argv[4]
    bucket_name = sys.argv[5]
    file_prefix = sys.argv[6] # raw_<Business Unit>_<location>_<enviroment>_<origin system>_<origin table>_YYMMDD_
    extension_file = sys.argv[7]

    file_name = get_file()
    load_file()

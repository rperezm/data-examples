import sys
import os
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "my-service-account.json"

# Move Files
# Select max date from file pattern and return the final file
def get_file(p_src, p_file_prefix, p_extension_file, p_process_date):

    storage_client = storage.Client()
    print(f'Bucket Name : {p_src}')
    print(f'File Prefix : {p_file_prefix}')

    blobs = storage_client.list_blobs(src, prefix=f'{p_file_prefix}_{p_process_date}')
    timestamp = max([blob.name.split('.')[0].split('_')[-1] for blob in blobs])
    print(f'Max Timestamp : {timestamp}')
    
    file_name = f'{p_file_prefix}_{p_process_date}_{timestamp}{p_extension_file}'
    
    return file_name

# Move file from landing to raw or raw to landing bucket
def move_files():

    file_name = get_file(src, f'{job_id}/{file_prefix}', extension_file, process_date)
    print(f'File Name : {file_name}')
    storage_client = storage.Client()
    
    """Copies a blob from one bucket to another with a new name."""
    src_bucket = storage_client.get_bucket(src)
    src_blob = src_bucket.blob(file_name)
    dst_bucket = storage_client.get_bucket(dst)

    src_bucket.copy_blob(src_blob, dst_bucket, file_name.replace(job_id,table))

    print(f'File {src_blob.name} in bucket {src_bucket.name} copied to bucket {dst_bucket.name}.')
    delete_file(src,file_name)

# Delete file from landing or raw bucket
def delete_file(src,file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(src)
    blob = bucket.blob(file_name)

    blob.delete()
    print(f'file {file_name} deleted from landing.')


if __name__ == '__main__':

    src = sys.argv[1]
    dst = sys.argv[2]
    table = sys.argv[3]
    job_id = sys.argv[4]
    extension_file = sys.argv[5]
    file_prefix = sys.argv[6]
    process_date = sys.argv[7]
    
    move_files()

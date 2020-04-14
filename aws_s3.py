import boto3
import io
import urllib3
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

access_ID = '' #add AWS access ID
secret_key = '' #add AWS secret key

session = boto3.Session(aws_access_key_id=access_ID, aws_secret_access_key=secret_key)
conn = session.resource('s3')
bucket_name = '' #add bucket name
prefix_name = 'query_history/'
source_path_name = 'source/'
target_path_name = 'target/'
processed_path_name = 'processed/'
failed_path_name = 'failed/'
management_path_name = 'management/'
bucket = conn.Bucket(bucket_name)


def upload_file(file_to_upload, path):
    csv_buffer = io.StringIO()
    if management_path_name in file_to_upload:
        file_to_upload.to_csv(csv_buffer)
    else:
        file_to_upload.to_csv(csv_buffer, index=False)
    conn.Object(bucket_name, prefix_name + path).put(Body=csv_buffer.getvalue())


def read_to_csv(path, is_management_table):
    obj = conn.meta.client.get_object(Bucket=bucket_name, Key=path)

    if is_management_table:
        df = pd.read_csv(obj['Body'], delimiter='\001', names=['filename', 'query_id', 'traceback', 'query'],
                         escapechar='\\', lineterminator='\n', engine='python')
    else:
        df = pd.read_csv(obj['Body'], delimiter='\001',
                         names=['1', '2', '3', '4', '5', '6'], escapechar='\\', lineterminator='\n')
    return df


def get_list_of_files(path):
    return bucket.objects.filter(Delimiter='/', Prefix=prefix_name + path)


def move_file(path_from, path_to):
    copy_source = {
        'Bucket': bucket_name,
        'Key': path_from
    }
    conn.meta.client.copy(copy_source, Bucket=bucket_name, Key=prefix_name + path_to)
    conn.meta.client.delete_object(Bucket=bucket_name, Key=path_from)
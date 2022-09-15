from boto3.s3.transfer import TransferConfig
import boto3
import os
import sys
import threading
import json

BUCKET_NAME = "s3-object-operations-practice-examples"
WEBSITE_BUCKET_NAME = "s3staticwebsitehostingexample"


def s3_client():
    s3 = boto3.client('s3')
    """ :type: pyboto3.s3 """
    return s3


def s3_resource():
    return boto3.resource('s3')


def create_bucket(bucket_name):
    return s3_client().create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'ap-south-1'
    })


def update_bucket_policy(bucket_name):
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': [
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                'Resource': 'arn:aws:s3:::' + bucket_name + '/*'
            }
        ]
    }

    policy_string = json.dumps(bucket_policy)

    return s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy_string
    )


def upload_file(bucket_name):
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(file_path, bucket_name, "readme.txt")
    # put_object() is kind of similar to upload_file()


config = TransferConfig(multipart_threshold=1024 * 25,
                        max_concurrency=10,
                        multipart_chunksize=1024 * 25,
                        use_threads=True)


def upload_large_file(bucket_name):
    file_path = os.path.dirname(__file__) + '/large_file.pdf'
    key = 'multipart-test/large_file.pdf'
    s3Resource = s3_resource()
    s3Resource.Object(bucket_name, key).upload_file(file_path,
                                                    ExtraArgs={
                                                        'ContentType': 'text/pdf'},
                                                    Config=config,
                                                    Callback=ProgressPercentage(
                                                        file_path)
                                                    )


def multipart_download_boto3(bucket_name):
    file_path = os.path.dirname(__file__) + '/large_file.pdf'
    file_path1 = os.path.dirname(__file__)
    key = 'multipart-test/large_file.pdf'
    s3Resource = s3_resource()
    print("Download started...")
    s3Resource.Object(bucket_name, key).download_file(file_path,
                                                      Config=config)
    print("Download finished")


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def enable_versioning(bucket_name):
    s3_client().put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={
        'Status': 'Enabled'
    })


def host_static_website():
    s3 = boto3.client('s3', region_name='ap-south-1')
    """ :type : pyboto3.s3 """

    s3.create_bucket(
        Bucket=WEBSITE_BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1'
        }
    )

    update_bucket_policy(WEBSITE_BUCKET_NAME)

    website_configuration = {
        'ErrorDocument': {'Key': 'error.html'},
        'IndexDocument': {'Suffix': 'index.html'}
    }

    s3_client().put_bucket_website(
        Bucket=WEBSITE_BUCKET_NAME,
        WebsiteConfiguration=website_configuration
    )

    index_file = os.path.dirname(__file__) + '/index.html'
    error_file = os.path.dirname(__file__) + '/error.html'

    s3_client().put_object(Bucket=WEBSITE_BUCKET_NAME, ACL='public-read', Key='index.html',
                           Body=open(index_file).read(), ContentType='text/html')
    s3_client().put_object(Bucket=WEBSITE_BUCKET_NAME, ACL='public-read', Key='error.html',
                           Body=open(error_file).read(), ContentType='text/html')


# create_bucket(BUCKET_NAME)
# upload_file(BUCKET_NAME)
# upload_large_file(BUCKET_NAME)
# multipart_download_boto3(BUCKET_NAME)
# enable_versioning(BUCKET_NAME)
# host_static_website()

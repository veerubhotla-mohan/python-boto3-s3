import json
import boto3

BUCKET_NAME = "s3-object-operations-practice-examples"


def s3_clint():
    s3 = boto3.client('s3')
    """ :type: pyboto3.s3 """
    return s3


def create_bucket(bucket_name):
    return s3_clint().create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'ap-south-1'
    })


def create_bucket_policy(bucket_name, policy):
    return s3_clint().put_bucket_policy(Bucket=bucket_name, Policy=policy)


def update_bucket_policy(bucket_name, policy):
    # Same as creating a bucket policy
    return s3_clint().put_bucket_policy(Bucket=bucket_name, Policy=policy)


def list_buckets():
    return (s3_clint().list_buckets()["Buckets"])


def get_bucket_policy(bucket_name):
    return s3_clint().get_bucket_policy(Bucket=bucket_name)


def get_bucket_encryption(bucket_name):
    return s3_clint().get_bucket_encryption(Bucket=bucket_name)


def put_bucket_encryption(bucket_name):
    return s3_clint().put_bucket_encryption(Bucket=bucket_name, ServerSideEncryptionConfiguration={
        "Rules": [
            {
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'AES256'
                }
            }
        ]
    })


def delete_bucket(bucket_name):
    return s3_clint().delete_bucket(Bucket=bucket_name)


# 1. Creating a new bucket
# print(create_bucket(BUCKET_NAME))


# 2. Attaching a policy to the S3 bucket
policy = {
    "Id": "s3-bucket-policy",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AddPolicy",
            "Action": "s3:*",
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::mohan-s3-bucket-2022-sept/*",
            "Principal": "*"
        }
    ]
}
policy_json = json.dumps(policy)
# print(create_bucket_policy(BUCKET_NAME, policy_json))

# 3. List all the buckets
# print(list_buckets())

# 4. Get bucket properties
# print(get_bucket_policy(BUCKET_NAME))  # getting policy attached to a bucket
# print(get_bucket_encryption(BUCKET_NAME))  # getting the encryption details

# 5. Updating bucket policy
policy = {
    "Id": "s3-bucket-policy",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AddPolicy",
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::mohan-s3-bucket-2022-sept/*",
            "Principal": "*"
        }
    ]
}
policy_json = json.dumps(policy)
# print(update_bucket_policy(BUCKET_NAME, policy_json))

# 6. Default encryption
# print(put_bucket_encryption(BUCKET_NAME))

# 7. Delete a bucket
# print(delete_bucket(BUCKET_NAME))

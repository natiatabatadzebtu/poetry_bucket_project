import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
import argparse
from hashlib import md5
from time import localtime
import mimetypes
mimetypes.init()

load_dotenv()

def init_client():
    try:
        client = boto3.client("s3",
                    aws_access_key_id=getenv("aws_access_key_id"),
                    aws_secret_access_key=getenv("aws_secret_access_key"),
                    aws_session_token=getenv("aws_session_token"),
                    region_name=getenv("aws_region_name")
                )
        client.list_buckets()

        return client
    except ClientError as e:
        logging.error(e)
    except:
        logging.error("Unexpected error")


def list_buckets(aw3_s3_client):
    try:
        return aw3_s3_client.list_buckets()
    except ClientError as e:
        logging.error(e)
        return False

def create_bucket(aw3_s3_client, bucket_name, region='us-west-2'):
    try:
        location = {'LocationConstraint': region}
        aw3_s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def delete_bucket(aw3_s3_client, bucket_name):
    try:
        aw3_s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def bucket_exists(aw3_s3_client, bucket_name):
    try:
        response = aw3_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False

def download_file_and_upload_to_s3(aws_s3_client, bucket_name, url, file_name, keep_local=False):
    type = mimetypes.guess_type(url)[0]
    extension = mimetypes.guess_extension(type)
    allowed_extensions = [ '.bmp', '.jpg', '.jpeg', '.png', '.webp', '.mp4' ]
    file_name = f'{file_name}{extension}'
    if extension in allowed_extensions:
        from urllib.request import urlopen
        import io
        with urlopen(url) as response:
            content = response.read()
            try:
                aws_s3_client.upload_fileobj(
                    Fileobj=io.BytesIO(content),
                    Bucket=bucket_name,
                    ExtraArgs={'ContentType': type},
                    Key=file_name
                )
            except Exception as e:
                logging.error(e)
    
        if keep_local:
            with open(file_name, mode='wb') as file:
                file.write(content)
    
        # public URL
        return "https://s3-{0}.amazonaws.com/{1}/{2}".format(
            'us-west-2',
            bucket_name,
            file_name
        )
    else:
        print('Extension is invalid')
        return False

def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    try:
        response = aws_s3_client.put_object_acl(
            ACL="public-read",
            Bucket=bucket_name,
            Key=file_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False

def generate_public_read_policy(bucket_name):
    import json
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowListingOfUserFolder",
                "Action": ["s3:ListBucket"],
                "Effect": "Allow",
                "Principal": {
                    "AWS": "*"
                },
                "Resource": [f"arn:aws:s3:::{bucket_name}"],
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": ["dev/", "test/"],
                        "s3:delimiter": ["/"]
                    }
                }
            },
            {
                "Sid": "AllowListingOfUserFolder",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "*"
                },
                "Action": ["s3:*"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/folder/*"]
            }
        ],
    }

    return json.dumps(policy)

def create_bucket_policy(aws_s3_client, bucket_name):
    aws_s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=generate_public_read_policy(bucket_name)
    )
    print("Bucket policy created successfully")

def read_bucket_policy(aws_s3_client, bucket_name):
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        print(policy_str)
        return True
    except ClientError as e:
        logging.error(e)
        return False

if __name__ == "__main__":
    s3_client = init_client()

    # buckets = list_buckets(s3_client)

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--bucketName')
    # args = parser.parse_args()

    # if read_bucket_policy(s3_client, args.bucketName):
    #     print(f'Bucket: { args.bucketName } already has policy')
    # else:
    #     create_bucket_policy(s3_client, args.bucketName)

    # if bucket_exists(s3_client, args.bucketName):
    #     delete_bucket(s3_client, args.bucketName)
    # else:
    #     print(f'Bucket: { args.bucketName } does not exist')

    # if bucket_exists(s3_client, args.bucketName):
    #     print(f'Bucket: { args.bucketName } already exist')
    # else:
    #     create_bucket(s3_client, args.bucketName)
        

    # print(f'created bucket status: { create_bucket(s3_client, "new-bucket-btun")}')
    # print(f'deleted bucket status: { delete_bucket(s3_client, "new-bucket-btun")}')
    # print(f'Bucket exists: { bucket_exists(s3_client, "new-bucket-btun")}')
    # print(f"set read status: {set_object_access_policy(s3_client, 'new-bucket-natia', 'image_file_78bc222b20d1ff69cdf1290a7537d5fd.jpg')}")
    # print(f'Bucket Policy: { create_bucket_policy(s3_client, "new-bucket-natia") }')
    # print(f'Bucket Policy: { read_bucket_policy(s3_client, "new-bucket-natia") }')

    # if buckets:
    #     for bucket in buckets['Buckets']:
    #         print(f' {bucket["Name"]}')

    print(download_file_and_upload_to_s3(
        s3_client, 'new-bucket-natia',
        'https://www.coreldraw.com/static/cdgs/images/free-trials/img-ui-cdgsx.jpg',
        f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}',
        keep_local=True
    ))



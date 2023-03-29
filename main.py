import logging
from botocore.exceptions import ClientError
from auth import init_client
from bucket.crud import list_buckets, create_bucket, delete_bucket, bucket_exists, delete_file
from bucket.policy import read_bucket_policy, assign_policy
from object.crud import download_file_and_upload_to_s3, get_objects, upload_file, upload_file_obj, upload_file_put, multipart_upload, put_policy
from bucket.encryption import set_bucket_encryption, read_bucket_encryption
import argparse
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(
  description="CLI program that helps with S3 buckets.",
  usage='''
    How to download and upload directly:
    short:
        python main.py -bn new-bucket-btu-7 -ol https://cdn.activestate.com/wp-content/uploads/2021/12/python-coding-mistakes.jpg -du
    long:
       python main.py  --bucket_name new-bucket-btu-7 --object_link https://cdn.activestate.com/wp-content/uploads/2021/12/python-coding-mistakes.jpg --download_upload

    How to list buckets:
    short:
        python main.py -lb
    long:
        python main.py --list_buckets

    How to create bucket:
    short:
        -bn new-bucket-btu-1 -cb -region us-west-2
    long:
        --bucket_name new-bucket-btu-1 --create_bucket --region us-west-2

    How to assign missing policy:
    short:
        -bn new-bucket-btu-1 -amp
    long:
        --bn new-bucket-btu-1 --assign_missing_policy
    ''',
  prog='main.py',
  epilog='DEMO APP FOR BTU_AWS')

parser.add_argument(
  "-lb",
  "--list_buckets",
  help="List already created buckets.",
  # https://docs.python.org/dev/library/argparse.html#action
  action="store_true")

parser.add_argument(
  "-cb",
  "--create_bucket",
  help="Flag to create bucket.",
  choices=["False", "True"],
  type=str,
  nargs="?",
  # https://jdhao.github.io/2018/10/11/python_argparse_set_boolean_params
  const="True",
  default="False")

parser.add_argument("-bn",
                    "--bucket_name",
                    type=str,
                    help="Pass bucket name.",
                    default=None)

parser.add_argument("-bl",
                    "--bucket_list",
                    help="Bucket lists",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="True")

parser.add_argument("-bc",
                    "--bucket_check",
                    help="Check if bucket already exists.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="True")

parser.add_argument("-region",
                    "--region",
                    type=str,
                    help="Region variable.",
                    default=None)

parser.add_argument("-db",
                    "--delete_bucket",
                    help="flag to delete bucket",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-be",
                    "--bucket_exists",
                    help="flag to check if bucket exists",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-rp",
                    "--read_policy",
                    help="flag to read bucket policy.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-arp",
                    "--assign_read_policy",
                    help="flag to assign read bucket policy.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-amp",
                    "--assign_missing_policy",
                    help="flag to assign read bucket policy.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-du",
                    "--download_upload",
                    choices=["False", "True"],
                    help="download and upload to bucket",
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-ol",
                    "--object_link",
                    type=str,
                    help="link to download and upload to bucket",
                    default=None)

parser.add_argument("-lo",
                    "--list_objects",
                    type=str,
                    help="list bucket object",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-ben",
                    "--bucket_encryption",
                    type=str,
                    help="bucket object encryption",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-rben",
                    "--read_bucket_encryption",
                    type=str,
                    help="list bucket object",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-uf",
                    "--upload_file",
                    type=str,
                    help="Upload file",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-ufo",
                    "--upload_file_object",
                    type=str,
                    help="Upload file object",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-ufp",
                    "--upload_file_put",
                    type=str,
                    help="Upload file put",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-mu",
                    "--multipart_upload",
                    type=str,
                    help="Multipart Upload file",
                    nargs="?",
                    const="True",
                    default="False")
                    
parser.add_argument("-fl",
                    "--file_link",
                    type=str,
                    help="link to file upload to bucket",
                    default=None)

parser.add_argument("-pp",
                    "--put_policy",
                    type=str,
                    help="Put Policy",
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-df",
                    "--delete_file",
                    help="flag to delete file",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-fn",
                    "--file_name",
                    type=str,
                    help="name to file",
                    default=None)
                    

def main():
  s3_client = init_client()
  args = parser.parse_args()

  if args.bucket_name:
    if args.create_bucket == "True":
      if not args.region:
        parser.error("Please provide region for bucket --region REGION_NAME")
      if (args.bucket_check == "True") and bucket_exists(
          s3_client, args.bucket_name):
        parser.error("Bucket already exists")
      if create_bucket(s3_client, args.bucket_name, args.region):
        print("Bucket successfully created")

    if (args.delete_bucket == "True") and delete_bucket(
        s3_client, args.bucket_name):
      print("Bucket successfully deleted")

    if args.bucket_exists == "True":
      print(f"Bucket exists: {bucket_exists(s3_client, args.bucket_name)}")

    if args.read_policy == "True":
      print(read_bucket_policy(s3_client, args.bucket_name))

    if args.assign_read_policy == "True":
      assign_policy(s3_client, "public_read_policy", args.bucket_name)

    if args.assign_missing_policy == "True":
      assign_policy(s3_client, "multiple_policy", args.bucket_name)

    if args.object_link:
      if (args.download_upload == "True"):
        print(
          download_file_and_upload_to_s3(s3_client, args.bucket_name,
                                         args.object_link))
    if args.bucket_encryption == "True":
      if set_bucket_encryption(s3_client, args.bucket_name):
        print("Encryption set")
    if args.read_bucket_encryption == "True":
      print(read_bucket_encryption(s3_client, args.bucket_name))

    if args.upload_file == "True":
      if args.file_link:
        print(upload_file(s3_client, args.file_link, args.bucket_name))
    
    if args.upload_file_object == "True":
      if args.file_link:
        print(upload_file_obj(s3_client, args.file_link, args.bucket_name))
    
    if args.upload_file_put == "True":
      if args.file_link:
        print(upload_file_put(s3_client, args.file_link, args.bucket_name))

    if args.multipart_upload == "True":
      if args.file_link:
        print(multipart_upload(s3_client, args.file_link, args.bucket_name))

    if (args.list_objects == "True"):
      get_objects(s3_client, args.bucket_name)

    if (args.put_policy == "True"):
      put_policy(s3_client, args.bucket_name)

    if (args.delete_file == "True") and args.file_name and delete_file(
      s3_client, args.bucket_name, args.file_name):
      print("File successfully deleted")

  if (args.bucket_list):
    buckets = list_buckets(s3_client)
    if buckets:
      for bucket in buckets['Buckets']:
        print(f'  {bucket["Name"]}')


if __name__ == "__main__":
  try:
    main()
  except ClientError as e:
    logging.error(e)

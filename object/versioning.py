
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

def list_object_versions(aws_s3_client, bucket_name, file_name):
    versions = aws_s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=file_name
    )

    for version in versions['Versions']:
        version_id = version['VersionId']
        file_key = version['Key'],
        is_latest = version['IsLatest']
        modified_at = version['LastModified']

        # response = aws_s3_client.get_object(
        #     Bucket=bucket_name,
        #     Key=file_key[0],
        #     VersionId=version_id,
        # )
        # data = response['Body'].read()

        print(version_id, file_key, is_latest, modified_at)


def delete_old_files(aws_s3_client, bucket_name, file_name):
    versions = aws_s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=file_name
    )
    for version in versions['Versions']:
        if datetime.date(version['LastModified']) < (datetime.date(datetime.now() - relativedelta(months=6))):
            version_id = version['VersionId']
            file_key = version['Key'],

            response = aws_s3_client.delete_object(
                Bucket=bucket_name,
                Key=file_key[0],
                VersionId=version_id,
            )
        
            print(version_id, file_key)


def rollback_to_version(aws_s3_client, bucket_name, file_name, version):
    aws_s3_client.copy_object(
        Bucket=bucket_name,
        Key=file_name,
        CopySource={'Bucket': bucket_name, 'Key': file_name, 'VersionId': version}
    )

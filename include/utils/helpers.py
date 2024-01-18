from typing import Optional

import boto3
from botocore.errorfactory import ClientError


def s3_object_exists(bucket: str, key: str, s3: Optional[boto3.client] = None) -> bool:
    """
    Check if an object exists in an S3 bucket.

    Args:
        bucket: The name of the S3 bucket.
        key: The key of the object in the S3 bucket.
        s3: An optional pre-initialized boto3 S3 client.

    Returns:
        True if the object exists, False otherwise.
    """
    if s3 is None:
        s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise e

    return True


def get_aws_parameter(parameter_name: str, region: str = 'us-west-2', ssm: Optional[boto3.client] = None) -> str:
    """
    Get a parameter from AWS Systems Manager Parameter Store.

    Args:
        parameter_name: The name of the parameter to retrieve.
        ssm: An optional pre-initialized boto3 SSM client.
        region: The AWS region to use.

    Returns:
        The value of the parameter.
    """
    if ssm is None:
        ssm = boto3.client("ssm", region_name=region)

    response = ssm.get_parameter(Name=parameter_name)
    return response["Parameter"]["Value"]

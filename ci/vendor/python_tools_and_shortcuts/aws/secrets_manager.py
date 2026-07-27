import boto3

def get_secret(secret_name: str, region_name: str = "us-west-2") -> str:
    client = boto3.client('secretsmanager', region_name=region_name)

    response = client.get_secret_value(SecretId=secret_name)

    # SecretString is where normal text/JSON secrets live
    if 'SecretString' in response:
        secret_str = response['SecretString']
    else:
        # If stored as binary
        secret_str = response['SecretBinary'].decode('utf-8')

    return secret_str

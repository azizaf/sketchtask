import boto3
import botocore
import psycopg2
import os
s3 = boto3.resource("s3")

legacy_bucket = "legacy-s3-aziz-123"
production_bucket = "prods3-aziz-123"

def bucket_exists(bucket_name: str) -> bool:
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        return False

def object_exists(bucket_name: str, key: str) -> bool:
    try:
        s3.Object(bucket_name, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            return True
    else:
        return True

def get_connection():
    connection = psycopg2.connect(
        database=os.environ.get('MY_DB'),
        user=os.environ.get('MY_USER'),
        password=os.environ.get('MY_PASS'),
        host=os.environ.get('MY_HOST'),
        port='5432'
    )
    print(connection)

    __query = """
    CREATE TABLE mytable (
        id serial primary key,
        path_key varchar(255) not null
    );
    INSERT INTO mytable
        (path_key) VALUES 
        ('image/cat.drawio.png'),
        ('image/micro.drawio.png'),
        ('image/aang.drawio.png'),
        ('avatar/nano.drawio.png');
    """
    cursor = connection.cursor()
    cursor.execute(__query)
    cursor.close()

    return connection


def main():
    # check for the buckets
    if not bucket_exists(legacy_bucket) or not bucket_exists(production_bucket):
        raise Exception(
            "Either the legacy bucket or the production bucket does not exist")

    connection = get_connection()

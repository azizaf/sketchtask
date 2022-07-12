import boto3
import botocore
import psycopg2
import os
s3 = boto3.resource("s3")

#replace with your bucket names 
legacy_bucket = "legacy-s3-aziz-123"
production_bucket = "prods3-aziz-123"

#check if bucket exists 
def bucket_exists(bucket_name: str) -> bool:
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        return False

#check if object exists 
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

#connection to postgres database 
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

def close_connection(connection):
    connection.close()

#checking the db for files that still have the legacy path 
def fetch_files_to_copy(connection) -> list[str]:
    SQL_QUERY = "SELECT id, path_key FROM mytable WHERE path_key LIKE 'image%'"

    # Executing a SQL query
    cursor = connection.cursor()
    cursor.execute(SQL_QUERY)

    # Fetch result
    return cursor.fetchall()

def fetch_files(connection) -> list[str]:
    SQL_QUERY = "SELECT id, path_key FROM mytable"

    # Executing a SQL query
    cursor = connection.cursor()
    cursor.execute(SQL_QUERY)

    # Fetch result
    return cursor.fetchall()

#function to copy objects from legacy to production bucket 
def copy_object(source_bucket_name: str, source_key: str, target_bucket_name: str, target_file_name: str) -> bool:
    error = False
    try:
        bucket = s3.Bucket(target_bucket_name)
        bucket.copy({
            'Bucket': source_bucket_name,
            'Key': source_key
        }, target_file_name)
    except:
        error = True
    else:
        error = False
    finally:
        return error

#updating file path in db 
def update_file_path_indb(connection, id: str, new_key: str):
    cursor = connection.cursor()
    query = f"""
    UPDATE mytable 
        SET path_key='{new_key}'
        WHERE id={id}
    """
    cursor.execute(query)
    cursor.close()

def main():
    # check for the buckets
    if not bucket_exists(legacy_bucket) or not bucket_exists(production_bucket):
        raise Exception(
            "Either the legacy bucket or the production bucket does not exist")

    connection = get_connection()

    # Get all file path from the database with a certain file path format
    file_paths = fetch_files_to_copy(connection)
    print(file_paths)

    # For each of the files from above,
    for file_obj in file_paths:
        key, file_name = file_obj[1].split("/")

        # Check if the file exists in the legacy bucket (false=skip)
        if not object_exists(legacy_bucket, f"image/{file_name}"):
            print(f"{key}/{file_name}", "does not exists in legacy")
            continue

        # Check if the file exists in the production bucket (false=skip)
        if object_exists(production_bucket, f"avatar/{file_name}"):
            print(f"{key}/{file_name}", "does exists in production")
            update_file_path_indb(
                connection, file_obj[0], f"avatar/{file_name}")
            continue

        copied = copy_object(
            legacy_bucket, f"{key}/{file_name}", production_bucket, f"avatar/{file_name}")

        print("Copied", copied)
        if copied is not True:
            print(f"{key}/{file_name}", "Not copied")
            continue

        # else, update the database
        update_file_path_indb(connection, file_obj[0], f"avatar/{file_obj[1]}")

    # expecting the result of this to be []
    print("---"*10, "\n", fetch_files(connection))

    # close the connection to the database
    close_connection(connection)

def print_bucket_files(bucket_name):
    temp = client.list_objects(Bucket=bucket_name)['Contents']
    for i in temp:
        print(i.get('Key'))


if __name__ == "__main__":
    client = boto3.client("s3")
    #before transfer 
    print(f"{'*'*10}\nFiles in the legacy bucket")
    print_bucket_files(legacy_bucket)

    print(f"\n{'*'*10}\nFiles in the production bucket")
    print_bucket_files(production_bucket)
    
    #executing main function 
    print("\n")
    main()
    print("\n")

    #after transfer 
    print(f"{'*'*10}\nFiles in the legacy bucket")
    print_bucket_files(legacy_bucket)

    print(f"\n{'*'*10}\nFiles in the production bucket")
    print_bucket_files(production_bucket)
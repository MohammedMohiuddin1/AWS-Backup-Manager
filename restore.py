import boto3
import sys
import os
import botocore

# Global variables
s3 = boto3.resource("s3")
s3_client = boto3.client("s3")

"""
This is the main method responsible for traversing through files in the bucket and downloading them to a local directory
it takes in a bucket bath and a local directory path and does this accordingly
"""
def restore(bucket_dir_path, local_dir_path):
    print("Starting restore...")

    # Consistency with file naming conventions 
    local_dir_path = local_dir_path.replace("\\", "/")
    bucket_dir_path = bucket_dir_path.replace("\\", "/")

    # split to get appropriate values
    split = bucket_dir_path.split("::")
    try:
        bucket_name = split[0]
        bucket_directory = split[1]
    except IndexError:
        print("Error: please use valid format")
        return
    # Add and remove forward slashes from the end and the front for file naming convention
    if not bucket_directory.endswith("/"):
        bucket_directory += "/"
    if bucket_directory.startswith("/"):
        bucket_directory = bucket_directory[1:]
    if local_dir_path.startswith("/"):
        local_dir_path = local_dir_path[1:]
    
    # if the bucket doesn't exist simply just return since we don't have a location to download files from
    if not does_bucket_exist(s3_client, bucket_name):
        print("Bucket %s does not exist" % bucket_name)
        return
    # else get all the objects of the given bucket and bucket directory
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_directory)
    try: 
        for obj in objects["Contents"]:
            obj_key = obj["Key"]
            print(f"Downloading s3://{bucket_name}/{obj_key} to {local_dir_path}")
            download(s3_client, bucket_name, obj_key, local_dir_path)
    except KeyError:
        print(f"{bucket_directory} does not exist in {bucket_name}")
    print("All files restored succesfully")
            
# this is a helper method to simply loop through the clients buckets and see if the bucket exists
def does_bucket_exist(client, bucket_name):
    for bucket in client.list_buckets()["Buckets"]:
        if bucket["Name"] == bucket_name:
            return True
    return False 

# this is a helper method that downloads a file from the bucket to the local directory
def download(client, bucket_name, bucket_directory, local_directory):
    # create a new path by removing the first directory because we don't want a new initial folder
    new_path = bucket_directory.split("/", 1)[1]

    # join the local directory path to the new path to get the absolute path
    new_directory = os.path.join(local_directory, new_path)

    # if the directory doesn't exist, we create one
    os.makedirs(os.path.dirname(new_directory), exist_ok=True)

    # then open the file at the new directory in binary write mode and download the file from bucket to "file" (S3->file->new_dir)
    with open(new_directory, "wb") as file:
        client.download_fileobj(bucket_name, bucket_directory, file)

# This is the main method that was similar for backup in which we do some basic error handling and accept command line inputs as parameters
if len(sys.argv) != 4:
    print("Error: incorrect format")
    print("Correct Format: python3 restore.py restore bucket::<directory> <directory>")
    sys.exit(1)
if sys.argv[1] == "restore":
    restore(sys.argv[2], sys.argv[3])
else:
    print("Error: must enter restore")
    sys.exit(1)

import boto3
import sys
import os
import botocore

# Global variables that are used
s3 = boto3.resource("s3")
s3_client = boto3.client("s3")
s3_session = boto3.session.Session()


"""
This is the main method for backup, it calls all other helper methods to perform the backup 
It will take in two paths, the local path, and the bucket directory path
"""
def backup(local_dir_path, bucket_dir_path):
    
    # change both paths to have consistency amongst s3 and windows file paths
    local_dir_path = local_dir_path.replace('\\', '/')
    bucket_dir_path = bucket_dir_path.replace('\\', '/')

    # basic check to make sure that the local directory is valid
    if not os.path.isdir(local_dir_path):
        print("%s does not exist" % local_dir_path)
        return

    print("Backup starting...")

    # parse using split to get both bucket names and bucket directory
    split = bucket_dir_path.split("::")
    try:
        bucket_name = split[0]
        bucket_directory = split[1]
    except IndexError: 
        print("invalid format for bucket, please use dirA bucket::dirA format")
        return
    
    # ensure consistent path conventions
    if not local_dir_path.endswith("/"):
        local_dir_path += "/"
    if not bucket_directory.endswith("/"):
        bucket_directory += "/"
    if local_dir_path.startswith("/"):
        local_dir_path = local_dir_path[1:]

    # call helper method to check if the bucket exists
    if not does_bucket_exist(s3_client, bucket_name):
        try: # try to create the bucket and set the users region
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": s3_session.region_name})
            print(f"Creating bucket: {bucket_name} with region: {s3_session.region_name}")
        except botocore.exceptions.ClientError: 
            print("The name for the bucket already exists, please use a different name")
            return
    # recursively traverse through directories and files using os.walk    
    for root, directories, files in os.walk(local_dir_path):
        for file in files:

            # get the local file path
            file_path = os.path.join(root, file).replace("\\", "/")
            # get the s3 bucket path 
            bucket_path = os.path.join(bucket_directory, file_path[len(local_dir_path):].replace("\\", "/"))
            #create a boolean variable to check if we need a backup or not
            need_backup = False

            # another heloer method to check whether a file exists in the bucket already
            if does_file_existS3(bucket_name, s3_client, bucket_path):
                local_time = os.path.getmtime(file_path) # if it does exist, get the timestamps of the file in the bucket and the local file
                s3_time = s3_client.get_object(Bucket=bucket_name, Key=bucket_path)["LastModified"].timestamp() 
                if local_time > s3_time: # if the local file timestamp is greater, it means that that it was modified and so we set the boolean to true
                    need_backup = True
                else: # if the condition is false it means the file is up to date so we dont need to backup
                    need_backup = False
            else: # if the does file exist condition returns false it means we have encountered a new file so we need to backup
                need_backup = True
            
            if need_backup: # this is the code that does the actual backup
                try: 
                    upload(s3_client, file_path, bucket_name, bucket_path) # simple helper method to making things easy
                    print(f"Backed up {file_path} to s3://{bucket_name}/{bucket_path}") # output the file being backed up with both destinations
                except botocore.exceptions.ClientError:
                    print("Back up failed") 
            else: # if its false issue the appropriate message
                print(f"File unchanged, did not need to back up {file_path} to s3://{bucket_name}/{bucket_path}")
            

"""
the upload method takes in the client, local paths, and bucket paths 
and uploads a file properly
"""
def upload(client, local_path, bucket, bucket_directory):
    with open(local_path, "rb") as file: # open the local file in binary read mode 
        client.upload_fileobj(file, bucket, bucket_directory) # upload the file to the appropriate location


"""
The does bucket exist method simply just loops through the s3 clients total buckets
returns true or false based on whether the bucket exists or not
method is particularly used to check whether a bucket needs to be created or not 
"""
def does_bucket_exist(client, bucket_name):
    for bucket in client.list_buckets()["Buckets"]:
        if bucket["Name"] == bucket_name:
            return True
    return False 

"""
The does file exist method simply iterates through the objects in the given bucket and location to check
whether a file exists or not. If the file exists it will return true, else it will return false. 
"""
def does_file_existS3(bucket, client, key):
    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=key) 
        for obj in response.get('Contents', []):
            if obj['Key'] == key: 
                return True
    except Exception as e:
        
        return False
    
# This is essentially the "main" of the program Here is where I do some basic handling such as ensuring that the format is correct
if len(sys.argv) != 4:
    print("Error: incorrect format")
    print("Correct Format: python3 backup.py backup <directory> bucket::<directory>")
    sys.exit(1)
if "::" in sys.argv[3] and sys.argv[3].split("::")[0] == "":
    print("Error: bucket name is missing")
    print("Correct Format: python3 backup.py backup <directory> bucket::<directory>")
    sys.exit(1)
if sys.argv[1] == "backup":
    backup(sys.argv[2], sys.argv[3])
else:
    print("Error: must enter backup")
    sys.exit(1)




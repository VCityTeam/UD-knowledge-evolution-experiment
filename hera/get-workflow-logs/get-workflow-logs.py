import boto3
import os

def downloadDirectoryFromS3(bucketName, remoteDirectoryName, localDirName, endpoint, aws_access_key_id, aws_secret_access_key):
    s3_resource = boto3.resource("s3", endpoint_url=endpoint, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    bucket = s3_resource.Bucket(bucketName)

    for obj in bucket.objects.filter(Prefix=remoteDirectoryName):
        # remove the first part of the key (the prefix)
        step = obj.key[len(remoteDirectoryName):]
        formated_step = step.split("/")[0]
        # if step contains "querier", then keep it
        if "querier" in formated_step:
            if not os.path.exists(localDirName + "/" + remoteDirectoryName):
                os.makedirs(localDirName + "/" + remoteDirectoryName)
            bucket.download_file(obj.key, localDirName + "/" + remoteDirectoryName + "/" + formated_step + ".log")
        

def uploadFileToS3(bucketName, workflow_id, localFileName):
    s3_resource = boto3.resource("s3", endpoint_url=endpoint, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    bucket = s3_resource.Bucket(bucketName) 
    bucket.upload_file(localFileName, workflow_id + localFileName)

def merge_all_logs_files(workflow_id, datadir):
    # merge all logs files
    log_files = []
    for root, dirs, files in os.walk(datadir + workflow_id):
        for file in files:
            if file.endswith(".log"):
                log_files.append(os.path.join(root, file))

    with open(datadir + workflow_id + "merged_logs.log", "w") as outfile:
        for log_file in log_files:
            with open(log_file) as infile:
                outfile.write(infile.read())
                outfile.write("\n")

if __name__ == "__main__":
    endpoint   = "https://s3.pagoda.liris.cnrs.fr"
    bucketname = "argo-workflow-s3"
    datadir    = "./"
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    workflow_id = os.getenv("WORKFLOW_ID")

    # check if the environment variables are set
    if not all([aws_access_key_id, aws_secret_access_key, workflow_id]):
        print("Please set the environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and WORKFLOW_ID")
        exit(1)

    print("Downloading logs from S3...")
    print(f"Workflow ID: {workflow_id}")

    downloadDirectoryFromS3(bucketname, workflow_id, datadir, endpoint, aws_access_key_id, aws_secret_access_key)

    merge_all_logs_files(workflow_id, datadir)
from datetime import date, datetime
import os, sys
import json
import boto3
from botocore.exceptions import ClientError
prev_dir = 'C:\\Users\\Sudhanshu\\gitrepository\\Data-Engineering\\aws-etl-datapipeline-pyspark-youtube-data'
import json_operations

# config_data = json_operations.loadjsondata("configs/config.json")
# s3_jsondata_path = config_data["s3_data_path"]
s3_bucket_details = f"{prev_dir}\\bucket-info.json"
# s3_data = json_operations.loadjsondata(s3_jsondata_path)

region_name = "us-east-1"
env = "test"
current_datetime = datetime.now()
year        =current_datetime.year
month       =current_datetime.month
day         =current_datetime.day
hour        =current_datetime.hour
minute      =current_datetime.minute
second      =current_datetime.second
microsecond =current_datetime.microsecond
bucket_name =f"aws-etl-datapipeline-on-youtube-data-{env}-{year}{month}{day}{hour}{minute}{second}{microsecond}"

s3bucket = boto3.client("s3", region_name)

def create_bucket(bucketname, regionname):
    try:
        create_bucket_response = s3bucket.create_bucket(ACL = 'private',
                                                        Bucket =bucketname,
                                                        CreateBucketConfiguration = {
                                                            'LocationConstraint':regionname
                                                        })
        print(bucket_name)

    except ClientError as e:
        print(e)
    else:
        return create_bucket_response

if __name__ == "__main__":
    response = create_bucket(bucket_name, region_name)
    savejson_data_status = json_operations.savejsondata(s3_bucket_details, response)
    if savejson_data_status:
        print("Bucket Created")
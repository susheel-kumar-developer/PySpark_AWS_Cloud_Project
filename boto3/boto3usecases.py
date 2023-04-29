#access aws resources using python lang .. use boto3
import boto3

import boto3
boto3.session.Session(region_name="ap-south-1", botocore_session=None, profile_name=None)

client = boto3.client('s3')
#access s3 resources
#client.create_bucket(Bucket='susheel2025' )
s3 = boto3.client('s3')

response = client.delete_bucket(
    Bucket='susheel2023'
)

response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

#delete buckets
s3 = boto3.client('s3')
with open("C:\\bigdata\\drivers\\10000Records.csv", "rb") as f:
    s3.upload_fileobj(f, "susheeldb", "data/10000Records.csv")

import boto3
boto3.session.Session(region_name="ap-south-1", botocore_session=None, profile_name=None)
client = boto3.client('rds',region_name="ap-south-1")

client.create_db_instance(
    DBName='mysqldb',
    DBInstanceIdentifier='mysqlpoc',
    AllocatedStorage=20,
    DBInstanceClass='db.t3.micro',
    Engine='mysql',
    MasterUsername='myuser',
    MasterUserPassword='mypassword',
    DBSecurityGroups = [
    'sg-0bf2ca8a2349ea921',
    ],
    VpcSecurityGroupIds = [
    'vpc-09824a9d7a38b6032',
]


)
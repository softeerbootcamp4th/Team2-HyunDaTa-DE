import json
import boto3
from datetime import datetime, timezone, timedelta

# time
KST = timezone(timedelta(hours=9))
time_record = datetime.now(KST).strftime('%Y%m%d%H%M%S')
_today = time_record[2:]

with open("emr_env.json", "r") as f:
    emr_env = json.load(f)

# boto3 client
client = boto3.client(
    "emr",
    aws_access_key_id=emr_env['AWS_ACCESS_KEY'],
    aws_secret_access_key=emr_env['AWS_SECRET_ACCESS_KEY'],
    region_name=emr_env['AWS_REGION_NAME']
)


def create_emr_cluster():
    response = client.run_job_flow(
        Name=f"emr_cluster_{_today}",
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",  # Updated to a more modern instance type
                    "InstanceCount": 1,
                    "Configurations": [
                        {
                            "Classification": "spark",
                            "Properties": {"maximizeResourceAllocation": "false"},
                        },
                        {
                            "Classification": "spark-defaults",
                            "Properties": {"spark.dynamicAllocation.enabled": "false"},
                        },
                    ],
                },
                {
                    "Name": "Slave",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",  # Updated to a more modern instance type
                    "InstanceCount": 1,
                    "Configurations": [
                        {
                            "Classification": "spark",
                            "Properties": {"maximizeResourceAllocation": "false"},
                        },
                        {
                            "Classification": "spark-defaults",
                            "Properties": {"spark.dynamicAllocation.enabled": "false"},
                        },
                    ],
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
            "Ec2SubnetId": emr_env['EC2_SUBNET_ID']
        },
        Configurations=[
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.driver.extraClassPath": "/usr/lib/spark/jars/mysql-connector-java.jar:/usr/lib/hadoop-lzo/lib/hadoop-lzo.jar",
                    "spark.executor.extraClassPath": "/usr/lib/spark/jars/mysql-connector-java.jar:/usr/lib/hadoop-lzo/lib/hadoop-lzo.jar"
                }
            }
        ],
        BootstrapActions=[
            {
                'Name': 'Install MySQL JDBC Driver',
                'ScriptBootstrapAction': {
                    'Path': f's3://{emr_env["S3_BUCKET_NAME"]}/{emr_env["OUTPUT_PATH"]}/set_jar.sh'
                }
            }
        ],
        LogUri=f"s3://{emr_env['S3_BUCKET_NAME']}/{emr_env['OUTPUT_PATH']}/logs/",
        ReleaseLabel="emr-7.2.0",
        VisibleToAllUsers=True,
        JobFlowRole=emr_env['JOB_FLOW_ROLE'],
        ServiceRole=emr_env['SERVICE_ROLE'],
        Applications=[
            {"Name": "Spark"},
            {"Name": "Hadoop"},
        ]
    )
    cluster_id = response["JobFlowId"]
    print(f"Cluster created with ID: {cluster_id}")
    return cluster_id


def add_step_to_cluster(cluster_id):
    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "naver_cafe_genesis_누수",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"s3://{emr_env['S3_BUCKET_NAME']}/{emr_env['OUTPUT_PATH']}/sample_modeling_crawl.py",
                        "--bucket_name", emr_env['S3_BUCKET_NAME'],
                        "--base_path", emr_env['INPUT_PATH'],
                        "--car_name", "genesis",
                        "--issue", "누수"
                    ]
                }
            },
        ]
    )
    step_id = response['StepIds'][0]
    print(f"Step added with ID: {step_id}")
    return step_id


def lambda_handler(event, context):
    cluster_id = create_emr_cluster()
    add_step_to_cluster(cluster_id)

    return {
        "statusCode": 200,
        "body": "EMR Cluster Created"
    }

import json
import boto3
from datetime import datetime, timezone, timedelta


with open("emr_env.json", "r") as f:
    emr_env = json.load(f)

# boto3 client
client = boto3.client(
    "emr",
    aws_access_key_id=emr_env['AWS_ACCESS_KEY'],
    aws_secret_access_key=emr_env['AWS_SECRET_ACCESS_KEY'],
    region_name=emr_env['AWS_REGION_NAME']
)


def create_emr_cluster(end_datetime):
    response = client.run_job_flow(
        Name=f"emr_cluster_{end_datetime}",
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.2xlarge",  # Updated to a more modern instance type
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
                    "InstanceType": "m5.2xlarge",  # Updated to a more modern instance type
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
                    "Name": "Task",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.2xlarge",  # Updated to a more modern instance type
                    "InstanceCount": 2,
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
                }
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
            },
            {
                'Name': 'Install pip packages',
                'ScriptBootstrapAction': {
                    'Path': f's3://{emr_env["S3_BUCKET_NAME"]}/{emr_env["OUTPUT_PATH"]}/install_packages.sh'
                }
            }
        ],
        EbsRootVolumeSize=50,
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


def add_step_to_cluster(cluster_id, emr_run_date):
    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "Make View Table and Graph Modeling with Spark",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"s3://{emr_env['S3_BUCKET_NAME']}/{emr_env['OUTPUT_PATH']}/graph_modeling.py",
                        "--emr_run_date", emr_run_date
                    ]
                }
            },
        ]
    )
    step_id = response['StepIds'][0]
    print(f"Step added with ID: {step_id}")
    return step_id


def lambda_handler(event, context):
    time_ranges = event.get('time_ranges')
    end_datetime = max([tr['end_datetime']
                       for tr in time_ranges])

    cluster_id = create_emr_cluster(end_datetime)
    add_step_to_cluster(cluster_id, end_datetime)

    return {
        "statusCode": 200,
        "body": "EMR Cluster Created"
    }

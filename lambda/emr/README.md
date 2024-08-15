## EMR Cluster를 구축하고 Spark Submit 작업을 수행하는 AWS Lambda Function 작성

### create_emr_cluster

```
Spark 작업을 위한 EMR Cluster 구축에 필요한 Option들을 구축하는 부분

Instances
- Instance Group ⇒ 실행할 Master / Core / Task Node의 config를 지정하는 부분
- KeepJobFlowAliveWhenNoSteps ⇒ Step이 종료되었을 때 자동으로 종료하기 위한 부분

Configurations
- Properties에 추가하고자 하는 추가 config들을 추가해주는 부분

BootstrapActions
- EMR Cluster 생성 시에 실행되는 부분이며, Dockerfile에서 CMD와 비슷함.
- 실행시킬 파일이 s3에 적재되어있어야 함.

나머지 Option
- JobFlowRole ⇒ MapReduceRole
- ServiceRole ⇒ EMR_EC2_Instance_Role
- Applications ⇒ 실행시킬 Tools (Spark / Hadoop / …)
```


### add_step_to_cluster
    
```
실질적으로 실행하는 Job(Step)을 추가하는 부분

JobFlowId
- create_emr_cluster()를 실행하면 고유한 Job Number가 생성됨.

Steps
- 여기에 실행할 작업을 작성해주면 됨. Config 기반으로 Steps 자동화 가능해보임.
- ActionOnFailure: 작업 실패시 어떻게 할래? ⇒ Terminate 해야됨.
- 그 아래는 Spark Submit 코드와 동일하다.
```

### AWS 관련 정보
- 아래 정보가 담긴 **emr_env.json** 파일을 Lambda function과 동일한 dir에 저장해주세요.

```bash
{
    "S3_BUCKET_NAME": "bucket_name",
    "AWS_ACCESS_KEY": "access_key",
    "AWS_SECRET_ACCESS_KEY": "secret_access_key",
    "AWS_REGION_NAME": "region_name",
    "SERVICE_ROLE": "EMRservicerole",
    "JOB_FLOW_ROLE": "EMRjobflowrole",
    "EC2_SUBNET_ID": "subnet_Id",
    "INPUT_PATH": "input_data_path",
    "OUTPUT_PATH": "output_path"
}
```

### JDBC / Hadoop_lzo 설치
- RDS에 데이터를 업로드할 때, 위 두가지가 없어서 오랜시간 오류를 겪었음.
- BootstrapAction 옵션을 활용하면 필요한 사전작업을 수행할 수 있음.

```bash
#!/bin/bash

# MySQL JDBC 드라이버를 저장한 S3 경로
JDBC_JAR_URL="s3내 mysql-connector-java.jar 파일 경로"

# MySQL JDBC 드라이버를 다운로드하여 Spark의 JAR 디렉토리로 이동
sudo aws s3 cp $JDBC_JAR_URL /usr/lib/spark/jars/mysql-connector-java.jar

# 권한 설정 (필요할 경우)
sudo chmod 644 /usr/lib/spark/jars/mysql-connector-java.jar

# Hadoop LZO 압축 코덱 설치
sudo yum install -y hadoop-lzo
```

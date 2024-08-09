## SNS/News crawling and monitoring using Apache Airflow

### File Directory
```
📂root
┗ 📂config
    ┗ 📜aws.env
    ┗ 📜car_community_news.yaml
┗ 📂dags
    ┗ 📂crawl
    ┗ 📂monitor
        ┗ 📜car_community_news.py
┗ 📜docker-compose.yaml
┗ 📜Dockerfile
┗ 📜requirements.txt
```


### Usage
- docker-compose.yaml에 들어간 apache-airflow 이미지에 추가로 필요한 라이브러리 설치를 위해 Dockerfile을 작성했습니다.
- compose file 내 image tag를 주석처리하고, build를 활성화 시킨 후 추가로 작업할 부분들을 Dockerfile에 같이 추가해줍니다.
- 초기 airflow UI (8080 port) ID/PW는 airflow/airflow이며, 추후 수정할 예정입니다.

    ```bash
    docker compose up -d
    ```

### Notice
- S3, EMR, Redshift등 airflow를 사용해서 AWS에 올리는 코드를 작성할 때, 반드시 aws config가 없는지 확인해주세요. 
- 필요한 파일들은 DAG 폴더 내에서 작업을 진행합니다.
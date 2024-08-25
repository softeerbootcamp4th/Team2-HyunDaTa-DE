# EMR Cluster 구축 및 Spark Submit 작성

## EMR Cluster

### 개요
- AWS EMR은 Hadoop/Spark/Hive등 빅데이터 처리에 필요한 Tool을 별도의 설정없이 사용할 수 있는 기능임.
- EMR Cluster는 기본적으로 EC2 인스턴스의 자원을 기반으로 작동하며, 사용 시간에 따라 요금이 부과됨.
- 하나의 연속적인 파이프라인으로 실행되기 위해서 / EMR을 효과적으로 사용하기 위해서 AWS Lambda로 EMR Cluster를 구축하는 코드 작성
- 1시간마다 작동되는 프로젝트 주기에 맞춰, Step Function의 종료 신호를 받아와 자동으로 실행되어 하나의 연결된 파이프라인 구축


### create_emr_cluster
    
    Spark 작업을 위한 EMR Cluster 구축에 필요한 Option들을 구축하는 부분

    - Instances
    Instance Group ⇒ 실행할 Master / Core / Task Node의 config를 지정하는 부분
    KeepJobFlowAliveWhenNoSteps ⇒ Step이 종료되었을 때 자동으로 종료하기 위한 부분

    - Configurations
    Properties에 추가하고자 하는 추가 config들을 추가해주는 부분

    - BootstrapActions
    EMR Cluster 생성 시에 실행되는 부분이며, Dockerfile에서 CMD와 비슷함.
    실행시킬 파일이 s3에 적재되어있어야 함.

    JobFlowRole ⇒ MapReduceRole
    ServiceRole ⇒ EMR_EC2_Instance_Role
    Applications ⇒ 실행시킬 Tools (Spark / Hadoop / …)


- 기본적으로 Master/Core/Task*2 총 4개의 인스턴스를 하나의 클러스터로 활용하고있음.
- AWS S3에 실행시킬 Spark Submit을 저장해두고, Spark Submit에 필요한 설치파일/라이브러리는 BootstrapActions을 통해 해결.


### add_step_to_cluster
    
    실질적으로 실행하는 Job(Step)을 추가하는 부분
    
    - JobFlowId
    create_emr_cluster()를 실행하면 고유한 Job Number가 생성되며, aws-cli를 통해 작업 여부를 모니터링 하고싶다면, 고유 Job Number가 필요함.
    
    - Steps
    한 번의 EMR Cluster에서 수행할 작업을 정의하는 영역
    
    - ActionOnFailure
    작업 실패시 EMR Cluster의 상태를 지정하는 부분 (Terminate -> 실패시 종료)


## Spark Submit

### 파이프라인

<img src="https://github.com/user-attachments/assets/cb9b7db5-dcf2-4e29-a414-f8b42eca80c0">


### 모델링 
- 현재 데이터 수집 단계에서는 1시간마다 현재 시점으로부터 과거 2일치의 데이터를 크롤링하여 모니터링 진행.
    > 네이버 동호회 카페, 보배드림, 자동차 갤러리와 같은 온라인 커뮤니티에서 한 게시물의 조회수/좋아요 수가 평균적으로 2~3일 정도가 지나면 수렴하는 경향성을 보였으며, 조원들과 과거 2일치 데이터를 모니터링하기로 결정함.

- 2일의 모니터링 기간을 가진다고 하여도, 최근 1시간 내에 수집된 데이터가 과거에 수집된 데이터에 비하여 상대적으로 강력한 힘을 가지고 있다고 판단.
    > 앞서 조사한 조회수/좋아요 수 증가 추이가 로그 그래프 형상을 띄고있는 것을 확인했으며, 이에 따라 팀원끼리 산정한 조회수/좋아요 수 데이터에 최초 수집 시점으로부터 지난 N시간을 팀원끼리 산정한 방정식에 입력으로 넣어 값을 곱해줌으로써 보정 진행.

- Graph Score를 산정할 때 과거 데이터의 영향성이 무시되면 안된다고 판단.
    > 2021-01-01일에 '아이오닉 - 누수' 관련 게시물이 한꺼번에 많이 올라오고, 그 다음날에 게시물이 안올라왔다고 가정할 때, 조회수/좋아요 수를 기반으로 관심도 수치를 산정하게 될 경우 급격한 경사를 보이게 된다. 하지만, 수치 데이터와는 다르게 실제로 '아이오닉 - 누수'의 관심도가 줄어들었다고 보기 어렵다고 판단함. 따라서, 과거의 경향성을 반영하는 관심도 수치 산정 수식을 적용함.


- DTW(Dynamic Time Warping) 기반 그래프 유사도 측정
    > 최근 3개월동안 수집된 '자동차 - 이슈' 관심도 그래프가 과거에 존재했던 사례 중 가장 유사한 Case를 선별하기 위해 시계열 그래프의 유사도를 측정해주는 DTW 알고리즘 활용.
    
    > 가장 유사하다고 판단한 3개의 그래프를 보여주고, 모니터링하고자하는 '자동차 - 이슈' Case가 어떻게 변화할 수 있을지 같이 보여주는 데이터 프로덕트 개발 수행


### 최적화
- Spark Web UI 서버에서 제공해주는 DAG 시각화결과를 바탕으로 작동되는 작업의 흐름을 파악하고, 병목현상이 발생하는 영역 탐색
- 데이터를 salting / repartitioning / caching함으로써 EMR에서 처리되는 
- 27~30분가량 걸리던 작업이 대략 20분정도에 종료되는 효과를 얻음.
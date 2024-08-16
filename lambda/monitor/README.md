# AWS Lambda로 selenium 크롤링 자동화하기
AWS Lambda에서 selenium 크롤링을 위해 사용할 Docker 컨테이너 이미지를 위한 Dockerfile과 샘플 크롤링 스크립트입니다.

## 1. 크롤링하는 AWS Lambda를 실행시키는 Step Functions
`step_functions` 디렉터리입니다.

`step_functions.json`은 payload로 받은 시간 범위, 커뮤니티, 자동차에 대해 3중 루프를 만들어 비동기적으로 크롤링하는 AWS Lambda를 실행 시킵니다.

EventBridge에서 cron을 통해 Step Functions의 실행을 자동화했으며 그 입력으로 `payload.json`을 줬습니다.

24시간 범위의 크롤링을 수행합니다.

다만, 이를 한번에 수행할 경우 AWS Lambda의 최대 실행 시간을 초과할 가능성이 높아 6시간씩 나누어 AWS Lambda를 생성했습니다.

이 부분은 더 확실하게 개선이 필요할 것 같습니다.

cron 실행 식은 `0 * 16-29 8 ? 2024`입니다. 교육 기간이 끝날 때까지 자동으로 실행토록 했습니다.


## 2. 시간 범위를 구하는 AWS Lambda
`start_hours_ago` 시간 전에서부터 `duration_hours` 시간 후까지의 시간 범위를 계산합니다.

lambda_function.lambda_handler에 전달하는 event parameter의 구성 방식은 다음과 같습니다.
```json
{
    "start_hours_ago": <hours>,
    "duration_hours": <hours>
}
```

## 3. 크롤링하는 AWS Lambda
`labmda-crawler` 디렉터리입니다.

도커 컨테이너 이미지 위에서 크롤링을 수행합니다.

`/extract` 디렉터리에서 사용한 크롤링 코드를 이용했습니다.

lambda_function.lambda_handler에 전달하는 event parameter의 구성 방식은 다음과 같습니다.
```json
{
    "community":<community>,
    "car_name":<car_name>,
    "start_datetime":<yyyy-MM-dd hh:mm>,
    "end_datetime":<yyyy-MM-dd hh:mm>
}
```

### 3.1. AWS Lambda에서 selenium 사용하기
Python 3.12에서는 크롬 드라이버와 관련한 용량 문제 때문에 도커 컨테이너 이미지를 이용해야 합니다.
1. `./lambda-crawler/Dockerfile`를 빌드합니다.
```bash
export IMAGE_NAME=selenium-chrome-driver

cd lambda-crawler
docker build -t ${IMAGE_NAME} .
```
2. AWS ECR에 컨테이너 이미지를 푸시합니다.
```bash
export AWS_ACCOUNT_ID={your_aws_id}
export AWS_REGION={your_region}
export IMAGE_TAG=latest

docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}
```
3. AWS Lambda를 구성할 때 컨테이너 이미지 사용을 선택합니다.
4. 이후 절차는 일반 Lambda와 유사합니다.


### 3.2. 로컬에서 크롤링 하는 AWS Lambda 테스트 하기
두 가지 방법 모두 `./lambda-crawler/` 디렉터리에서 실행합니다.

`./lambda-crawler/event.json` 파일을 수정해서 다양한 상황에 대해 테스트할 수 있습니다.

#### Using SAM
```bash
docker build -t selenium-chrome-driver .
sam build
sam local invoke MonitorCommunity --event event.json
```

#### Using Docker Image
```bash
docker build -t selenium-chrome-driver .
docker run -p 9000:8080 selenium-chrome-driver
# 아래는 새 터미널에서 실행
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @event.json
```

### 3.3. AWS CLI로 AWS Lambda의 도커 컨테이너 이미지 업데이트 하기
```bash
export IMAGE_NAME=selenium-chrome-driver
export AWS_ACCOUNT_ID={your_aws_id}
export AWS_REGION={your_region}
export IMAGE_TAG=latest

aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

docker build -t ${IMAGE_NAME} .
docker tag ${IMAGE_NAME}:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}

aws lambda update-function-code \
    --function-name {aws_lambda_name} \
    --image-uri ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}
```

## References
- https://youtu.be/8XBkm9DD6Ic?si=MT9x_og_yUC2IFrn
- https://towardsdev.com/easily-use-selenium-with-aws-lambda-2cc49ca43b93
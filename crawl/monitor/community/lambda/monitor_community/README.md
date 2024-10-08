# 커뮤니티 게시글을 모니터링하는 AWS Lambda
AWS Lambda를 통해 커뮤니티에 올라오는 새로운 게시글을 모니터링 합니다.

도커 컨테이너 이미지 위에서 크롤링을 수행합니다.

[`pre-collect`](https://github.com/softeerbootcamp4th/Team2-HyunDaTa-DE/tree/main/crawl/pre-collect) 디렉터리에서 사용한 크롤링 코드를 이용했습니다.

Event parameter의 예시 구성 방식은 다음과 같습니다.
```json
{
    "community":"bobaedream",
    "car_name":"ioniq",
    "start_datetime":"2024-08-17 01:00",
    "end_datetime":"2024-08-17 06:59"
}
```

## 1. AWS Lambda에서 selenium과 Chrome driver 사용하기
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


## 2. 로컬에서 테스트 하기
`./lambda-crawler/event.json` 파일을 수정해서 다양한 상황에 대해 테스트할 수 있습니다.

### Using SAM
```bash
docker build -t selenium-chrome-driver .
sam build
sam local invoke MonitorCommunity --event event.json
```

### Using Docker Image
```bash
docker build -t selenium-chrome-driver .
docker run -p 9000:8080 selenium-chrome-driver
# 아래는 새 터미널에서 실행
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @event.json
```

## 3. AWS CLI로 AWS Lambda의 도커 컨테이너 이미지 업데이트 하기
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
# AWS Lambda로 뉴스 모니터링 자동화
Selenium 크롤링을 통해 KBS, MBC, SBS의 자동차에 대한 기사를 모니터링 하는 코드입니다.

## 로컬에서 테스트 하기
```bash
docker build -t news-monitoring-image .
docker run -p 9000:8080 selenium-chrome-driver
# 아래는 새 터미널에서 실행
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @event.json
```

## AWS ECR에 도커 컨테이너 이미지 푸시 및 AWS Lambda 업데이트 하기
```bash
export IMAGE_NAME=news-monitoring-image
export AWS_ACCOUNT_ID={your_aws_id}
export AWS_REGION={your_region}
export IMAGE_TAG=latest

aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

docker build -t ${IMAGE_NAME} .
docker tag ${IMAGE_NAME}:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}

aws lambda update-function-code \
    --function-name monitor-news \
    --image-uri ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}:${IMAGE_TAG}
```

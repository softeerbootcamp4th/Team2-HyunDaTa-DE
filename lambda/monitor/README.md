# AWS Lambda로 selenium 크롤링 자동화하기
AWS Lambda에서 selenium 크롤링을 위해 사용할 Docker 컨테이너 이미지를 위한 Dockerfile과 샘플 크롤링 스크립트입니다.

## 크롤링하는 AWS Lambda
`labmda-crawler` 디렉터리입니다.

도커 컨테이너 이미지 위에서 크롤링을 수행합니다.

`/extract` 디렉터리에서 사용한 크롤링 코드를 이용했습니다.

lambda_function.lambda_handler는 event parameter의 구성 방식은 다음과 같습니다.
```json
{
    "community":<community>,
    "car_name":<car_name>,
    "start_datetime":<yyyy-MM-dd hh:mm>,
    "end_datetime":<yyyy-MM-dd hh:mm>
}
```

## 다른 AWS Lambda를 트리거 하는 AWS Lambda
`lambda-trigger` 디렉터리입니다.

크롤링하는 AWS Lambda를 모든 자동차와 커뮤니티에 대해 비동기적으로 실행 시킵니다.

필요한 event parameter는 없습니다.

## AWS Lambda에서 selenium 사용하기
Python 3.12에서는 크롬 드라이버와 관련한 용량문제 때문에 도커 컨테이너 이미지를 이용해야 합니다.
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


## Test on Local
### 크롤링 AWS Lambda
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

### 트리거 AWS Lambda
방법을 찾아보는 중입니다.

## References
- https://youtu.be/8XBkm9DD6Ic?si=MT9x_og_yUC2IFrn
- https://towardsdev.com/easily-use-selenium-with-aws-lambda-2cc49ca43b93
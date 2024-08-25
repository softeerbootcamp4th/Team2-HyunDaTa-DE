# 자동차 관련 뉴스를 모니터링하는 AWS Lambda
AWS Lambda를 통해 KBS, MBC, SBS의 자동차에 대한 기사를 모니터링 하는 코드입니다.

도커 컨테이너 이미지 위에서 크롤링을 수행합니다.

Event parameter의 예시 구성 방식은 다음과 같습니다.
```json
{
    "news":"kbs",
    "car_name":"ioniq",
    "start_datetime":"2024-08-17 01:00",
    "end_datetime":"2024-08-17 06:59"
}
```

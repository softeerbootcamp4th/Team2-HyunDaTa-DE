# 모니터링 결과를 병합하는 AWS Lambda
Step Functions를 통해 시간을 분할하여 크롤링한 결과들을 하나의 파일로 병합합니다.

분할되어 S3에 저장됐던 파일은 동일 디렉터리에서 archive 디렉터리로 이동시킵니다.

Event parameter의 예시 구성 방식은 다음과 같습니다.
```json
{
    "time_ranges": [
        {"start_datetime": "2024-08-17 15:00", "end_datetime": "2024-08-17 20:59"},
        {"start_datetime": "2024-08-17 09:00", "end_datetime": "2024-08-17 14:59"},
        {"start_datetime": "2024-08-17 03:00", "end_datetime": "2024-08-17 08:59"},
        {"start_datetime": "2024-08-16 21:00", "end_datetime": "2024-08-17 02:59"}
    ],
    "communities": [
        "bobaedream",
        "dcinside",
        "naver_cafe"
    ],
    "car_names": [
        "avante", "palisade"
    ]
}
```
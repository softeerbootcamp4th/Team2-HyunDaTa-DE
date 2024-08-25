# 모니터링 결과를 RDS에 업데이트 하는 AWS Lambda

`merge_results` AWS Lambda가 병합한 크롤링 결과를 RDS에 업데이트 합니다.

`time_ranges` 파라미터에서 가장 이른 시간과 가장 늦은 시간을 추출하여 병합된 파일에 접근합니다.

Event parameter의 예시 구성 방식은 다음과 같습니다.
```json
{
    "time_ranges": [
        {"start_datetime": "2024-08-17 15:00", "end_datetime": "2024-08-17 20:59"},
        {"start_datetime": "2024-08-17 09:00", "end_datetime": "2024-08-17 14:59"},
        {"start_datetime": "2024-08-17 03:00", "end_datetime": "2024-08-17 08:59"},
        {"start_datetime": "2024-08-16 21:00", "end_datetime": "2024-08-17 02:59"}
    ],
    "newses": [
        "kbs",
        "mbc",
        "sbs"
    ],
    "car_names": [
        "avante", "palisade"
    ]
}
```
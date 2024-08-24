# Step Functions: 크롤링에서 RDS 업데이트까지
`step_functions.json`은 payload로 받은 뉴스, 자동차에 대해 2중 루프를 만들어 비동기적으로 뉴스를 크롤링하는 AWS Lambda를 실행 시킵니다.

EventBridge에서 cron을 통해 Step Functions의 실행을 자동화했으며 그 입력으로 `payload.json`을 줬습니다.

Step Functions가 한 번 실행될 때 이전 1시간의 뉴스를 크롤링 하여 S3에 데이터를 저장합니다.

이후 S3에 있는 크롤링 데이터를 모아 RDS를 업데이트 하는 AWS Lambda를 실행합니다.

cron 실행 식은 `0 * 20-29 8 ? 2024`입니다. 교육 기간이 끝날 때까지 자동으로 실행토록 했습니다.
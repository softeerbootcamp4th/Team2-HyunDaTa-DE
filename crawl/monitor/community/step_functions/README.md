# 커뮤니티 모니터링을 자동화하는 Step Functions: 크롤링에서 EMR 실행까지
`step_functions.json`은 payload로 받은 시간 범위, 커뮤니티, 자동차에 대해 3중 루프를 만들어 비동기적으로 크롤링하는 AWS Lambda를 실행 시킵니다.

EventBridge에서 cron을 통해 Step Functions의 실행을 자동화했으며 그 입력으로 `payload.json`을 줬습니다.

Step Functions가 한 번 실행될 때 이전 48시간의 게시글을 크롤링 합니다.

다만, 이를 한 AWS Lambda가 수행할 경우 AWS Lambda의 최대 실행 시간을 초과할 가능성이 높아 시간 범위를 6시간씩 나누어 AWS Lambda를 생성했습니다.

이 부분은 더 확실하게 개선이 필요할 것 같습니다.

cron 실행 식은 `0 * 16-29 8 ? 2024`입니다. 교육 기간이 끝날 때까지 자동으로 실행토록 했습니다.

## TODO
- 크롤링 결과를 RDS에 업데이트하는 Lambda 구현 및 Step Functions 업데이트
## Team2-HyunDaTa-DE
> 안녕하세요, 현대자동차 소프티어 부트캠프 4기 데이터엔지니어링 2조 **HyunDaTa** 입니다. 

## 💬 프로젝트 소개

현대자동차의 결함 이슈에 대해 네이버카페, 디씨인사이드, 보배드림 커뮤니티 반응과 뉴스를 주기적으로 모니터링하고, 관심도가 올라간 결함 이슈가 생기면 알림 서비스를 제공하는 프로젝트입니다 :)

## 시연 영상
- [Youtube Link](https://www.youtube.com/watch?v=bik-lCagBaU)

## 프로젝트 파이프라인
<img src="https://github.com/user-attachments/assets/0a4a1305-8f31-4ede-a799-7be178450fbe">


## 기능 

### Hot Issue

<img src="https://github.com/user-attachments/assets/3edb1120-ec77-4e48-957c-2ac52736586e">

- 자체 개발한 관심도 산정 모델을 통해 최근 3개월 동안 각 차량의 결함/이슈에 대한 값을 수치화 시키고, Threshold를 초과한 차량의 결함/이슈를 빠르게 확인할 수 있습니다.

- 현재 Hot Issue로 지정된 차량의 결함/이슈가 과거 사례 중 어떤 Case와 가장 유사한지 최대 3가지를 함께 보여주어 이후 3개월의 관심도 증감 추이의 변화 가능성을 함께 제공합니다. 

- 과거의 특정 결함 이슈에 대해 뉴스가 보도된 경우, 그 시점을 현재 날짜로 매핑하여 그래프에 수직선으로 표시하였습니다. 이 기능을 통해 과거 뉴스 보도 시점 전후의 데이터 변화를 직관적으로 파악할 수 있습니다. 사용자는 그래프에서 보고 싶은 차량-이슈만 선택적으로 On/Off 할 수 있어, 원하는 정보만 집중적으로 볼 수 있습니다.

### Hot Issue Alert System

<img src="https://github.com/user-attachments/assets/53b7ae34-047e-4b15-96d5-36fae816f4b1">

- 매 시간마다 실행되는 데이터 파이프라인이 특정 차량-결함 이슈를 Hot Issue로 감지하면, 이메일과 Slack을 통해 관련 담당자에게 자동으로 알림을 보내줍니다. 

- 알림에는 차량 결함 및 자체 개발한 서비스 내 핫이슈 탭으로 이동할 수 있는 URL을 함께 제공합니다. 


### News Alert System

<img src="https://github.com/user-attachments/assets/75f8bd89-29c6-41a7-adaf-1d5a78f2a666">

- 현대자동차의 차량 결함이나 이슈가 뉴스에 보도될 경우 자동 알림 기능을 제공합니다.

- GPT API를 활용하여 차종, 이슈, 헤드라인, 원문 정보를 요약한 결과와 함께 뉴스 URL을 제공합니다.


## 👨‍💻 팀원 소개

|                                          DE                                          |                                         DE                                          |                                          DE                                          |                                           
| :----------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------: | 
| <img src="https://github.com/user-attachments/assets/245c0722-8fc1-481e-843d-c21664949e18" width="400px" alt="김영일"/> | <img src="https://github.com/user-attachments/assets/615da6fe-1537-41dd-9721-f0ef0758af55" width="400px" alt="이민섭"/> | <img src="https://github.com/user-attachments/assets/bfab13fd-5fc4-4bd5-99e1-2fb684662fa4" width="400px" alt="한경훈"/>
|                    [김영일](https://github.com/patrashu)                     |                         [이민섭](https://github.com/Neogr1)                          |                         [한경훈](https://github.com/gyeongpunch) 


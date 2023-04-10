# MINT: Musical asset INTeligence, 음악 자산 분석 엔진
<br/>


뮤직카우 서비스 내의 음악저작권 자산을 분석하여, 자체 엔진을 만들었습니다.

## Hyperlink
### Engine Demonstration Video
[![Video Label](http://img.youtube.com/vi/3dWhwRV1Kvc/0.jpg)](https://youtu.be/3dWhwRV1Kvc)<br/><br/>
이미지를 클릭하면 해당 영상으로 이동합니다.<br/>
엔진 시연 영상<br/><br/>

### Web service hyperlink
해당 엔진을 기반으로, 정보를 제공하는 musicowlabs 웹서비스를 만들었습니다.<br/><br/>
[musicowlabs](https://musicowlabs.com)<br/>
[위의 링크가 안될경우](https://3.39.149.157/)<br/><br/>

### project MuTech article hyperlink
프로젝트 과정, 시행착오가 담긴 아티클입니다.<br/><br/>
[[MuTech] 1. Introduction: 데이터로 보는 음악 저작권 투자](https://jaealways.tistory.com/25)<br/>
[[MuTech] 2. SNS index: 음악 저작권료를 예측할 수 있을까?](https://jaealways.tistory.com/38)<br/>
[[MuTech] 3. time-series clustering: 서로 다른 음악저작권을 분류할 수 있을까?](https://jaealways.tistory.com/42)<br/>
[[MuTech] 4. PER, BETA: 음악 저작권 시장에서 금융지표가 작동할까?](https://jaealways.tistory.com/77)<br/>
[[MuTech] 5. 공포탐욕지수로 보는 음악저작권](https://jaealways.tistory.com/88)<br/>
[[MuTech] 6. 금융 텍스트 활용하기](https://jaealways.tistory.com/92)<br/>
[[MuTech] 7. Topic Modeling: 뉴스로 보는 음악저작권 투자](https://jaealways.tistory.com/96)<br/>
[[MuTech] 8. MINT: 음악저작권 분석 엔진](https://jaealways.tistory.com/103)<br/>
[[MuTech] 9. PLM을 활용한 토픽 모델링](https://jaealways.tistory.com/115)<br/>
[[MuTech] 10. 프로젝트 MuTech 일지](https://jaealways.tistory.com/116)<br/>
<br/><br/>

## Architecture
![Figure1](https://user-images.githubusercontent.com/71856506/230923294-13c1ab49-fdf0-434d-acdb-63542c2b3fd0.png)<br/>
MINT 엔진 시간별 동작 아키텍처

### Architecture by directory
1) technical_analysis: 보통 EDA나 모델을 테스트한 스크립트를 모았습니다. 유의미한 결과를 보이면 엔진의 모델링 부분에 추가합니다.<br/>
2) crawlers, data_crawling: 데이터 크롤링과 관련된 스크립트가 있습니다.<br/>
3) data_modeling: 자연어처리나 금융지표 계산 같은 모델링 스크립트가 있습니다.<br/>
4) data_preprocessing: raw 데이터를 전처리하는 스크립트가 있습니다.<br/>
5) data_transformation: mongodb에 적재된 raw data를 data_preprocessing에 있는 로직을 이용해 mysql에 적재시킵니다.<br/>

## Contributors




## Contributors


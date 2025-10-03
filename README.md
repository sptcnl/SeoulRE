# SeoulRE
## Airflow 설정
### 1. API host 설정
관리자 > 커넥션들 > 커넥션 추가 <br>
| 필드명 | 값 |
|---|---|
| 커넥션 ID | seoul_openapi |
| 커넥션 유형 | HTTP |
| 호스트 | openapi.seoul.go.kr |
| 포트 | 8088 |
| Extra | {"api_key":"your_api_key"}|
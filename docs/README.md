# 문서 색인

루트의 [`README.md`](../README.md) 는 "이 저장소를 어떻게 돌리는가" 를 단계(1~7) 순서로 적은
실행 기록이다. 이 폴더는 그와 별개로 **"데이터가 어떻게 흐르고 어디에 쌓이고 어떻게 쓰이는가"**,
**"규모가 커지면 무엇이 달라지는가"**, **"무엇을 공부해야 하는가"** 를 주제별로 정리한 것이다.

| 문서 | 무엇에 답하나 | 이럴 때 본다 |
|---|---|---|
| [01-event-flow.md](01-event-flow.md) | 이벤트 한 건이 태어나서 정산될 때까지 거치는 모든 홉 | "이 이벤트 지금 어디 있지?" |
| [02-data-stores.md](02-data-stores.md) | 원본이 어디에 어떤 모양으로 쌓이고 누가 읽는가 (토픽/Parquet/Redis/Postgres 스키마 전부) | "원본 데이터를 직접 까 보고 싶다" |
| [03-ad-decision-api.md](03-ad-decision-api.md) | 광고 결정 API 규격 — 요청/응답 필드, 시퀀스, Outbox, 운영 확장 | "광고는 누가 어떻게 정하나?" |
| [04-scale-100m.md](04-scale-100m.md) | 1억 건 규모에서 무엇이 먼저 깨지고 무엇을 바꿔야 하나 | "이 구조 그대로 쓸 수 있나?" |
| [05-kafka-guide.md](05-kafka-guide.md) | Kafka 구조·개념·설계 판단 + 이 저장소 대응표 + 학습 자료 | "Kafka 를 제대로 배우고 싶다" |
| [06-learning-path.md](06-learning-path.md) | 4주 학습 로드맵, 실습 과제, 면접 예상질문 | "어디서부터 공부하지?" |
| [07-flink-guide.md](07-flink-guide.md) | Flink 의 역할과 **Flink UI(:8181) 읽는 법** — 그래프·워터마크·백프레셔·체크포인트 (캡처 포함) | "Flink 화면에서 뭘 봐야 하지?" |
| [08-minio-guide.md](08-minio-guide.md) | MinIO 의 역할과 **콘솔(:9001)·`mc` 로 원본 보는 법**, Flink 가 파일을 쓰는 방식 (캡처 포함) | "원본 파일이 어떻게 생기지?" |
| [09-dashboard-guide.md](09-dashboard-guide.md) | **관찰 대시보드(:8088) 읽는 법** — 패널별 칸의 뜻·색 기준·정상 범위, 대사 원인 분해 읽기 (캡처 포함) | "대시보드 숫자가 무슨 뜻이지?" |
| [10-aws-migration.md](10-aws-migration.md) | **AWS 이전 설계** — 컴포넌트별 AWS 서비스, 목표 구조, 코드에서 바꿀 자리, 옮기는 순서와 대사로 검증하는 병행 운영, 비용·함정 (방법만 서술) | "클라우드로 올리면 뭘 바꿔야 하지?" |
| [architecture-presentation.html](architecture-presentation.html) | 설계 결정 6가지와 트레이드오프 (발표용 슬라이드) | 남에게 설명할 때 |

## 화면 세 개의 역할이 다르다

| 화면 | 주소 | 역할 |
|---|---|---|
| **플레이어** | http://localhost:3001 | 사람이 콘텐츠를 보고 **광고를 실제로 본다**. 이벤트가 태어나는 곳. 어떤 광고가 나왔는지(광고주/캠페인/소재/CPM)를 화면에서 보여 주고, 그 광고 1편이 뒤에서 어디까지 갔는지 바로 조회한다.<br/>주소: `#/` 홈 · `#/watch/<content_id>` 재생 · `#/track/<ad_request_id>` 광고 1편 추적 |
| **관찰 대시보드** | http://localhost:8088 | 전체가 초당 얼마나 흐르는지. 수집·버퍼·처리·정합성·대사를 한 화면에. |
| **이벤트 추적기** | http://localhost:3000 | 이벤트 1건을 골라 넣는 실험대. 중복/SSAI/지연/스키마 위반을 하나씩 비교. |

세 화면은 같은 파이프라인을 본다. 플레이어에서 광고를 한 편 보면
대시보드의 impression 카운터가 올라가고, 추적기의 다섯 단계와 같은 경로를 탄다.

## 빠른 경로

```bash
make up                      # 전체 기동 (플레이어 포함)
bash scripts/flink-submit.sh # 실시간 처리 잡
bash scripts/flink-archive.sh# 원본 적재 잡
# 브라우저에서 http://localhost:3001 -> 콘텐츠 선택 -> 광고 재생 -> [데이터 확인]
bash scripts/flush-windows.sh && bash scripts/batch.sh   # 확정 집계까지 보고 싶을 때
```

## 캡처 다시 찍기

`img/` 의 스크린샷 30장은 [`scripts/capture/`](../scripts/capture/README.md) 로 다시 찍을 수 있다.
상황(부하 중 / 배치 후 / 사고 중)을 먼저 만들어 두고 찍어야 한다 — 순서는 그 README 에 있다.

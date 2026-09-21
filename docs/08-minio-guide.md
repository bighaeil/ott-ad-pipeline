# 08. MinIO — 원본 Parquet 이 쌓이는 곳, 어떻게 보나

MinIO 는 이 파이프라인의 **원본 보관소**다. Flink archive 잡이 Kafka 5개 토픽을 가공 없이
Parquet 으로 떨어뜨리고, Spark 배치가 그것을 읽어 정산한다. 이 문서는 콘솔(:9001) 화면과 CLI 로
그 원본을 직접 확인하는 법, 그리고 **파일이 실제로 어떻게 만들어지는지**를 적는다.

- 콘솔: http://localhost:9001 — 로그인 `minioadmin` / `minioadmin`
- S3 API: http://localhost:9000 — Flink / Spark / pyarrow 가 쓰는 쪽 (브라우저로 열 곳이 아니다)
- 스키마와 원본 활용법은 [02-data-stores.md](02-data-stores.md#4-minio-parquet-원본)

---

## 1. MinIO 는 무엇이고 왜 여기 있나

**AWS S3 와 같은 API 를 쓰는 오브젝트 스토리지**다. 로컬에서 S3 대신 쓴다.
코드는 `s3a://events/...` 로 쓰므로 운영에서 진짜 S3 로 바꿀 때 **주소와 키만 바꾸면 된다.**

| 누가 | 무엇을 | 경로 |
|---|---|---|
| Flink archive 잡 | 쓴다 | `s3a://events/dt=YYYY-MM-DD/hour=HH/part-*` |
| Spark 배치 | 읽는다 | `s3a://events/` 전체 |
| 플레이어 / 추적기 | 읽는다 (4단계 확인) | 해당 `dt/hour` 만 |

### 먼저 알아 둘 것 — 폴더는 없다

오브젝트 스토리지에는 **폴더가 없다.** `dt=2026-09-18/hour=07/part-…` 전체가 하나의 **키(이름)** 이고,
콘솔이 `/` 를 기준으로 잘라 폴더처럼 보여 줄 뿐이다. 그래서:

- 빈 폴더는 존재할 수 없다 (안에 파일이 없으면 폴더도 안 보인다)
- 콘솔에서 "폴더" 의 크기·수정시각은 의미가 없다 (`0B`, 목록을 만든 시각)
- `dt=`, `hour=` 형태는 **Hive 파티션 규칙**이다. Spark / pyarrow 가 이 이름을 보고 `dt`, `hour` 컬럼을 자동으로 만든다

### 호스트의 `data/minio/` 를 직접 열면 안 된다

```
data/minio/events/dt=2026-09-18/hour=09/part-15b29b3f-…-0-40/xl.meta
```

`part-…` 가 파일이 아니라 **디렉터리**이고 안에 `xl.meta`(작은 객체는 데이터가 이 안에 들어 있다)
또는 `part.1` 이 있다. MinIO 내부 저장 형식이라 Parquet 로 열리지 않는다.
**원본은 반드시 MinIO(콘솔 · `mc` · S3 API)를 통해서 꺼낸다.**

---

## 2. 콘솔 화면 순서대로

### 2-1. Object Browser — 버킷 목록

![버킷 목록](img/minio-01-buckets.png)

버킷은 `events` 하나다 (`minio-init` 컨테이너가 기동 시 만든다).
여기의 **Objects / Size 는 백그라운드 스캐너가 주기적으로 센 값이라 조금 늦다.**
정확한 값이 필요하면 CLI 의 `mc du` 를 쓴다 (5절).

### 2-2. 버킷 → 날짜 → 시간

![dt 목록](img/minio-03-dt.png)

`events` → `dt=2026-09-18` → `hour=07` 순으로 들어간다.
**`dt`, `hour` 는 `event_time`(이벤트 발생 시각) 기준 UTC** 다. 서버 도착 시각이 아니다.
그래서 지연 이벤트(생성기 30~60초, 플레이어 90초)가 정각 직후에 도착하면 이전 시간 폴더에 들어갈 수 있다.

### 2-3. 시간 폴더 안 — 파일 목록

![hour 폴더 파일 목록](img/minio-04-hour-files.png)

**시각이 두 가지로 보이는 데 주의한다.**
폴더 이름 `hour=07` 은 UTC 인데, `Last Modified` 는 브라우저 시간대(KST)로 `16:05` 처럼 보인다.
9시간 차이가 나는 게 정상이다.

파일 이름 읽는 법:

```
part-15b29b3f-ef4a-4ce4-b341-df775f1701a3-0-12
     └──────────── writer UUID ────────────┘ │ └ 순번 (이 writer 가 만든 12번째 파일)
                                             └ 서브태스크 번호 (병렬도 1 이라 항상 0)
```

- **writer 가 토픽마다 하나**라서 UUID 가 5종류다 (impression / quartile / click / request / behavior).
  파일 하나에는 한 토픽의 이벤트만 들어 있고, 어느 토픽인지는 `source_topic` 컬럼에 있다.
- **확장자가 없다.** Flink filesystem 커넥터의 기본 이름 규칙이다. 내용은 Parquet 이다(첫 4바이트 `PAR1`).
- `_SUCCESS` — 파티션 커밋 표시. 의미는 4절에서 설명한다.

### 2-4. 파일을 누르면 — 객체 상세

![객체 상세](img/minio-05-object-detail.png)

| 버튼 | 쓸모 |
|---|---|
| **Download** | 로컬로 받아 DuckDB · pandas 등으로 연다 (가장 실용적) |
| Preview | 텍스트/이미지용이라 **Parquet 은 내용이 안 보인다** |
| Inspect | 객체 메타데이터 |
| Share | 기간 제한 다운로드 URL(presigned URL) 생성 |
| Delete | **누르지 말 것.** 원본이 사라지고 배치 결과가 바뀐다 |

### 2-5. 버킷 설정 — Buckets → events

![버킷 요약](img/minio-06-bucket-summary.png)

로컬에서는 전부 기본값(Private · 버전관리 없음 · 수명주기 없음)이다. 운영이라면 여기서 정할 것들:

| 항목 | 운영 권장 | 이유 |
|---|---|---|
| **Lifecycle** | 90일 뒤 저비용 계층, N년 뒤 삭제 | 원본은 계속 쌓인다 ([02 문서 7절](02-data-stores.md#7-보존정리-정책)) |
| Versioning | 켜기 검토 | 실수로 지운 원본 복구 |
| Quota | 버킷 용량 상한 | 폭주 방지 |
| Encryption | 켜기 | `user_id` 같은 개인정보가 들어 있다 |

### 2-6. Monitoring → Metrics

![Metrics](img/minio-07-metrics.png)

서버·드라이브 상태와 전체 객체 수. 로컬은 서버 1 · 드라이브 1 이다.

### 2-7. Monitoring → Trace — **Flink 가 실제로 뭘 하는지 보이는 화면**

![Trace](img/minio-08-trace.png)

[Start Trace] 를 누르면 MinIO 로 들어오는 S3 요청이 실시간으로 찍힌다.
archive 잡이 돌고 있으면 **10초마다** 같은 묶음이 반복된다. 다음 절에서 이 요청의 의미를 설명한다.

---

## 3. 콘솔에서 무엇을 확인하면 되나

| 궁금한 것 | 어디서 |
|---|---|
| 원본이 쌓이고 있나 | `events` → 오늘 `dt` → 최신 `hour` 에 방금 시각의 파일이 있나 |
| 파일이 너무 잘게 쪼개지지 않았나 | 파일 크기 — 지금은 수 KB (4절) |
| Flink 가 쓰고 있나 | **Trace** 에 `CompleteMultipartUpload` 가 10초마다 찍히나 |
| 특정 광고 1편이 들어갔나 | 콘솔로는 못 본다(Parquet 내용 검색 불가) → 플레이어의 [데이터 확인] 또는 5절 명령 |

---

## 4. Flink 가 MinIO 에 쓰는 방식

Trace 로 25초 동안 잡은 요청을 종류별로 세면 이렇다 (부하 중, 실측):

```
15  s3.NewMultipartUpload     events/dt=2026-09-18/hour=09/part-…?uploads
15  s3.PutObjectPart          events/dt=2026-09-18/hour=09/part-…?uploadId=…&partNumber=1
15  s3.CompleteMultipartUpload events/dt=2026-09-18/hour=09/part-…?uploadId=…
 8  s3.PutObject              events/dt=2026-09-18/hour=09/_SUCCESS
23  s3.DeleteMultipleObjects  events/?delete
```

`DeleteMultipleObjects` 는 Hadoop s3a 클라이언트가 파일을 쓴 뒤 상위 "폴더 표시" 객체를 정리하는 요청이라 무시해도 된다.
나머지를 시간 순서로 풀면:

```
이벤트가 들어온다
   │  writer 가 파일을 "쓰는 중"  ──  NewMultipartUpload + PutObjectPart
   │  이 상태의 파일은 목록에 안 보인다 (아직 객체가 아니라 "진행 중 업로드")
   ▼
체크포인트 완료 (10초마다)
   │  CompleteMultipartUpload  →  이 순간 part-… 가 목록에 나타난다
   │  PartitionCommitter 가 _SUCCESS 를 PutObject
   ▼
다음 파일 시작
```

여기서 나오는 성질 네 가지:

**① 파일은 체크포인트마다(10초) 하나씩 닫힌다.**
`archive.sql` 에는 `rollover-interval = '1 min'` 이 있지만, **Parquet 같은 bulk 포맷은
체크포인트마다 무조건 파일을 닫는다.** 그래서 실제 간격은 `min(롤링 간격, 체크포인트 간격)` = 10초다.
실측: 5개 writer 모두 `09:20:48, 09:20:58, 09:21:08 …` 정확히 10초 간격으로 파일이 생겼다.
→ **이벤트가 Parquet 로 보이기까지 최대 약 10초.**

**② 그래서 파일이 작다 (small files).**
`hour=09` 실측: 파일 136개, 파일당 행 수 중앙값 77 · 최소 1, 크기 중앙값 12 KB.
**행이 1개뿐인 파일도 5.2 KB** 다 — 26개 컬럼의 스키마·통계를 담은 Parquet 푸터가 고정비로 붙기 때문이다.
1억 건 규모에서는 이것부터 문제가 된다 → [04-scale-100m.md](04-scale-100m.md#3-5-저장-계층--small-files-와-컴팩션).

**③ 체크포인트가 실패하면 파일이 안 보인다.**
진행 중 업로드는 Complete 되기 전까지 객체가 아니다. 이것이 **exactly-once 파일 커밋**이다 —
장애가 나면 반쯤 쓴 파일이 남지 않고, 복구 후 마지막 체크포인트부터 다시 쓴다.
`mc stat` 으로 보면 ETag 끝이 `-1` 인데, 멀티파트 업로드(파트 1개)로 만들어진 객체라는 표시다.

**④ 이 설정에서 `_SUCCESS` 는 "그 시간이 끝났다" 는 뜻이 아니다.**
`sink.partition-commit.trigger = 'process-time'`, `delay = '0 s'` 라서 **커밋할 때마다 덮어쓴다**
(위 trace 에서 25초 동안 8번). "이 파티션에 확정된 파일이 있다" 정도의 의미다.
운영에서 "이 시간 데이터는 다 들어왔으니 배치를 돌려도 된다" 는 신호로 쓰려면
`trigger = 'partition-time'` + 워터마크 기준 `delay = '1 h'` 로 바꿔야 한다.

---

## 5. CLI 로 보기 (`mc`)

MinIO 컨테이너 안에 `mc` 가 들어 있다. 매번 주소를 주기 번거로우니 별칭 변수를 쓴다.

```bash
MC='docker compose exec -T -e MC_HOST_local=http://minioadmin:minioadmin@localhost:9000 minio mc'

$MC ls local/events/                                   # 날짜 목록
$MC ls local/events/dt=2026-09-18/hour=09/ | tail      # 최신 파일
$MC du --depth 3 local/events/                         # 시간별 파일 수·크기 (정확한 값)
$MC find local/events --name _SUCCESS                  # 커밋된 파티션
$MC stat local/events/dt=2026-09-18/hour=09/<파일명>   # 크기·ETag·시각
$MC admin trace local                                  # 실시간 S3 요청 (Ctrl+C 로 종료)
```

실측 `mc du` 결과 예:

```
1.0MiB  190 objects  events/dt=2026-09-18/hour=07
299KiB   55 objects  events/dt=2026-09-18/hour=08
144KiB   27 objects  events/dt=2026-09-18/hour=09
```

### Parquet 내용까지 보기

```bash
# (a) 파일 하나를 호스트로 꺼내기 → DuckDB / pandas 등으로 연다
$MC cat local/events/dt=2026-09-18/hour=09/<파일명> > sample.parquet

# (b) 꺼내지 않고 바로 — pyarrow 가 들어 있는 player 컨테이너에서
docker compose exec player python -c "
import pyarrow.parquet as pq
from pyarrow import fs as pafs
s3 = pafs.S3FileSystem(endpoint_override='minio:9000', access_key='minioadmin',
                       secret_key='minioadmin', scheme='http', region='us-east-1')
f = pq.ParquetFile(s3.open_input_file('events/dt=2026-09-18/hour=09/<파일명>'))
print(f.metadata.num_rows, 'rows,', f.metadata.num_row_groups, 'row group')
print(f.schema_arrow)
print(f.read().slice(0, 5).to_pylist())
"
```

시간대 전체를 조건으로 읽거나 특정 `ad_request_id` 를 찾는 예는
[02-data-stores.md 4-4절](02-data-stores.md#4-minio-parquet-원본)에 있다.

---

## 6. 상황별로 어디를 보나

| 증상 | 볼 곳 | 흔한 원인 |
|---|---|---|
| `events` 버킷이 비어 있다 | Flink Overview 의 Running Jobs | archive 잡을 안 올렸다 → `bash scripts/flink-archive.sh` |
| 최신 파일이 안 생긴다 | Flink archive 잡 → **Checkpoints** | 체크포인트 실패 → 파일이 Complete 되지 않음 |
| 파일은 있는데 배치가 0건 | 파일의 `dt` / `hour` | `event_time` 기준 UTC 라 예상한 폴더와 다를 수 있다 |
| 콘솔 숫자와 `mc du` 가 다르다 | — | 콘솔 수치는 스캐너 기준이라 늦다. `mc du` 가 정확 |
| 파일 시각이 9시간 어긋나 보인다 | — | 폴더는 UTC, 콘솔 시각은 KST |
| 특정 광고가 들어갔는지 | 플레이어 [데이터 확인] 4단계 | 콘솔은 내용 검색 불가 |

---

## 7. 5분 실습

1. 콘솔 → Monitoring → **Trace** → [Start Trace]. 10초마다 `CompleteMultipartUpload` 묶음이 찍히는 것을 본다.
2. 플레이어(:3001)에서 광고 1편을 본다 → 10초 안에 `events/dt=오늘/hour=지금(UTC)` 에 새 파일이 생긴다.
3. `docker compose kill taskmanager` 로 Flink 워커를 죽인다 → Trace 가 조용해지고 새 파일이 안 생긴다.
   `docker compose start taskmanager` → 잡이 체크포인트에서 복구되고 다시 10초마다 파일이 생긴다
   (`scripts/scenario_flink_kill.sh` 가 같은 일을 자동으로 하고 구멍이 없는지까지 검사한다).
   멈춰 있던 동안의 이벤트도 **빠짐없이** 들어온다 (Kafka 오프셋이 체크포인트에 있었으므로).
4. `mc du --depth 3` 로 시간대별 파일 수를 보고, "파일당 평균 크기" 를 계산해 본다 → small files 를 숫자로 확인.

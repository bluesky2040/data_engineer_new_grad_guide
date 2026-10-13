# 데이터 엔지니어 신입 취업 준비 가이드 (기술 보완판)

> **버전 안내**
> 도구와 서비스의 기능, 기본값, 가격, 지원 범위는 버전에 따라 바뀝니다. 이 문서의 설정값과 코드는 개념을 설명하기 위한 예시이므로, 실제 적용 전에 사용하는 버전의 공식 문서로 확인하세요. 코드 예시는 입문자가 흐름을 이해할 수 있도록 단순화했습니다.

## 목차

1. [이 글을 어떻게 읽어야 할까?](#1-이-글을-어떻게-읽어야-할까)
2. [신입에게 우선 필요한 핵심 역량](#2-신입에게-우선-필요한-핵심-역량)
3. [데이터 파이프라인의 기본 구조와 설계 원칙](#3-데이터-파이프라인의-기본-구조와-설계-원칙)
4. [배치와 스트리밍](#4-배치와-스트리밍)
5. [워크플로 오케스트레이션: Airflow 등](#5-워크플로-오케스트레이션-airflow-등)
6. [대용량 데이터 처리: Spark, PySpark, Polars](#6-대용량-데이터-처리-spark-pyspark-polars)
7. [데이터 저장 구조와 아키텍처](#7-데이터-저장-구조와-아키텍처)
8. [데이터 포맷과 오픈 테이블 포맷](#8-데이터-포맷과-오픈-테이블-포맷)
9. [데이터베이스 확장과 NoSQL](#9-데이터베이스-확장과-nosql)
10. [데이터 품질, 계보, 거버넌스, 관측성](#10-데이터-품질-계보-거버넌스-관측성)
11. [Data Contract, Schema Evolution, Data Versioning, Feature Store](#11-data-contract-schema-evolution-data-versioning-feature-store)
12. [클라우드 데이터 서비스](#12-클라우드-데이터-서비스)
13. [인프라, 배포, 자동화](#13-인프라-배포-자동화)
14. [포트폴리오 프로젝트 예시](#14-포트폴리오-프로젝트-예시)
15. [8주 학습 계획 예시](#15-8주-학습-계획-예시)
16. [신입 면접에서 설명할 수 있어야 할 질문](#16-신입-면접에서-설명할-수-있어야-할-질문)
17. [우선순위 정리](#17-우선순위-정리)
18. [결론](#18-결론)

---

## 1. 이 글을 어떻게 읽어야 할까?

데이터 엔지니어링은 Python과 SQL로 데이터를 처리하는 것에서 출발하지만, 실무에서는 데이터 수집·변환·적재, 워크플로 관리, 저장 구조, 품질 검증, 장애 복구, 클라우드 운영까지 연결됩니다. 그래서 학습 범위가 넓어 보입니다.

다만 신입 취업 준비생이 아래에 나오는 모든 도구를 숙련 수준으로 다뤄야 한다는 뜻은 아닙니다. 이를 실제 학습 계획으로 바꿀 때는 다음 순서가 현실적입니다.

```text
기초 역량  →  완성도 있는 프로젝트  →  핵심 기술 확장  →  선택 분야 심화
(Python/SQL/Git)  (end-to-end 파이프라인)  (Docker, 클라우드, Spark, dbt)  (스트리밍, 레이크하우스 등)
```

신입 채용에서 중요한 것은 기술 이름을 많이 아는지보다 다음을 할 수 있는지입니다.

- 데이터를 직접 수집하고 처리하는 파이프라인을 만들 수 있는가
- 실패 상황(네트워크 오류, 중복, 누락, 스키마 변경)을 설명하고 복구할 수 있는가
- 결과가 올바른지 검증하는 규칙을 갖고 있는가
- 설계 선택의 이유와 한계를 말할 수 있는가

### 이 문서의 표기

| 표기 | 의미 |
|---|---|
| **[필수]** | 신입 지원 전에 갖추는 것이 좋은 기반 |
| **[권장]** | 기본 프로젝트 이후 확장하면 경쟁력이 되는 기술 |
| **[선택]** | 진로·프로젝트에 따라 선택하는 심화 주제 |

---

## 2. 신입에게 우선 필요한 핵심 역량

### 2.1 Python [필수]

Python은 API 호출, 파일 처리, 데이터 변환, 자동화, 테스트에 널리 사용됩니다.

**우선 익힐 내용**

- 함수, 모듈, 클래스의 기본 구조
- 예외 처리와 `logging` 모듈 (`print` 대신 로그 레벨 사용)
- 파일 입출력과 JSON/CSV 처리, 인코딩(UTF-8) 문제
- HTTP 클라이언트(`requests`, `httpx`)와 API 페이지네이션
- 가상환경(`venv`, `uv`, `poetry` 등)과 의존성 고정
- 타입 힌트와 데이터 모델링(`dataclass`, `pydantic`)
- `pytest`를 이용한 단위 테스트
- Git을 이용한 버전 관리
- 제너레이터와 청크 처리로 메모리 사용량 제어

#### 예시: 재시도·타임아웃이 있는 API 수집기

네트워크 호출에는 **타임아웃**과 **재시도(지수 백오프)** 가 기본으로 들어가야 합니다. 타임아웃이 없으면 응답이 오지 않을 때 작업이 무한정 멈출 수 있습니다.

```python
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1,                      # 1s, 2s, 4s ... 간격으로 재시도
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],               # 멱등한 메서드만 자동 재시도
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(session, url, params, page_size=100):
    """페이지 번호 방식 예시. 제너레이터로 한 건씩 넘겨 메모리를 아낀다."""
    page = 1
    while True:
        resp = session.get(
            url,
            params={**params, "page": page, "per_page": page_size},
            timeout=(3.05, 30),                # (연결, 읽기) 타임아웃
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            break
        logger.info("fetched page=%d count=%d", page, len(items))
        yield from items
        page += 1
```

**API 페이지네이션 방식**

| 방식 | 특징 | 주의점 |
|---|---|---|
| Offset/Page | 구현이 단순 | 수집 중 데이터가 추가·삭제되면 누락·중복 가능, 깊은 페이지에서 느려질 수 있음 |
| Cursor(커서) | 마지막 위치 토큰으로 다음 페이지 조회 | 일관성이 좋지만 토큰 만료·순서 규칙 확인 필요 |
| 시간 기반(`updated_since`) | 증분 수집에 적합 | 시간대, 경계값(≥ vs >), 늦게 도착하는 데이터 고려 |

또한 API의 **요청 한도(rate limit)** 를 확인하고, `429` 응답과 `Retry-After` 헤더를 처리해야 합니다.

#### 코드 구조화 예시

파이프라인 코드는 **순수 함수(입력 → 출력, 외부 부작용 없음)** 로 변환 로직을 분리하면 테스트가 쉬워집니다.

```text
project/
├── src/
│   ├── extract.py      # 외부 시스템에서 가져오기 (I/O)
│   ├── transform.py    # 순수 함수: 정제·변환 (테스트 용이)
│   ├── load.py         # DB/저장소에 적재 (I/O)
│   └── quality.py      # 품질 검사 규칙
├── tests/
│   └── test_transform.py
├── dags/               # Airflow DAG
├── sql/                # DDL, 변환 SQL
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

```python
# tests/test_transform.py
from src.transform import normalize_order

def test_normalize_order_strips_and_casts():
    raw = {"order_id": "1001", "amount": " 12.50 ", "status": "PAID"}
    assert normalize_order(raw) == {"order_id": 1001, "amount": 12.5, "status": "paid"}
```

#### Pandas, Polars, PySpark 선택 기준

Pandas를 무조건 피하거나 PySpark만 사용해야 하는 것은 아닙니다. 작은 데이터에는 Pandas가 간단하고 효율적입니다. 단일 머신에서 더 빠른 처리나 지연 실행이 필요하면 Polars, 단일 머신의 메모리·시간 한계를 넘으면 Spark 같은 분산 엔진을 고려합니다. (자세한 기준은 6장 참고)

### 2.2 SQL과 관계형 데이터베이스 [필수]

SQL은 데이터 엔지니어에게 가장 중요한 기본기 중 하나입니다. 데이터를 추출하는 것뿐 아니라 정합성, 중복, 적재 방식, 성능을 이해하는 데 필요합니다.

**학습 항목**

- `SELECT`, `WHERE`, `JOIN`(INNER/LEFT/FULL/CROSS), `GROUP BY`, `HAVING`
- 서브쿼리와 CTE(`WITH`)
- 윈도 함수(`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `SUM() OVER`)
- 기본키·외래키·고유·`CHECK`·`NOT NULL` 제약조건
- 인덱스와 `EXPLAIN` / `EXPLAIN ANALYZE`를 이용한 실행 계획 기초
- 트랜잭션, ACID, 격리 수준의 기본 개념
- UPSERT, 증분 적재, 중복 제거
- 스키마 설계와 정규화 기초
- `NULL`의 3값 논리(`= NULL`이 아니라 `IS NULL`)

PostgreSQL이나 MySQL 중 하나를 골라 실습해도 출발점으로 충분합니다. 둘을 동시에 깊게 파기보다 하나를 익히고, 이후 차이점을 비교하는 것이 효율적입니다.

#### 필수 패턴 1: 윈도 함수로 최신 레코드만 남기기 (중복 제거)

```sql
-- 같은 order_id가 여러 번 들어온 경우, 가장 최근 updated_at 한 건만 선택
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC, ingested_at DESC   -- 동률 방지를 위한 보조 정렬
        ) AS rn
    FROM raw_orders
)
SELECT *
FROM ranked
WHERE rn = 1;
```

#### 필수 패턴 2: 이전 행과 비교하기

```sql
-- 일별 매출과 전일 대비 증감
SELECT
    order_date,
    daily_revenue,
    daily_revenue - LAG(daily_revenue) OVER (ORDER BY order_date) AS diff_from_prev_day
FROM daily_sales;
```

#### 필수 패턴 3: UPSERT (PostgreSQL)

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id    BIGINT        PRIMARY KEY,
    customer_id BIGINT        NOT NULL,
    amount      NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    status      TEXT          NOT NULL,
    updated_at  TIMESTAMPTZ   NOT NULL,
    loaded_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

INSERT INTO orders (order_id, customer_id, amount, status, updated_at)
SELECT order_id, customer_id, amount, status, updated_at
FROM stg_orders
ON CONFLICT (order_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    amount      = EXCLUDED.amount,
    status      = EXCLUDED.status,
    updated_at  = EXCLUDED.updated_at,
    loaded_at   = now()
WHERE orders.updated_at < EXCLUDED.updated_at;   -- 더 오래된 데이터가 최신을 덮어쓰지 않도록 방지
```

주의할 점:

- 같은 `INSERT ... ON CONFLICT` 문 안에서 **같은 키가 두 번 이상 나오면** PostgreSQL은 오류를 냅니다. 그래서 스테이징 단계에서 위의 `ROW_NUMBER` 방식으로 먼저 중복을 제거합니다.
- MySQL은 `INSERT ... ON DUPLICATE KEY UPDATE`, PostgreSQL 15 이상과 여러 웨어하우스는 `MERGE` 문을 지원합니다. 문법과 동작은 제품마다 다릅니다.

#### 필수 패턴 4: 고수위선(High-water mark) 기반 증분 조회

```sql
-- 마지막으로 성공 적재한 시각 이후에 바뀐 행만 가져온다
SELECT *
FROM source_orders
WHERE updated_at >= :last_success_ts - INTERVAL '5 minutes'   -- 늦게 반영되는 데이터를 위한 여유(lookback)
  AND updated_at <  :run_ts;
```

경계에서 데이터가 누락되지 않도록 약간의 **중첩 구간(lookback)** 을 두고, 겹쳐서 가져온 데이터는 UPSERT로 흡수합니다.

#### 인덱스와 실행 계획 기초

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM orders WHERE customer_id = 123 AND order_date >= DATE '2026-01-01';
```

확인할 점:

| 신호 | 의미 / 점검 |
|---|---|
| `Seq Scan` (큰 테이블) | 적절한 인덱스가 없거나, 조건 선택도가 낮음 |
| `Index Scan` / `Index Only Scan` | 인덱스를 활용 중 |
| 예상 행 수와 실제 행 수의 큰 차이 | 통계 정보 오래됨 (`ANALYZE` 필요 가능) |
| `Sort`/`Hash` 단계에서 디스크 사용 | 메모리 설정 또는 쿼리 구조 점검 |
| 조건 컬럼에 함수 적용 (`WHERE DATE(col) = ...`) | 인덱스를 못 쓸 수 있음. 범위 조건으로 변경 |

복합 인덱스는 **컬럼 순서**가 중요합니다(예: `(customer_id, order_date)`는 `customer_id` 조건이 있을 때 효과적). 인덱스는 조회를 빠르게 하지만 쓰기 비용과 저장 공간이 늘어납니다.

#### 트랜잭션과 격리 수준

- **ACID**: 원자성, 일관성, 격리성, 지속성
- 격리 수준(Read Committed, Repeatable Read, Serializable 등)에 따라 허용되는 이상 현상(dirty read, non-repeatable read, phantom read)이 달라집니다.
- 기본 격리 수준은 제품마다 다릅니다(예: PostgreSQL은 기본 Read Committed, MySQL InnoDB는 기본 Repeatable Read).
- 파이프라인에서는 **"삭제 후 적재" 같은 여러 단계를 하나의 트랜잭션으로 묶어** 중간 실패 시 반쪽짜리 데이터가 남지 않도록 합니다.

### 2.3 Linux, Git, Docker 기초 [필수 → 권장]

데이터 파이프라인은 개발자의 로컬 환경뿐 아니라 서버나 컨테이너에서 실행될 수 있습니다.

- **Linux**: 기본 명령(`grep`, `awk`, `sed`, `find`, `tail -f`), 환경변수, 프로세스, 파일 권한, `cron`, 셸 스크립트 기초
- **Git**: 브랜치 전략, 커밋 메시지, Pull Request, 충돌 해결, `.gitignore`(비밀정보·데이터 파일 제외)
- **Docker**: 이미지/컨테이너 구분, `Dockerfile`, 볼륨(데이터 영속화), 네트워크, 환경변수 주입
- **비밀정보 관리**: 비밀번호·API 키를 코드나 저장소에 넣지 않고 환경변수, `.env`(저장소 제외), 클라우드 Secret Manager 등을 사용
- **CI**: 코드 푸시 시 린트·테스트를 자동 실행(GitHub Actions 등)

Kubernetes와 Terraform은 유용하지만 처음부터 필수로 깊게 학습할 필요는 없습니다. 먼저 Docker로 애플리케이션을 실행하고, 배포·운영 요구가 생겼을 때 확장하는 것이 좋습니다.

---

## 3. 데이터 파이프라인의 기본 구조와 설계 원칙

일반적인 데이터 파이프라인은 다음과 같이 구성됩니다.

```text
원천 시스템 / 공개 API / 파일
          |
          v
      수집(Ingestion)
          |
          v
   원본 저장소(Raw / Bronze)
          |
          v
   정제·변환(Transform)
          |
          v
   검증 및 품질 검사
          |
          v
  분석용 저장소(Warehouse / Gold)
          |
          v
   SQL 분석 / 대시보드 / 서비스
```

### 3.1 ETL과 ELT

| 구분 | 흐름 | 특징 |
|---|---|---|
| **ETL** | 추출 → 변환 → 적재 | 변환을 적재 전에 수행. 목적지에는 정제된 데이터만 적재 |
| **ELT** | 추출 → 적재 → 변환 | 원본을 먼저 웨어하우스/레이크에 적재하고 그 안에서 SQL 등으로 변환. 클라우드 웨어하우스에서 흔한 방식이며 dbt와 잘 맞음 |

어느 한쪽이 항상 옳은 것은 아닙니다. 데이터 민감도, 변환 복잡도, 목적지 엔진의 성능과 비용에 따라 선택합니다.

### 3.2 설계 질문

파이프라인을 설계할 때는 다음 질문에 답할 수 있어야 합니다.

1. 데이터는 어디에서 오며, 얼마나 자주 수집하는가?
2. 전체 데이터를 매번 가져오는가, 변경분만 가져오는가?
3. 실패한 작업을 어떻게 재시도하는가?
4. 재실행해도 중복 데이터가 생기지 않는가?
5. 데이터가 누락되거나 스키마가 바뀌면 어떻게 감지하는가?
6. 처리 건수와 실행 시간, 비용을 어떻게 관찰하는가?
7. 결과가 올바른지 어떤 규칙으로 검증하는가?

이 질문들에 대한 답을 프로젝트 README와 설계 문서에 남기는 것이 중요합니다.

### 3.3 멱등성(Idempotency): 가장 중요한 설계 원칙

**멱등성**이란 같은 작업을 여러 번 실행해도 결과가 한 번 실행한 것과 같다는 성질입니다. 재시도, 백필(과거 재처리), 장애 복구가 모두 "다시 실행"에 의존하므로 파이프라인의 핵심 요건입니다.

| 패턴 | 설명 | 적합한 경우 |
|---|---|---|
| **파티션 덮어쓰기** | 날짜 파티션 단위로 지우고 다시 쓰기 | 일/시간 단위 배치, 파일 기반 저장소 |
| **UPSERT / MERGE** | 고유 키 기준으로 있으면 갱신, 없으면 삽입 | 변경이 발생하는 엔티티 테이블 |
| **삭제 후 삽입 (같은 트랜잭션)** | 해당 기간 데이터를 지우고 새로 삽입 | RDBMS, 소규모 |
| **스테이징 → 교체(swap)** | 임시 테이블에 완성 후 한 번에 교체 | 부분 상태 노출을 막고 싶을 때 |
| **중복 제거 키** | 레코드의 해시·자연키로 중복 판별 | 이벤트 로그 적재 |

**안티패턴**: 단순 `INSERT`(append)만 하는 작업에 재시도를 켜면, 실패 후 재시도 시 일부 데이터가 중복 적재될 수 있습니다.

### 3.4 증분 적재와 백필, 늦게 도착하는 데이터

- **Full load**: 매번 전체를 가져옴. 단순하지만 데이터가 커지면 비효율적.
- **Incremental load**: 변경분만 가져옴. 효율적이지만 기준 컬럼(`updated_at`, 증가 ID 등)이 신뢰할 수 있어야 하고, **삭제된 행**은 별도 방법(소프트 삭제 플래그, CDC)이 없으면 감지하기 어렵습니다.
- **Backfill**: 과거 구간을 재처리. 실행 구간(`data_interval`)을 파라미터로 받도록 설계해야 쉽습니다.
- **Late-arriving data**: 이벤트 발생 시각(event time)보다 늦게 시스템에 도착하는 데이터. lookback 구간을 두거나 파티션을 재처리하는 정책을 정합니다.
- **시간대**: 저장은 UTC로 통일하고, 표시 단계에서 변환하는 것을 기본으로 삼으면 혼란을 줄일 수 있습니다.

### 3.5 장애 유형과 대응 (포트폴리오에서 설명할 만한 목록)

| 장애 | 감지 방법 | 대응 예 |
|---|---|---|
| API 일시 오류/타임아웃 | HTTP 상태, 예외 로그 | 백오프 재시도, 최종 실패 시 알림 |
| 중복 적재 | 고유 키 중복 검사 | UPSERT, 멱등 설계 |
| 스키마 변경(컬럼 추가·삭제·타입 변경) | 스키마 검증, 로드 실패 | 원본 보존 후 변환 단계에서 처리, 변경 알림 |
| 데이터 누락/지연 | 행 수·신선도 검사 | 해당 파티션 재처리, 알림 |
| 부분 실패(일부 적재 후 종료) | 트랜잭션/스테이징 | 트랜잭션 롤백 또는 스테이징 교체 |
| 잘못된 값(음수 금액 등) | 범위·유효성 검사 | 격리(quarantine) 테이블로 분리, 원인 분석 |

잘못된 레코드를 그냥 버리지 말고 별도 테이블이나 경로에 **격리(dead-letter / quarantine)** 하면 원인 추적과 재처리가 쉬워집니다.

---

## 4. 배치와 스트리밍

### 4.1 배치 처리(Batch Processing) [필수]

일정 시간 동안 쌓인 데이터를 모아서 처리합니다.

**예**

- 매일 새벽 전날 주문 데이터를 집계
- 하루치 공개 데이터를 수집하고 정제
- 주기적으로 데이터 웨어하우스를 갱신

**장점**: 구조가 비교적 단순하고, 대량 데이터를 효율적으로 처리하며, 재처리가 쉽습니다.
**단점**: 결과가 실시간으로 나오지 않습니다(지연이 스케줄 주기만큼 존재).

### 4.2 스트리밍 처리(Streaming Processing) [권장~선택]

이벤트가 발생하는 대로 계속 처리합니다.

**예**

- 결제 이벤트를 수초 이내에 집계
- 센서 데이터에서 이상 징후 탐지
- 웹 로그를 실시간으로 분석

### 4.3 Kafka 핵심 개념

Kafka는 주로 이벤트 스트리밍 플랫폼으로 사용됩니다.

| 개념 | 설명 |
|---|---|
| **Topic** | 이벤트가 기록되는 논리적 채널 |
| **Partition** | 토픽을 나눈 단위. 처리량 확장의 기본 단위이며 **파티션 내에서만 순서가 보장**됨 |
| **Key** | 같은 키는 같은 파티션으로 가므로, 같은 엔티티의 이벤트 순서를 유지하는 데 사용 |
| **Offset** | 파티션 내 레코드의 위치 번호 |
| **Producer / Consumer** | 이벤트를 발행 / 구독하는 클라이언트 |
| **Consumer Group** | 파티션을 나눠 병렬로 소비하는 소비자 집합 |
| **Retention** | 소비 여부와 관계없이 설정된 기간·용량만큼 보관 (재처리 가능) |
| **Log Compaction** | 키별 최신 값을 유지하는 보관 방식 |
| **Schema Registry** | Avro/Protobuf/JSON Schema 등의 스키마를 중앙 관리하고 호환성 검사 |

**전달 보장(delivery semantics)**

| 보장 | 의미 | 비고 |
|---|---|---|
| At-most-once | 최대 한 번 (유실 가능) | 커밋을 먼저 하고 처리 |
| At-least-once | 최소 한 번 (중복 가능) | 처리 후 커밋. 가장 흔한 기본 접근 |
| Exactly-once | 정확히 한 번 효과 | Kafka 트랜잭션/멱등 프로듀서 등 제한된 조건과 설정이 필요. 외부 시스템에 쓸 때는 싱크의 멱등성도 필요 |

실무에서는 **at-least-once + 멱등한 싱크(UPSERT)** 조합으로 "사실상 한 번" 효과를 내는 경우가 많습니다.

```python
# 개념 예시: 처리 후 커밋(at-least-once). confluent-kafka 사용 가정
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-loader",
    "enable.auto.commit": False,        # 처리 완료 후 직접 커밋
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["orders"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue                         # 실제로는 오류 유형별 처리
    upsert_to_db(msg.key(), msg.value()) # 멱등한 UPSERT
    consumer.commit(message=msg)         # 성공 후 오프셋 커밋
```

참고: Kafka는 최근 버전에서 ZooKeeper 없이 KRaft 모드로 운영하는 방향으로 바뀌었습니다. 학습 시 사용하는 버전의 문서를 확인하세요.

### 4.4 Flink 핵심 개념

Flink는 상태를 가진 스트림 처리와 이벤트 시간 기반 연산에 강점이 있습니다.

| 개념 | 설명 |
|---|---|
| **Event time vs Processing time** | 이벤트가 실제 발생한 시각 vs 시스템이 처리한 시각. 늦게 도착한 데이터가 있으면 둘이 다름 |
| **Watermark** | "이 시각 이전의 이벤트는 대부분 도착했다"는 진행 표시. 윈도 결과를 언제 확정할지 결정 |
| **Window** | Tumbling(고정, 겹침 없음), Sliding(겹침), Session(활동 간격 기반) |
| **State** | 집계값처럼 연산이 기억하는 데이터 |
| **Checkpoint** | 상태의 일관된 스냅샷. 장애 후 복구에 사용 |

Kinesis와 Google Pub/Sub도 클라우드 기반 이벤트 전달에 활용됩니다.

### 4.5 학습 순서 제안

처음부터 Kafka와 Flink를 모두 깊게 배울 필요는 없습니다.

1. 배치 파이프라인을 먼저 완성
2. "왜 실시간이 필요한가?"를 요구사항으로 정리(지연 허용 시간, 정확도)
3. Kafka로 이벤트 발행·소비 + 멱등 적재 구현
4. 필요한 경우에만 Flink/Spark Structured Streaming 등 스트림 처리 엔진 추가

---

## 5. 워크플로 오케스트레이션: Airflow 등

오케스트레이션 도구는 여러 작업의 실행 순서, 일정, 의존성, 실패 처리를 관리합니다. 데이터 자체를 처리하는 엔진이라기보다 **"무엇을 언제 어떤 순서로 실행할지"** 를 조율하는 계층입니다.

### 5.1 Apache Airflow [필수: 도구 하나]

- **DAG**(방향성 비순환 그래프)로 작업 간 의존성을 표현
- 정해진 일정(cron 등)에 맞춰 실행
- 재시도와 실행 이력 관리, 작업별 로그 확인
- 백필(과거 구간 재실행) 지원

**핵심 개념**

| 개념 | 설명 |
|---|---|
| DAG / Task | 워크플로 / 개별 작업 단위 |
| Operator / TaskFlow(`@task`) | 작업을 정의하는 방식 |
| Scheduler / Executor / Worker | 일정 판단 / 실행 방식 / 실제 실행 주체 |
| `catchup` | 과거 누락 구간을 자동 실행할지 여부 |
| Data interval | 해당 실행이 담당하는 데이터 구간(시작~끝). 멱등한 파라미터화의 핵심 |
| XCom | 작업 간 **작은** 데이터 전달 수단. 큰 데이터는 저장소 경로만 전달 |
| Sensor | 파일 도착, 외부 조건을 기다리는 작업 |
| Pool / `max_active_runs` | 동시 실행 제한(외부 API, DB 보호) |

#### 예시: 일 배치 DAG (Airflow 2.x 스타일)

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task   # Airflow 3에서는 `from airflow.sdk import dag, task`

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),   # 작업이 무한정 걸리지 않도록
}

@dag(
    dag_id="orders_daily",
    schedule="0 2 * * *",              # 매일 02:00 (Airflow 2.4+ 에서는 schedule 사용)
    start_date=datetime(2026, 1, 1),
    catchup=False,                      # 백필은 필요할 때 명시적으로 실행
    max_active_runs=1,
    default_args=default_args,
    tags=["orders"],
)
def orders_daily():

    @task
    def extract(data_interval_start=None, data_interval_end=None) -> str:
        # 구간을 파라미터로 받아 같은 구간은 항상 같은 경로에 저장 (멱등)
        path = f"raw/orders/dt={data_interval_start:%Y-%m-%d}/data.json"
        # ... API 호출 후 path 에 저장 ...
        return path

    @task
    def transform(raw_path: str) -> str:
        # ... 정제 후 staging 경로 반환 ...
        return raw_path.replace("raw", "staging")

    @task
    def validate(staging_path: str) -> str:
        # 품질 검사 실패 시 예외를 발생시켜 적재를 막는다
        return staging_path

    @task
    def load(staging_path: str) -> None:
        # 스테이징 → 타깃으로 UPSERT 또는 파티션 덮어쓰기
        ...

    load(validate(transform(extract())))

orders_daily()
```

**자주 하는 실수**

- DAG 파일 최상위(import 시점)에서 DB 조회·API 호출을 실행 → 스케줄러가 DAG를 파싱할 때마다 실행되어 부하 유발
- `datetime.now()`를 작업 안에서 직접 사용 → 재실행 시 결과가 달라져 멱등성이 깨짐. 실행 구간 값을 사용
- XCom에 대용량 데이터를 전달
- 재시도를 켰지만 적재가 멱등하지 않음

### 5.2 Prefect와 Dagster

Airflow와 같은 문제 영역을 다루지만 구성 방식과 개발 경험, 데이터 자산 모델링, 운영 방식 등에 차이가 있습니다.

| 도구 | 특징(개념 수준) |
|---|---|
| Airflow | 가장 널리 쓰이는 사실상의 표준. 생태계와 자료가 풍부 |
| Prefect | Python 함수 중심, 동적 워크플로 구성에 유연 |
| Dagster | "데이터 자산(asset)" 중심 모델링, 계보·테스트와 결합이 강함 |

신입 준비 단계에서는 세 도구를 모두 깊게 다루기보다 **Airflow 하나로 파이프라인을 완성**하고, 다른 도구는 개념과 차이점을 비교하는 정도로 시작해도 됩니다.

### 5.3 재시도와 멱등성

Airflow의 재시도 설정만으로 데이터 중복이 자동 방지되는 것은 아닙니다. 작업을 다시 실행해도 결과가 안전하도록 증분 적재, 고유 키, UPSERT, 파티션 덮어쓰기 등의 설계가 필요합니다. (3.3 참고)

---

## 6. 대용량 데이터 처리: Spark, PySpark, Polars

### 6.1 Apache Spark / PySpark [권장]

Spark는 여러 노드에 데이터를 분산해 처리할 수 있는 엔진입니다. PySpark는 Python에서 Spark를 사용하는 인터페이스입니다.

**핵심 개념**

| 개념 | 설명 |
|---|---|
| **지연 실행(Lazy evaluation)** | 변환(transformation)은 계획만 세우고, 액션(`count`, `write` 등)이 호출될 때 실행 |
| **Narrow / Wide 변환** | `filter`, `select`처럼 파티션 내에서 끝나는 변환 vs `groupBy`, `join`처럼 파티션 간 데이터 이동(**셔플**)이 필요한 변환 |
| **셔플(Shuffle)** | 노드 간 데이터 재분배. 네트워크·디스크 I/O가 커서 성능의 주요 병목 |
| **파티션** | 병렬 처리의 단위. 너무 적으면 병렬성 부족, 너무 많으면 스케줄링 오버헤드 |
| **Broadcast join** | 작은 테이블을 모든 실행기에 복사해 큰 테이블의 셔플을 피하는 조인 |
| **데이터 쏠림(Skew)** | 특정 키에 데이터가 몰려 일부 작업만 오래 걸리는 현상 |
| **Spill** | 메모리가 부족해 디스크로 내려 쓰는 현상 |
| **AQE(Adaptive Query Execution)** | 실행 중 통계를 바탕으로 파티션 병합, 조인 전략 변경, 쏠림 완화를 돕는 기능(최근 버전에서 기본 활성) |

#### 예시: PySpark 집계 작업

```python
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("daily_sales")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # 처리한 파티션만 덮어쓰기
    .getOrCreate()
)

orders = spark.read.parquet("s3a://my-bucket/silver/orders/")
products = spark.read.parquet("s3a://my-bucket/silver/dim_product/")   # 작은 차원 테이블

daily = (
    orders
    .filter(F.col("order_date") >= "2026-01-01")                       # 가능한 한 일찍 필터링(프루닝)
    .join(F.broadcast(products), "product_id")                         # 작은 테이블은 broadcast
    .groupBy("order_date", "category")
    .agg(F.sum("amount").alias("revenue"), F.count("*").alias("order_cnt"))
)

daily.explain()                                                        # 실행 계획 확인

(daily.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://my-bucket/gold/daily_sales/"))
```

**성능 점검 체크리스트**

1. `explain()`과 Spark UI에서 **셔플 양, 스테이지별 시간, 특정 태스크만 오래 걸리는지(쏠림)** 확인
2. 필요한 컬럼만 `select`, 가능한 한 일찍 `filter` (컬럼 프루닝, 파티션 프루닝, 조건 푸시다운)
3. 스키마를 명시하거나 컬럼 지향 포맷(Parquet) 사용 → 스키마 추론 비용 감소
4. 작은 파일이 너무 많은지 확인 (파일 수가 많으면 메타데이터·스케줄링 비용 증가). 쓰기 전 `coalesce`/`repartition`으로 조절
5. Python UDF는 JVM 간 직렬화 비용이 크므로, 가능하면 내장 함수(`pyspark.sql.functions`)를 우선 사용
6. `repartition`(셔플 발생, 균등 분배)과 `coalesce`(셔플 없이 파티션 감소) 차이 이해
7. 캐시(`cache`)는 같은 데이터를 여러 번 재사용할 때만 사용하고, 사용 후 해제

### 6.2 Polars와 Pandas

| 도구 | 특징 | 적합한 규모(대략적 가이드) |
|---|---|---|
| **Pandas** | 널리 쓰임, 생태계 풍부. 주로 즉시 실행, 메모리 사용량이 큰 편 | 메모리에 여유 있게 올라가는 작은~중간 데이터 |
| **Polars** | 열 지향, 지연 실행(`scan_*` + `collect`), 멀티스레드 활용 | 단일 머신에서 큰 데이터를 빠르게 처리 |
| **Spark** | 여러 노드에 분산 처리 | 단일 머신의 메모리·시간 한계를 넘거나 이미 클러스터 환경이 있는 경우 |

```python
import polars as pl

result = (
    pl.scan_parquet("orders/*.parquet")        # 지연 실행: 필요한 컬럼/행만 읽도록 최적화
    .filter(pl.col("order_date") >= pl.date(2026, 1, 1))
    .group_by("category")
    .agg(pl.col("amount").sum().alias("revenue"))
    .collect()
)
```

도구 선택은 유행이 아니라 **데이터 크기, 메모리, 처리 시간, 운영 복잡도, 비용**을 기준으로 해야 합니다. "데이터가 수 GB인데 클러스터를 쓴다"는 과설계가 될 수 있다는 점을 면접에서 설명할 수 있으면 좋습니다.

### 6.3 Databricks와 Amazon EMR

Databricks는 Spark 기반 분석·데이터 플랫폼을 제공하며, Amazon EMR은 AWS에서 Spark 등 빅데이터 프레임워크를 실행할 수 있는 관리형 서비스입니다. 둘 다 클러스터 관리 부담을 줄여 주지만, 구성 방식과 과금 요소가 다릅니다.

**비용을 이해하기 위해 확인할 요소**

- 클러스터를 켜 둔 시간 (유휴 시간, **자동 종료** 설정)
- 컴퓨팅 자원의 크기와 개수, 스팟/온디맨드 인스턴스 선택
- 저장소 비용과 데이터 전송(특히 리전·클라우드 간 이동) 비용
- 셔플·메모리 사용량으로 인한 처리 시간 증가
- 작업 시간 대비 처리량

신입 포트폴리오에서는 소규모 실습과 비용 추정, 설계상의 트레이드오프를 설명할 수 있으면 충분합니다.

---

## 7. 데이터 저장 구조와 아키텍처

### 7.1 RDBMS, 데이터 웨어하우스, 데이터 레이크

| 구분 | 특징 | 주 용도 |
|---|---|---|
| **RDBMS (OLTP)** | 행 지향, 트랜잭션, 짧고 빈번한 읽기/쓰기 | 서비스 운영 데이터 |
| **데이터 웨어하우스 (OLAP)** | 주로 열 지향, 대규모 집계·분석 쿼리에 최적화 | 분석, BI |
| **데이터 레이크** | 객체 스토리지에 다양한 형태의 원본 데이터를 대규모로 저장 | 원본 보존, 다양한 엔진에서 활용 |
| **Lakehouse** | 레이크의 유연성 + 테이블 관리(트랜잭션·스키마·버전)·분석 기능을 결합하려는 접근 | 오픈 테이블 포맷(8장) 기반 |

**OLTP vs OLAP**: 같은 데이터라도 "주문 한 건 조회·수정"(OLTP)과 "작년 월별 지역별 매출 합계"(OLAP)는 요구되는 저장 구조가 다릅니다. 분석 쿼리가 운영 DB를 느리게 만들지 않도록 분리하는 것이 데이터 엔지니어링의 동기 중 하나입니다.

### 7.2 차원 모델링: Star Schema와 Snowflake Schema

- **Star Schema**: 중앙의 Fact 테이블을 Dimension 테이블과 연결하는 구조입니다. 이해와 조회가 비교적 단순합니다.
- **Snowflake Schema**: Dimension을 더 정규화해 여러 테이블로 나눈 구조입니다. 중복을 줄일 수 있지만 조인이 늘어날 수 있습니다.

**Fact vs Dimension**

- **Fact**: 측정 가능한 사건(판매, 클릭). 수치(measure)와 차원 키로 구성
- **Dimension**: 사건을 설명하는 맥락(상품, 고객, 날짜, 지역)
- **Grain(입도)**: Fact 테이블의 한 행이 무엇을 의미하는지(예: "주문 상품 한 줄"). **모델링 시 가장 먼저 정해야 하는 항목**입니다.

```sql
CREATE TABLE dim_date (
    date_key     INT PRIMARY KEY,         -- 예: 20260115
    full_date    DATE NOT NULL,
    year         SMALLINT, quarter SMALLINT, month SMALLINT, day_of_week SMALLINT,
    is_weekend   BOOLEAN
);

CREATE TABLE dim_product (
    product_key  BIGSERIAL PRIMARY KEY,   -- 대리키(surrogate key)
    product_id   TEXT NOT NULL,           -- 원천 시스템의 자연키(natural key)
    name         TEXT,
    category     TEXT
);

CREATE TABLE fact_sales (
    date_key     INT    NOT NULL REFERENCES dim_date(date_key),
    product_key  BIGINT NOT NULL REFERENCES dim_product(product_key),
    customer_key BIGINT NOT NULL,
    order_id     TEXT   NOT NULL,
    quantity     INT    NOT NULL,
    amount       NUMERIC(12,2) NOT NULL    -- grain: 주문 상품 한 줄
);
```

#### 느리게 변하는 차원(SCD)

고객의 등급이나 주소처럼 시간이 지나며 바뀌는 차원 속성을 처리하는 방식입니다.

| 유형 | 방식 | 특징 |
|---|---|---|
| Type 1 | 덮어쓰기 | 이력 없음, 단순 |
| Type 2 | 새 행 추가 + 유효 기간(`valid_from`, `valid_to`, `is_current`) | 과거 시점 기준 분석 가능. 가장 흔히 면접에서 다룸 |
| Type 3 | 이전 값 컬럼을 별도로 보관 | 제한적 이력 |

dbt의 **snapshot** 기능은 Type 2 이력 관리를 구현하는 데 사용됩니다.

### 7.3 dbt [권장]

dbt는 주로 SQL 기반 데이터 변환 모델을 관리하고, 모델 간 의존성을 표현하며, 테스트와 문서화를 지원합니다. 일반적으로 원천 데이터를 직접 수집하는 도구라기보다 웨어하우스나 분석 플랫폼 안에서 변환 계층(ELT의 T)을 관리하는 데 사용합니다.

**학습할 항목**

- 모델(`select` 문 하나가 하나의 모델)과 `ref()` 를 통한 모델 간 참조 → 의존 그래프(DAG) 자동 생성
- `source()` 로 원천 테이블 정의, 신선도(freshness) 검사
- **Materialization**: `view`, `table`, `incremental`, `ephemeral`
- 테스트: 일반 테스트(`unique`, `not_null`, `accepted_values`, `relationships`)와 사용자 정의 SQL 테스트
- 문서화(`description`)와 문서 사이트 생성
- 증분 모델의 `unique_key`와 증분 조건
- snapshot의 목적(SCD Type 2)
- 개발/운영 환경(target) 분리, `dbt build`(모델 실행 + 테스트)

```sql
-- models/silver/orders.sql
{{ config(materialized='incremental', unique_key='order_id') }}

select
    order_id,
    customer_id,
    cast(amount as numeric(12,2)) as amount,
    lower(status)                 as status,
    updated_at
from {{ source('raw', 'orders') }}

{% if is_incremental() %}
  -- 이미 적재된 최대 updated_at 이후 데이터만 처리
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

```yaml
# models/silver/schema.yml
version: 2
models:
  - name: orders
    description: "정제된 주문 테이블 (주문당 1행)"
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: status
        tests:
          - accepted_values:
              values: ['paid', 'cancelled', 'refunded']
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: customer_id
```

### 7.4 Medallion Architecture

흔히 다음과 같이 계층을 나눕니다.

| 계층 | 내용 | 원칙 |
|---|---|---|
| **Bronze** | 원본에 가까운 데이터 보존 | 가능한 한 수정하지 않고 적재 시각·출처 메타데이터를 추가 |
| **Silver** | 정제·표준화·중복 제거·타입 정리 | 엔티티 단위로 신뢰할 수 있는 데이터 |
| **Gold** | 분석·업무 활용에 맞게 집계·모델링 | 대시보드, 지표, 서비스가 직접 사용 |

이 구조는 모든 시스템에 반드시 적용해야 하는 표준이라기보다 데이터 처리 단계를 명확하게 나누는 설계 패턴입니다. 원본을 보존하면 **변환 로직에 버그가 있어도 원본에서 다시 계산**할 수 있다는 점이 핵심 이점입니다.

### 7.5 CDC와 Event Sourcing [선택]

- **CDC(Change Data Capture)**: 원천 데이터베이스의 변경 사항을 감지해 다른 시스템으로 전달하는 방식입니다.
- **Event Sourcing**: 현재 상태만 저장하는 대신 상태 변경 이벤트를 중심으로 기록하고, 이를 통해 상태를 재구성할 수 있도록 하는 설계 방식입니다.

둘은 서로 다른 개념입니다. CDC는 변경 사항을 포착하고 전달하는 기술적 접근이며, Event Sourcing은 시스템의 상태를 기록하는 아키텍처 패턴입니다.

**CDC 구현 방식 비교**

| 방식 | 원리 | 장단점 |
|---|---|---|
| **로그 기반** | DB의 트랜잭션 로그(PostgreSQL WAL의 논리적 디코딩, MySQL binlog)를 읽음 | 삭제 포함 모든 변경 포착, 원천 부하 낮음. 설정·권한·운영 복잡도 있음 (예: Debezium) |
| **쿼리 기반** | `updated_at` 등을 주기적으로 조회 | 구현이 단순. 삭제 감지 어렵고 중간 변경은 놓칠 수 있음 |
| **트리거 기반** | 변경 시 트리거가 변경 테이블에 기록 | 원천 DB 부하와 스키마 영향 |

로그 기반 CDC의 이벤트에는 보통 변경 전/후 값, 작업 유형(생성·수정·삭제), 트랜잭션 정보 등이 담기며, 하위 시스템은 이를 **순서와 멱등성을 고려해** 적용해야 합니다.

### 7.6 Lambda와 Kappa Architecture [선택]

- **Lambda Architecture**: 배치 경로(정확하고 완전한 결과)와 실시간 경로(빠른 근사 결과)를 함께 운영해 결과를 제공하는 접근입니다. 단점은 **두 경로의 로직을 중복 구현·유지**해야 한다는 점입니다.
- **Kappa Architecture**: 가능한 경우 스트리밍 경로 하나를 중심으로 데이터를 처리하고, 재처리도 스트림을 다시 처리(로그 재생)하는 방식으로 접근합니다. 단점은 장기 보관·재처리 비용과 스트림 처리 복잡도입니다.

두 방식 모두 장단점이 있습니다. 데이터의 지연 허용 범위, 재처리 요구, 시스템 복잡도에 따라 선택합니다. 이름을 암기하는 것보다 어떤 문제를 해결하는지 설명할 수 있어야 합니다.

### 7.7 Data Mesh [선택]

Data Mesh는 단일 중앙 팀에 데이터 책임을 집중시키기보다 도메인별 데이터 소유권과 제품 관점, 공통 거버넌스를 강조하는 조직·아키텍처 접근입니다. 흔히 다음 네 원칙으로 설명됩니다.

1. 도메인 중심의 데이터 소유권
2. 데이터를 제품으로 취급
3. 셀프서비스 데이터 플랫폼
4. 연합형(federated) 거버넌스

단순히 특정 저장소나 제품 하나를 설치한다고 구현되는 것은 아닙니다.

---

## 8. 데이터 포맷과 오픈 테이블 포맷

### 8.1 파일 포맷

| 포맷 | 저장 방식 | 특징 | 주 용도 |
|---|---|---|---|
| **CSV/JSON** | 행(텍스트) | 사람이 읽기 쉬움. 타입·스키마 정보 약함, 용량 큼 | 교환, 소규모 |
| **Parquet** | 열 지향 | 컬럼 단위 압축, 컬럼 프루닝, 통계 기반 조건 푸시다운(row group 단위) | 분석 작업의 사실상 표준 |
| **ORC** | 열 지향 | Hive/Hadoop 생태계에서 많이 사용, 인덱스·통계 포함 | 분석 처리 |
| **Avro** | 행 지향 | 스키마를 포함한 레코드 직렬화, 스키마 진화 지원 | 이벤트 스트리밍(Kafka), 레코드 단위 교환 |

**열 지향이 분석에 유리한 이유**: "매출 합계"처럼 일부 컬럼만 필요한 쿼리에서 해당 컬럼만 읽을 수 있고, 같은 타입의 값이 모여 있어 압축률이 높습니다.

**압축 코덱**: Snappy(빠름, 보통 압축률), ZSTD(높은 압축률, 속도와 균형), Gzip(높은 압축률, 느림) 등. 읽기 속도와 저장 비용 사이의 균형으로 선택합니다.

**파티셔닝과 파일 크기**

```text
s3://bucket/silver/orders/
    order_date=2026-01-01/part-0001.parquet
    order_date=2026-01-02/part-0001.parquet
```

- 자주 필터링하는 컬럼(주로 날짜)으로 파티션을 나누면 쿼리가 불필요한 파티션을 건너뜁니다(파티션 프루닝).
- **카디널리티가 너무 높은 컬럼**(예: `user_id`)으로 파티션을 나누면 작은 파일이 폭증합니다.
- 일반적으로 파일 하나가 너무 작으면(수 MB 이하) 메타데이터·열기 비용이 늘고, 너무 크면 병렬성이 떨어집니다. 엔진과 환경별 권장 크기(대략 수십~수백 MB 수준에서 조정)를 확인하세요.

### 8.2 오픈 테이블 포맷 [선택]

- **Apache Iceberg**
- **Delta Lake**
- **Apache Hudi**

이 기술들은 데이터 레이크에 저장된 파일(Parquet 등)을 **테이블처럼** 관리하고, 스키마 변경·테이블 버전(스냅샷)·변경 데이터 처리 등 여러 기능을 지원합니다. 파일 위에 **메타데이터 계층**(스냅샷, 매니페스트, 트랜잭션 로그 등)을 두어 구현합니다.

**공통적으로 제공하려는 기능**

| 기능 | 설명 |
|---|---|
| ACID 트랜잭션 | 동시 읽기/쓰기에서도 일관된 테이블 뷰 제공 |
| 스키마 진화 | 컬럼 추가·이름 변경 등을 안전하게 처리 |
| 타임 트래블 | 과거 스냅샷 기준으로 조회 |
| Upsert/Delete(Merge) | 파일 기반 저장소에서 행 단위 변경 |
| 파티션/레이아웃 관리 | 작은 파일 병합(compaction), 파티션 진화 등 |

**개념적 차이(요약, 세부 기능은 버전별로 확인 필요)**

| 항목 | 특징 |
|---|---|
| Iceberg | 스냅샷·매니페스트 기반 메타데이터 구조, 여러 엔진과의 호환성을 강조 |
| Delta Lake | `_delta_log` 트랜잭션 로그 기반, Spark/Databricks 환경에서 널리 사용 |
| Hudi | 증분 처리와 레코드 단위 upsert에 초점, Copy-on-Write / Merge-on-Read 방식 제공 |

기능과 엔진 호환성은 각 프로젝트 및 버전에 따라 다르므로 실제 적용 전에 확인해야 합니다.

---

## 9. 데이터베이스 확장과 NoSQL

### 9.1 Replication, Partitioning, Sharding [선택]

| 개념 | 설명 | 주 목적 |
|---|---|---|
| **Replication** | 데이터를 복제 | 가용성, 읽기 처리량, 백업 |
| **Partitioning** | 데이터를 기준에 따라 논리적/물리적으로 분할 | 관리 용이성, 쿼리 성능(프루닝) |
| **Sharding** | 데이터를 여러 서버(노드)에 분산 저장 | 쓰기·저장 용량의 수평 확장 |

이들은 같은 개념이 아닙니다. 파티셔닝은 한 데이터베이스 내부에서도 적용할 수 있고, 샤딩은 일반적으로 여러 노드에 데이터를 나누어 배치하는 확장 전략입니다.

**알아둘 점**

- **복제 지연(replication lag)**: 비동기 복제에서는 읽기 복제본이 최신 쓰기를 즉시 반영하지 않을 수 있습니다.
- **파티셔닝 방식**: Range(날짜 범위), List(값 목록), Hash(해시 분산).
- **샤드 키 선택**: 데이터와 요청이 고르게 분산되어야 하며, 특정 샤드에 몰리면 **핫스팟**이 생깁니다. 샤드 간 조인·트랜잭션은 복잡해지므로 샤딩은 다른 방법(인덱스, 파티셔닝, 읽기 복제본, 스케일 업)으로 해결되지 않을 때 고려합니다.

```sql
-- PostgreSQL 선언적 파티셔닝 예 (범위)
CREATE TABLE events (
    event_id   BIGINT,
    event_time TIMESTAMPTZ NOT NULL,
    payload    JSONB
) PARTITION BY RANGE (event_time);

CREATE TABLE events_2026_01 PARTITION OF events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

### 9.2 NoSQL [선택]

MongoDB와 Cassandra는 데이터 모델과 확장 방식이 서로 다릅니다.

| 항목 | MongoDB | Cassandra |
|---|---|---|
| 모델 | 문서(JSON 유사) | 와이드 컬럼(파티션 키 + 클러스터링 키) |
| 모델링 접근 | 함께 읽는 데이터는 **내장(embedding)**, 독립적이면 **참조(referencing)** | **쿼리 우선 모델링**: 조회 패턴마다 테이블 설계, 조인 없음 |
| 확장 | 샤딩, 복제 세트 | 분산 노드 구조, 쓰기 확장성 중시 |
| 일관성 | 설정에 따라 조절 | 일관성 수준(consistency level)을 쿼리별로 조절 |

신입 단계에서는 모든 NoSQL 제품을 숙련하기보다 RDBMS의 강점과 한계를 먼저 이해하고, 실제 사용 사례에 따라 하나의 NoSQL 모델을 경험하는 것으로 시작해도 충분합니다. 핵심은 "**왜 RDBMS로 부족한지**"를 요구사항(데이터 모델의 유연성, 쓰기 규모, 지연시간)으로 설명할 수 있는 것입니다.

---

## 10. 데이터 품질, 계보, 거버넌스, 관측성

### 10.1 Data Quality [필수]

데이터 품질 검사는 파이프라인이 "실행됐다"는 사실만으로 데이터가 올바르다고 판단하지 않도록 합니다. **성공 상태의 작업이 잘못된 데이터를 만들 수 있기 때문**입니다.

**품질 차원**

| 차원 | 질문 | 검사 예 |
|---|---|---|
| 완전성(Completeness) | 필요한 값이 비어 있지 않은가? | 필수 컬럼 NULL 비율 |
| 고유성(Uniqueness) | 중복이 없는가? | 기본키·고유키 중복 |
| 유효성(Validity) | 형식·범위가 맞는가? | 금액 ≥ 0, 날짜 형식, 허용값 목록 |
| 일관성(Consistency) | 테이블 간 관계가 맞는가? | 외래키 참조, 합계 대사 |
| 적시성(Timeliness) | 최신인가? | 마지막 적재 시각, 지연 |
| 정확성(Accuracy) | 실제 값과 일치하는가? | 원천 대비 건수·합계 비교(reconciliation) |

**SQL로 구현하는 기본 검사 예시**

```sql
-- 1) 필수 컬럼 NULL 개수 (0이어야 통과)
SELECT COUNT(*) AS null_cnt FROM orders WHERE customer_id IS NULL;

-- 2) 키 중복 (결과 행이 없어야 통과)
SELECT order_id, COUNT(*) FROM orders GROUP BY order_id HAVING COUNT(*) > 1;

-- 3) 허용 범위 위반
SELECT COUNT(*) FROM orders WHERE amount < 0;

-- 4) 참조 무결성 (고아 레코드)
SELECT COUNT(*)
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 5) 볼륨 이상 감지: 오늘 건수가 최근 7일 평균의 50% 미만이면 경고
WITH daily AS (
    SELECT order_date, COUNT(*) AS cnt FROM orders
    WHERE order_date >= CURRENT_DATE - 8 GROUP BY order_date
)
SELECT
    (SELECT cnt FROM daily WHERE order_date = CURRENT_DATE) AS today_cnt,
    (SELECT AVG(cnt) FROM daily WHERE order_date < CURRENT_DATE) AS avg_prev_7d;
```

**실패 시 정책을 정해 두세요.**

| 정책 | 설명 |
|---|---|
| 차단(fail fast) | 파이프라인을 중단하고 하위 테이블 갱신을 막음 (치명적 규칙) |
| 경고(warn) | 알림만 보내고 진행 (덜 치명적인 규칙) |
| 격리(quarantine) | 문제 행만 별도 테이블로 분리하고 나머지 진행 |

Great Expectations와 Soda는 데이터 검증 규칙을 정의하고 실행하는 데 활용할 수 있습니다. 처음에는 SQL, Python, dbt 테스트로 기본 검사를 구현한 뒤 도구를 추가해도 됩니다.

### 10.2 Data Lineage [권장~선택]

데이터가 어떤 원천에서 출발해 어떤 변환을 거쳐 최종 테이블이나 보고서가 되었는지 추적하는 것입니다.

- **장애 원인 분석**: 이상 값이 어느 단계에서 생겼는지 거슬러 올라감
- **영향 분석**: 컬럼을 변경하면 어떤 하위 테이블·대시보드가 영향을 받는지 파악
- 테이블 단위 계보와 **컬럼 단위 계보**가 있으며, dbt의 `ref` 그래프, OpenLineage 같은 표준, 카탈로그 도구가 이를 지원합니다.

### 10.3 Data Governance와 Metadata [선택]

- **Data Governance**: 데이터 소유권, 접근 권한, 보안, 품질 책임, 정책을 관리하는 체계
- **Metadata**: 데이터의 구조(기술 메타데이터), 의미·소유자(비즈니스 메타데이터), 생성·갱신 이력(운영 메타데이터)
- **Data Catalog**: 데이터 자산을 검색하고 설명과 소유권 등을 확인할 수 있게 하는 목록·검색 체계
- **개인정보 보호**: 개인식별정보(PII)는 최소 수집, 접근 제한, 마스킹·가명처리, 보관 기간 관리가 필요합니다. 적용 법규와 사내 정책은 조직·국가마다 다르므로 해당 기준을 확인해야 합니다.
- **권한 관리**: 최소 권한 원칙, 역할 기반 접근 제어(RBAC)

OpenMetadata와 DataHub는 메타데이터와 데이터 자산을 관리하는 데 활용되는 플랫폼 예시입니다. 신입은 먼저 테이블 설명, 소유자, 스키마, 계보의 의미를 이해하는 것이 중요합니다.

### 10.4 Monitoring, Logging, Observability [권장]

| 용어 | 역할 |
|---|---|
| **Logging** | 어떤 일이 발생했는지 기록 |
| **Monitoring** | 정해진 지표와 임계값을 관찰하고 알림 |
| **Observability** | 로그·지표·추적(trace) 등을 활용해 시스템 내부 상태와 원인을 파악 |
| **Data Observability** | 데이터의 신선도, 분포, 스키마, 볼륨, 품질 이상 등을 관찰 |

**파이프라인에서 기록·관찰하면 좋은 지표**

- 실행 성공/실패, 실행 시간, 재시도 횟수
- 읽은 행 수 / 쓴 행 수 / 버려진(격리된) 행 수
- 데이터 신선도(마지막 성공 적재 시각)
- 품질 검사 통과율
- 비용 관련 지표(처리 데이터량, 클러스터 사용 시간)

**좋은 로그의 예**: 실행 구간, 입력·출력 건수, 소요 시간을 구조화된 형태로 남깁니다.

```python
logger.info(
    "load_done",
    extra={"table": "orders", "interval_start": str(start), "rows_in": n_in,
           "rows_upserted": n_out, "rows_rejected": n_bad, "elapsed_sec": round(elapsed, 2)},
)
```

Grafana, CloudWatch, ELK Stack 등은 인프라와 애플리케이션 관측에 활용할 수 있습니다. Monte Carlo와 Bigeye는 데이터 관측성 플랫폼의 예시입니다. 모든 상용 도구를 직접 사용해야 취업할 수 있는 것은 아닙니다. **알림 피로(alert fatigue)** 를 피하기 위해 정말 조치가 필요한 항목만 알림으로 설정하는 것이 중요합니다.

---

## 11. Data Contract, Schema Evolution, Data Versioning, Feature Store

- **Data Contract**: 데이터 생산자와 소비자가 스키마, 의미, 품질, 전달 조건(지연·주기) 등을 합의하는 계약입니다.
- **Schema Evolution**: 시간이 지나면서 컬럼이나 데이터 구조가 변경되는 것을 관리하는 방법입니다.
- **Data Versioning**: 데이터 또는 데이터 산출물의 버전을 식별하고 추적하는 접근입니다.
- **Feature Store**: 머신러닝 모델에서 사용하는 특성(Feature)을 정의·저장·공유하고, 학습과 추론 간 일관성을 지원하는 시스템입니다.

### 11.1 스키마 변경 호환성

| 변경 유형 | 하위 호환성(기존 소비자가 계속 동작?) | 비고 |
|---|---|---|
| 선택적(nullable/기본값 있는) 컬럼 추가 | 대체로 안전 | 소비자가 `SELECT *`를 쓰면 영향 가능 |
| 컬럼 삭제 | 깨질 수 있음 | 사전 공지·단계적 제거(deprecate) |
| 컬럼 이름 변경 | 깨질 수 있음 | 새 컬럼 추가 → 이전 컬럼 병행 → 제거 |
| 타입 변경 | 깨질 수 있음 | 호환 가능한 확장(예: INT → BIGINT)은 상대적으로 안전 |
| 의미 변경(같은 이름, 다른 의미) | **가장 위험** (오류 없이 잘못된 결과) | 계약·문서·리뷰 필요 |

Avro/Protobuf 같은 포맷과 Schema Registry는 **Backward / Forward / Full 호환성** 규칙으로 변경을 검사할 수 있습니다.

### 11.2 Data Contract 예시(개념)

```yaml
dataset: orders
owner: commerce-data-team
schema:
  - { name: order_id,   type: bigint,        nullable: false, unique: true }
  - { name: amount,     type: decimal(12,2), nullable: false, constraint: ">= 0" }
  - { name: status,     type: string,        allowed: [paid, cancelled, refunded] }
sla:
  freshness: "매일 06:00까지 전일 데이터 반영"
  completeness: "행 수가 전일 대비 ±30% 이내"
change_policy: "컬럼 삭제·이름 변경 시 최소 2주 전 공지"
```

이 개념들은 데이터 플랫폼의 성숙도가 높아질수록 중요해집니다. 입문 단계에서는 용어를 모두 구현하기보다, **스키마 변경이 파이프라인과 소비자에게 어떤 영향을 주는지**부터 이해하는 것이 좋습니다.

### 11.3 Data Versioning과 Feature Store

- **버전 관리 방법 예**: 불변 파일 + 날짜/실행 ID 기반 경로, 오픈 테이블 포맷의 스냅샷(타임 트래블), 모델 학습에 쓴 데이터셋 버전 기록
- **Feature Store 핵심 문제**: 학습 시와 서빙(추론) 시 **같은 방식으로 계산한 특성**을 쓰지 않으면 **training-serving skew**가 생김. 또한 학습 데이터를 만들 때 미래 정보가 섞이지 않도록 **시점 기준(point-in-time) 조인**이 필요합니다.

---

## 12. 클라우드 데이터 서비스

### 12.1 AWS 예시

| 서비스 | 주요 역할 | 알아둘 점 |
|---|---|---|
| S3 | 객체 스토리지 | 레이크의 기본 저장소. 버킷 정책, 수명주기, 암호화 설정 |
| Glue | 데이터 통합·ETL 및 카탈로그(Data Catalog) | 크롤러/카탈로그로 테이블 메타데이터 관리 |
| Athena | S3 등의 데이터를 SQL로 조회하는 서버리스 분석 서비스 | 일반적으로 **스캔한 데이터량 기준 과금** → Parquet·파티셔닝으로 비용·시간 절감 |
| Lambda | 이벤트 기반 코드 실행 | 실행 시간·메모리 제한 확인, 가벼운 이벤트 처리에 적합 |
| Kinesis | 스트리밍 데이터 수집·처리 서비스 계열 | 샤드(처리 단위) 개념 |
| EMR | Spark 등 빅데이터 프레임워크를 실행하는 관리형 환경 | 클러스터 크기·종료 정책이 비용 좌우 |
| IAM | 접근 권한 관리 | **최소 권한 원칙**, 액세스 키를 코드에 넣지 않기 |

### 12.2 GCP 예시

| 서비스 | 주요 역할 | 알아둘 점 |
|---|---|---|
| BigQuery | 관리형 데이터 웨어하우스·분석 플랫폼 | 스토리지와 컴퓨트 분리. 과금 모델(온디맨드/예약 용량)을 확인. 파티셔닝·클러스터링으로 스캔량 절감 |
| Dataflow | Apache Beam 기반 데이터 처리 서비스 | 배치·스트리밍을 같은 프로그래밍 모델로 |
| Pub/Sub | 메시지·이벤트 전달 서비스 | 구독 기반 전달 |

### 12.3 한 클라우드에서 완성할 흐름 예시 (AWS)

```text
Python 수집기 ──> S3 (raw, JSON)
                   │
                   ▼
        Glue Job / Python(Parquet 변환)
                   │
                   ▼
        S3 (silver/gold, Parquet, 날짜 파티션)
                   │
                   ▼
   Glue Data Catalog ──> Athena (SQL 조회) ──> 대시보드
```

AWS와 GCP를 동시에 모두 숙련할 필요는 없습니다. 한 클라우드에서 수집, 저장, 변환, 조회, 스케줄링의 흐름을 완성하고 다른 클라우드 서비스와 개념을 비교하는 것이 현실적인 출발점입니다.

### 12.4 클라우드 실습 시 주의사항

- **비용 알림(budget alert)** 을 먼저 설정하고, 실습 후 리소스를 삭제하거나 자동 종료
- 무료 이용 한도와 조건은 시점·계정에 따라 다르므로 공식 안내 확인
- 공개 저장소(GitHub 등)에 **액세스 키·비밀번호를 올리지 않기** (유출 시 즉시 폐기·재발급)
- 퍼블릭 버킷 설정 주의

---

## 13. 인프라, 배포, 자동화

### 13.1 Docker, Kubernetes, Terraform

- **Docker**: 애플리케이션과 실행 환경을 이미지로 패키징합니다.
- **Kubernetes**: 컨테이너화된 워크로드를 배포·확장·관리합니다.
- **Terraform**: 코드로 클라우드 및 인프라 리소스를 선언하고 관리합니다(IaC, Infrastructure as Code).

신입은 먼저 Docker Compose 등을 이용해 데이터베이스, 파이프라인, 관련 서비스를 로컬에서 함께 실행해 보는 것이 좋습니다. Kubernetes와 Terraform은 기초 파이프라인을 만든 뒤 학습해도 됩니다.

#### 예시: 로컬 개발용 Docker Compose (PostgreSQL)

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${POSTGRES_USER}          # .env 파일에서 주입 (저장소에는 .env.example만 커밋)
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: warehouse
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data         # 컨테이너를 지워도 데이터 유지
      - ./sql/init:/docker-entrypoint-initdb.d  # 초기 DDL 자동 실행
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d warehouse"]
      interval: 5s
      retries: 10

volumes:
  pgdata:
```

#### 이미지 작성 시 기본 사항

- 고정된 베이스 이미지 태그 사용(`python:3.12-slim` 등), 의존성 버전 고정
- 비밀정보를 이미지에 굽지 않기(빌드 인자·레이어에 남음)
- 불필요한 파일은 `.dockerignore`로 제외, 가능한 경우 비루트 사용자로 실행

### 13.2 CI/CD와 GitOps

- **CI(Continuous Integration)**: 코드 변경 시 테스트와 검증을 자동으로 수행합니다.
- **CD(Continuous Delivery/Deployment)**: 검증된 변경 사항을 배포 가능한 상태로 만들거나 자동 배포합니다.
- **GitOps**: Git에 선언된 원하는 상태를 기준으로 인프라나 애플리케이션 상태를 관리하는 접근입니다.

#### 데이터 프로젝트용 CI 예시 (GitHub Actions)

```yaml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install -r requirements.txt
      - run: ruff check .          # 린트
      - run: pytest -q             # 단위 테스트
      # 필요 시: DAG import 오류 검사, SQL 린트(sqlfluff), dbt parse/compile
```

포트폴리오에서는 코드 저장소에 테스트 절차와 실행 방법을 명시하고, 최소한의 자동 테스트를 구성해도 좋은 평가 근거가 될 수 있습니다.

### 13.3 데이터 파이프라인 테스트 전략

| 테스트 유형 | 대상 | 예 |
|---|---|---|
| 단위 테스트 | 변환 함수 | 정상/경계/결측 입력에 대한 출력 검증 |
| 통합 테스트 | 수집→적재 흐름 | 컨테이너 DB를 띄워 소량 데이터로 end-to-end 실행 |
| **멱등성 테스트** | 같은 입력으로 2회 실행 | 결과 행 수와 값이 변하지 않는지 |
| 데이터 테스트 | 실제 산출 데이터 | 10장의 품질 검사 규칙 |
| DAG 무결성 테스트 | 오케스트레이션 정의 | DAG import 성공, 순환 의존 없음 |

---

## 14. 포트폴리오 프로젝트 예시

### 프로젝트: 공개 데이터 기반 배치 파이프라인

**목표:** 공개 API에서 데이터를 수집해 정제·검증한 뒤 분석용 테이블로 적재하고, 실행 상태와 품질 결과를 확인할 수 있게 합니다.

```text
공개 API
   |
   v
Python 수집기
   |
   v
원본 파일(S3 또는 로컬 저장소)
   |
   v
Airflow DAG
   |
   v
정제 및 변환
   |
   v
품질 검사
   |
   v
PostgreSQL 또는 분석 저장소
   |
   v
SQL 분석 / 대시보드
```

### 필수 구현 범위

1. API 페이지네이션과 오류 처리
2. 원본 데이터 보존 (Bronze)
3. 증분 적재 또는 중복 방지 (UPSERT/파티션 덮어쓰기)
4. Airflow 기반 스케줄링
5. 데이터 품질 검사
6. Retry와 Timeout 설정
7. 로그 및 성공·실패 상태 기록
8. Docker 기반 실행 환경
9. README, 아키텍처 다이어그램, 데이터 모델 문서
10. 테스트 데이터와 재현 가능한 실행 방법

### 확장 과제

- Parquet 저장과 파티셔닝
- dbt 기반 변환 및 테스트
- Spark를 이용한 대용량 처리
- Kafka를 이용한 이벤트 스트리밍
- Grafana 또는 CloudWatch 기반 모니터링
- 데이터 계보와 메타데이터 관리

모든 확장 과제를 한 번에 수행하기보다 핵심 파이프라인의 안정성을 먼저 확보한 후 하나씩 추가합니다.

### 프로젝트 완성도를 높이는 점검표

**설계 문서(README)에 포함할 내용**

- [ ] 문제 정의와 데이터 출처(라이선스·이용 약관 확인)
- [ ] 아키텍처 다이어그램과 기술 선택 이유(왜 Airflow? 왜 PostgreSQL?)
- [ ] 데이터 모델(ERD 또는 스타 스키마)과 **그레인** 정의
- [ ] 증분/멱등 전략(어떤 키로, 어떻게 중복을 막는가)
- [ ] 품질 검사 규칙 목록과 실패 시 정책
- [ ] 장애 시나리오와 복구 절차(3.5 표 참고)
- [ ] 실행 방법(한 줄 명령으로 재현: `docker compose up` 등)과 테스트 방법
- [ ] 한계와 개선 계획(예: "삭제된 행은 감지하지 못함 → CDC 도입 검토")

**실제로 검증해 보여줄 것**

- [ ] 같은 구간을 두 번 실행해도 행 수가 늘지 않음
- [ ] 중간에 작업을 강제로 실패시킨 후 재실행해 복구됨
- [ ] 의도적으로 잘못된 데이터(NULL, 음수, 중복)를 넣으면 품질 검사가 탐지함
- [ ] 쿼리 성능을 `EXPLAIN`으로 확인하고 인덱스 추가 전후를 비교

---

## 15. 8주 학습 계획 예시

| 주차 | 목표 | 결과물 |
|---|---|---|
| 1주 | Python, SQL, Git 복습 | API 수집 스크립트와 SQL 쿼리 (윈도 함수·CTE 연습 포함) |
| 2주 | 데이터 모델링과 RDBMS 적재 | 테이블 설계, 중복 방지(UPSERT) 로직, 인덱스·`EXPLAIN` 실습 |
| 3주 | ETL 및 품질 검사 | 재현 가능한 수집·변환·검증 코드, 단위 테스트 |
| 4주 | Airflow DAG | 예약 실행, 작업 의존성, 재시도·타임아웃, 백필 실습 |
| 5주 | Docker와 테스트 자동화 | 컨테이너 실행 환경(Compose), CI에서 자동 테스트 |
| 6주 | 클라우드 저장·조회 서비스 | S3/Glue/Athena 등 한 가지 흐름, 비용 알림 설정 |
| 7주 | 로그·재시도·장애 복구 | 오류 시나리오 재현과 복구 검증, 멱등성 테스트 |
| 8주 | 문서화와 발표 준비 | README, 아키텍처, 기술 선택 근거, 면접 질문 답변 정리 |

이 계획은 주당 학습 시간과 기존 경험에 따라 조정해야 하는 예시입니다. 이미 숙련된 항목은 줄이고 부족한 기본기에 시간을 더 배분합니다. 8주 이후에는 dbt, Spark/Polars, Kafka 중 **하나씩** 선택해 확장 과제로 추가하는 것을 권합니다.

---

## 16. 신입 면접에서 설명할 수 있어야 할 질문

각 질문에 대해 **"정의 → 내 프로젝트에서의 선택 → 근거와 한계"** 순서로 답하는 연습을 해 보세요.

| 질문 | 답변에 포함하면 좋은 핵심 |
|---|---|
| 배치 처리와 스트리밍 처리는 어떤 상황에서 각각 적합한가? | 지연 허용 범위, 복잡도, 재처리 용이성, 비용 |
| 전체 적재와 증분 적재의 차이는 무엇인가? | 기준 컬럼, 삭제 감지 문제, lookback, 백필 |
| 같은 작업을 재실행해도 중복이 생기지 않도록 어떻게 설계했는가? | 고유 키 + UPSERT, 파티션 덮어쓰기, 스테이징 교체, 멱등성 테스트 |
| 파이프라인의 한 단계가 실패하면 어디서부터 어떻게 복구하는가? | 원본 보존, 태스크 단위 재실행, 트랜잭션/스테이징, 알림 |
| 데이터 품질을 어떤 규칙으로 검증했는가? | NULL/중복/범위/참조/볼륨 검사, 실패 시 차단·격리 정책 |
| SQL 쿼리가 느릴 때 어떤 순서로 원인을 확인하는가? | `EXPLAIN (ANALYZE)`, 풀스캔 여부, 인덱스·통계, 조인 순서, 불필요한 컬럼·행 |
| Spark를 사용해야 할 이유가 있었는가? Pandas나 Polars로 충분하지 않았는가? | 데이터 크기, 메모리, 처리 시간, 운영 복잡도 |
| 클라우드 비용을 줄이기 위해 어떤 선택을 했는가? | 컬럼 지향 포맷·파티셔닝으로 스캔량 감소, 자동 종료, 적정 사이징 |
| 스키마가 변경되면 하위 테이블과 소비자에게 어떤 영향이 있는가? | 호환 가능/불가능 변경 구분, 계약, 단계적 제거 |
| 프로젝트에서 본인이 직접 설계하고 구현한 부분은 무엇인가? | 구체적 기여와 의사결정 근거 |

**추가로 자주 나오는 기술 질문**

- OLTP와 OLAP의 차이, 행 지향과 열 지향 저장의 차이
- `INNER JOIN`과 `LEFT JOIN`의 차이, `WHERE`와 `HAVING`의 차이
- `ROW_NUMBER`, `RANK`, `DENSE_RANK`의 차이
- 인덱스가 오히려 성능을 떨어뜨리는 경우
- Star Schema에서 Fact와 Dimension, 그레인이란 무엇인가
- SCD Type 2는 어떻게 구현하는가
- Kafka에서 순서는 어디까지 보장되는가 (파티션 단위)
- Spark에서 셔플이란 무엇이고 어떻게 줄이는가
- 이벤트 시간과 처리 시간의 차이, 늦게 도착한 데이터는 어떻게 다루는가

질문에 대해 용어를 정의하는 데 그치지 않고, 본인 프로젝트의 실제 선택과 결과를 근거로 설명하는 연습이 중요합니다.

---

## 17. 우선순위 정리

### 반드시 먼저 갖출 기반 [필수]

- Python 기초와 코드 구조화, 테스트
- SQL(윈도 함수, 조인, CTE), RDBMS, 데이터 모델링, 인덱스·실행 계획 기초
- Git과 Linux 기초
- ETL 파이프라인 하나를 끝까지 구현
- Airflow 등 오케스트레이션 도구 하나
- 데이터 품질 검사, 로깅, 재시도와 **멱등성(중복 방지)**
- 완성도 있는 프로젝트와 문서화

### 기본 프로젝트 이후 확장할 기술 [권장]

- Docker와 클라우드 서비스 하나
- Spark/PySpark 또는 Polars
- 데이터 웨어하우스(차원) 모델링과 dbt
- 파일 포맷(Parquet)과 파티셔닝
- 모니터링 및 데이터 계보
- Kafka/Flink 기반 스트리밍

### 진로와 프로젝트에 따라 선택할 심화 주제 [선택]

- Kubernetes, Terraform, GitOps
- CDC와 Event Sourcing
- Lambda/Kappa Architecture
- Iceberg, Delta Lake, Hudi
- Data Mesh, Data Contract, Feature Store
- OpenMetadata, DataHub, 상용 Data Observability 도구
- MongoDB, Cassandra 등 NoSQL
- 여러 클라우드 플랫폼의 심화 운영

---

## 18. 결론

데이터 엔지니어링 분야의 기술 범위는 넓지만, 신입 취업을 위해 모든 도구를 미리 마스터해야 하는 것은 아닙니다. 핵심은 **기본기를 갖추고, 작동하는 데이터 파이프라인을 직접 만들며, 데이터 품질과 장애 복구까지 설명할 수 있는 것**입니다.

기술 목록을 넓게 훑는 것보다 하나의 프로젝트를 끝까지 완성하고, 설계 이유와 한계를 명확히 설명하는 경험이 더 강한 경쟁력이 됩니다. 채용 요건과 보상 수준은 회사·지역·직무·시장 상황에 따라 다르므로, 특정 월급이나 취업 가능성을 보장하는 기준으로 받아들이기보다 실제 공고와 비교해 준비 계획을 세우는 것이 좋습니다.

---

## 부록: 핵심 용어 빠른 참조

| 용어 | 한 줄 설명 |
|---|---|
| 멱등성(Idempotency) | 같은 작업을 반복 실행해도 결과가 같은 성질 |
| 백필(Backfill) | 과거 구간 데이터를 다시 처리하는 것 |
| 고수위선(High-water mark) | 마지막으로 처리한 위치(시각/ID). 증분 처리의 기준점 |
| 그레인(Grain) | Fact 테이블 한 행이 나타내는 단위 |
| 대리키(Surrogate key) | 원천 키와 무관하게 웨어하우스에서 부여하는 키 |
| 셔플(Shuffle) | 분산 처리에서 노드 간 데이터 재분배 |
| 파티션 프루닝 | 조건에 맞지 않는 파티션을 읽지 않고 건너뛰는 최적화 |
| 컬럼 프루닝 | 필요한 컬럼만 읽는 최적화 |
| Watermark | 스트림 처리에서 이벤트 시간 진행도를 나타내는 표시 |
| Dead-letter / Quarantine | 처리 실패·검증 실패 레코드를 별도로 보관하는 저장소 |
| 계보(Lineage) | 데이터의 출처와 변환 경로 |
| 데이터 계약(Data Contract) | 생산자·소비자 간 스키마·품질·전달 조건에 대한 합의 |

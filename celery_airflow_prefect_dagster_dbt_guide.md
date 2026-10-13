# Celery · Airflow · Prefect · Dagster · dbt 비교와 사용법 가이드

> **버전 안내**
> 이 문서의 코드와 CLI는 개념과 사용 흐름을 설명하기 위한 예시입니다. 다섯 도구 모두 버전에 따라 API, 설정, 기본값, 권장 방식이 바뀝니다(특히 Airflow 2 → 3, Prefect 2 → 3, Dagster의 프로젝트 구조와 CLI). 실제 적용 전에 사용하는 버전의 공식 문서로 확인하세요.

## 목차

1. [가장 먼저 이해할 것: 이 도구들은 같은 종류가 아니다](#1-가장-먼저-이해할-것-이-도구들은-같은-종류가-아니다)
2. [한눈에 보는 비교표](#2-한눈에-보는-비교표)
3. [Celery](#3-celery)
4. [Apache Airflow](#4-apache-airflow)
5. [Prefect](#5-prefect)
6. [Dagster](#6-dagster)
7. [dbt](#7-dbt)
8. [같은 파이프라인을 세 오케스트레이터로 비교](#8-같은-파이프라인을-세-오케스트레이터로-비교)
9. [조합 패턴: 실무에서는 어떻게 함께 쓰는가](#9-조합-패턴-실무에서는-어떻게-함께-쓰는가)
10. [선택 가이드](#10-선택-가이드)
11. [신입을 위한 학습 순서](#11-신입을-위한-학습-순서)
12. [면접 대비 질문](#12-면접-대비-질문)
13. [공통 실수와 점검표](#13-공통-실수와-점검표)

---

## 1. 가장 먼저 이해할 것: 이 도구들은 같은 종류가 아니다

다섯 도구를 "경쟁 관계"로 비교하면 혼란스럽습니다. 역할이 다르기 때문입니다.

| 도구 | 한 줄 정의 | 분류 |
|---|---|---|
| **Celery** | 메시지 브로커를 통해 작업을 워커에게 분산 실행시키는 **분산 태스크 큐** | 애플리케이션 비동기 처리 |
| **Airflow** | 작업의 순서·일정·재시도를 관리하는 **워크플로 오케스트레이터** (스케줄링 중심) | 오케스트레이션 |
| **Prefect** | Python 코드를 거의 그대로 워크플로로 만드는 **오케스트레이터** (코드 중심, 동적 처리에 유연) | 오케스트레이션 |
| **Dagster** | 데이터 **자산(Asset)** 중심으로 정의하는 **오케스트레이터** (계보·품질 통합) | 오케스트레이션 |
| **dbt** | 웨어하우스 안에서 **SQL 변환 모델**을 관리·테스트하는 도구 (ELT의 T) | 변환(Transformation) |

### 계층 관점

```text
┌──────────────────────────────────────────────────────────────┐
│  오케스트레이션 (언제, 어떤 순서로, 실패하면 어떻게?)          │
│  Airflow / Prefect / Dagster                                  │
│        │ 호출               │ 호출                │ 호출       │
│        ▼                    ▼                     ▼            │
│  Python 수집 코드     dbt (SQL 변환·테스트)    Spark 작업 등    │
│                                                               │
│  (분산 실행 계층: Celery는 Airflow의 Executor 등으로도 쓰임)   │
└──────────────────────────────────────────────────────────────┘
```

핵심 구분:

- **dbt는 스케줄러가 아닙니다.** dbt Core는 SQL을 실행·테스트할 뿐이고, "매일 2시에 실행"은 외부 오케스트레이터(또는 dbt의 상용 클라우드 서비스)가 맡습니다.
- **Celery는 데이터 파이프라인 오케스트레이터가 아닙니다.** 웹 애플리케이션에서 "이메일 발송", "리포트 생성" 같은 비동기 작업을 처리하는 데 주로 쓰이며, 백필·데이터 구간(data interval)·계보 같은 데이터 엔지니어링 기능은 기본 제공하지 않습니다.
- **Airflow / Prefect / Dagster는 서로 대체 가능한 후보**입니다. 한 프로젝트에서 보통 하나를 선택합니다.
- 실무에서는 **오케스트레이터 + dbt** 조합이 흔합니다.

---

## 2. 한눈에 보는 비교표

### 2.1 기본 특성

| 항목 | Celery | Airflow | Prefect | Dagster | dbt |
|---|---|---|---|---|---|
| 주 목적 | 비동기 태스크 큐 | 배치 워크플로 스케줄링 | 코드 중심 워크플로 | 자산 중심 데이터 오케스트레이션 | SQL 변환·테스트·문서화 |
| 중심 모델 | Task (메시지) | DAG / Task | Flow / Task | Asset (+ Op/Job) | Model (SELECT 문) |
| 의존성 정의 | `chain`, `group`, `chord` | `a >> b` 또는 함수 호출 | 함수 호출 순서 / `.submit()` | 함수 인자(자산 이름) | `ref()` / `source()` |
| 스케줄링 | Celery Beat(주기 작업) | 내장(핵심 기능) | Deployment 스케줄 | Schedule / Sensor | 없음 (외부 도구 필요) |
| 백필(과거 재처리) | 기본 없음(직접 구현) | 강력함 (data interval 기반) | 직접 구성 / 파라미터 기반 | 파티션 기반으로 강력 | `--full-refresh`, 증분 모델 |
| 데이터 계보 | 없음 | 제한적 (Asset 개념으로 확장 중) | 제한적 | **기본 제공(Asset 그래프)** | **모델 단위 계보(DAG, docs)** |
| 데이터 품질 검사 | 없음 | 별도 구성 | 별도 구성 | Asset Check | **내장 테스트** |
| 웹 UI | 기본 없음 (Flower 등 별도) | 있음 | 있음 | 있음 (자산 그래프 중심) | 문서 사이트(`dbt docs`) |
| 로컬 개발 편의성 | 브로커 필요 | 중간 (구성 요소 많음) | **높음** (일반 Python 실행) | 높음 | 높음 (DB 연결 필요) |
| 동적 워크플로 | 가능 (코드로 호출) | Dynamic Task Mapping | **매우 유연** | 동적 파티션 등 | 제한적 (Jinja 매크로) |
| 생태계·자료·채용 수요 | 매우 넓음(웹 백엔드) | **데이터 분야에서 가장 큼** | 중간 | 성장 중 | 분석 엔지니어링에서 매우 큼 |
| 러닝커브 | 낮음~중간 | 중간~높음 | 낮음 | 중간 | 낮음 (SQL 숙련 전제) |

### 2.2 "누가 무엇을 하는가" 매트릭스

| 하고 싶은 일 | Celery | Airflow | Prefect | Dagster | dbt |
|---|:-:|:-:|:-:|:-:|:-:|
| API/웹 요청 중 비동기 작업 실행 | ◎ | △ | ○ | △ | ✕ |
| 매일 새벽 배치 스케줄 실행 | △ (Beat) | ◎ | ◎ | ◎ | ✕ |
| 작업 간 의존성·재시도 관리 | ○ | ◎ | ◎ | ◎ | △ (모델 의존성만) |
| SQL 변환 로직 관리·테스트 | ✕ | ✕ | ✕ | ✕ | ◎ |
| 외부 API에서 데이터 수집(코드 실행) | ○ | ◎ | ◎ | ◎ | ✕ |
| 데이터 자산 계보·상태 가시화 | ✕ | △ | △ | ◎ | ○ |
| 과거 구간 백필 | ✕ | ◎ | ○ | ◎ | ○ |

(◎ 핵심 강점 · ○ 가능 · △ 제한적/보조적 · ✕ 목적이 아님)

---

## 3. Celery

### 3.1 개념

Celery는 Python의 **분산 태스크 큐**입니다. 호출자는 작업을 "메시지"로 브로커에 넣고, 별도 프로세스인 **워커**가 이를 꺼내 실행합니다. 호출자와 실행자가 분리되므로 시간이 오래 걸리는 작업을 요청 흐름에서 떼어낼 수 있습니다.

```text
 Producer (웹 앱, 스크립트)
        │  task.delay(...)  → 메시지 전송
        ▼
   Broker (Redis / RabbitMQ)  ← 큐
        │
        ▼
   Worker 1, Worker 2, ...  (태스크 실행)
        │
        ▼
 Result Backend (Redis / DB 등) ← 결과 저장 (선택)
```

| 구성 요소 | 역할 |
|---|---|
| **Broker** | 태스크 메시지를 전달하는 중개자. Redis 또는 RabbitMQ를 주로 사용 |
| **Worker** | 큐에서 태스크를 가져와 실행하는 프로세스 |
| **Result Backend** | 태스크 결과·상태 저장 (필요할 때만 사용) |
| **Beat** | 주기적 작업을 스케줄링해 브로커로 보내는 프로세스 |
| **Canvas** | `chain`(순차), `group`(병렬), `chord`(병렬 후 취합) 등 태스크 조합 기능 |
| **Flower** | Celery 모니터링 웹 도구(별도 패키지) |

### 3.2 설치와 기본 사용

```bash
pip install celery redis
# 브로커용 Redis 실행 (예: Docker)
docker run -d --name redis -p 6379:6379 redis:7
```

```python
# tasks.py
import requests
from celery import Celery

app = Celery(
    "proj",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,             # 작업 완료 후 ack → 워커 중단 시 재전달 가능 (멱등성 필요)
    task_time_limit=600,             # 하드 타임리밋(초)
    task_soft_time_limit=540,        # 소프트 타임리밋(예외 발생, 정리 기회 제공)
    result_expires=3600,
    worker_prefetch_multiplier=1,    # 긴 작업이 섞인 경우 공정한 분배에 유리
)


@app.task(
    bind=True,
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,              # 1s, 2s, 4s ... 지수 백오프
    retry_backoff_max=600,
    retry_jitter=True,               # 재시도 시점 분산
    max_retries=5,
)
def fetch_page(self, page: int) -> int:
    resp = requests.get("https://api.example.com/orders", params={"page": page}, timeout=30)
    resp.raise_for_status()
    # 큰 데이터는 반환하지 말고 저장소에 쓴 뒤 경로나 건수만 반환
    return len(resp.json()["items"])
```

```bash
# 워커 실행
celery -A tasks worker --loglevel=INFO --concurrency=4

# 호출 (다른 터미널 / 앱 코드에서)
python -c "from tasks import fetch_page; r = fetch_page.delay(1); print(r.get(timeout=60))"
```

### 3.3 태스크 조합 (Canvas)

```python
from celery import chain, group, chord

# 순차: extract → transform → load (앞 결과가 다음 태스크의 첫 인자로 전달됨)
chain(extract.s("2026-01-01"), transform.s(), load.s())()

# 병렬: 여러 페이지를 동시에 수집
group(fetch_page.s(p) for p in range(1, 11))()

# 병렬 후 취합: 모두 끝나면 callback 실행
chord(group(fetch_page.s(p) for p in range(1, 11)))(summarize.s())
```

### 3.4 주기 작업 (Celery Beat)

```python
from celery.schedules import crontab

app.conf.beat_schedule = {
    "daily-report": {
        "task": "tasks.make_daily_report",
        "schedule": crontab(hour=2, minute=0),   # 매일 02:00 (설정한 timezone 기준)
    },
}
```

```bash
celery -A tasks beat --loglevel=INFO
```

> Beat 프로세스는 **하나만** 실행해야 합니다. 여러 개를 띄우면 같은 작업이 중복 발행됩니다.

### 3.5 데이터 엔지니어링 관점의 장단점

**장점**

- 설치와 개념이 단순하고, 웹 백엔드(Django, FastAPI 등)와의 결합 사례가 매우 많음
- 낮은 지연으로 많은 작은 작업을 빠르게 분산 처리
- 워커 수평 확장이 쉬움
- Airflow의 Executor로도 사용 가능(CeleryExecutor 등)

**단점 / 한계**

- 워크플로 단위의 **실행 이력, 구간(data interval), 백필, 계보**가 기본 제공되지 않음
- 작업 간 의존성 관리가 Canvas 수준이라 복잡한 DAG 운영에는 부담
- 기본 UI가 없어 Flower 등을 별도 구성해야 함
- 브로커 설정에 따라 **중복 실행·유실 가능성**을 이해해야 함

### 3.6 자주 하는 실수와 주의점

| 실수 | 설명 / 대응 |
|---|---|
| 비멱등 태스크 + `acks_late` | 워커 중단 시 같은 태스크가 다시 실행될 수 있음. 태스크를 **멱등하게** 설계 |
| Redis 브로커의 visibility timeout | ack 전 태스크가 이 시간을 넘기면 다른 워커에 재전달될 수 있음. 긴 작업·ETA 태스크는 설정 확인 |
| 큰 데이터를 인자/반환값으로 전달 | 메시지와 결과 백엔드 부담. 저장소에 쓰고 **경로만 전달** |
| Pickle 직렬화 | 보안 위험. JSON 사용 권장 |
| Beat 여러 개 실행 | 중복 스케줄 발생 |
| 결과 백엔드 무조건 사용 | 필요 없으면 `ignore_result=True` 로 부하 감소 |
| 태스크 시간 제한 없음 | 무한 대기 방지를 위해 time limit 설정 |

### 3.7 언제 선택하는가

- 웹 서비스의 비동기 작업(이메일, 알림, 이미지 처리, 리포트 생성)
- 이벤트 기반으로 수많은 짧은 작업을 분산 처리
- **데이터 파이프라인의 스케줄·백필·계보가 핵심 요구라면 Celery 단독 사용은 권하지 않음** → Airflow/Prefect/Dagster 사용

---

## 4. Apache Airflow

### 4.1 개념

Airflow는 워크플로를 **Python 코드로 정의한 DAG**로 관리하고, 스케줄에 따라 실행하며 의존성·재시도·이력을 관리하는 오케스트레이터입니다. 배치 데이터 파이프라인 분야에서 가장 널리 쓰이는 도구입니다.

```text
          ┌──────────────┐
DAG 파일 ─▶│ DAG Processor │ (파일 파싱)
(Python)  └──────┬───────┘
                 ▼
          ┌──────────────┐   상태 저장   ┌───────────────┐
          │  Scheduler   │◀────────────▶│ Metadata DB   │
          └──────┬───────┘               └───────────────┘
                 │ 작업 할당
                 ▼
          ┌──────────────┐
          │  Executor     │ → Local / Celery / Kubernetes ...
          └──────┬───────┘
                 ▼
             Workers (작업 실제 실행)

   Web UI / API Server (조회·수동 실행·로그)
```

| 용어 | 설명 |
|---|---|
| **DAG** | 작업과 의존 관계를 정의한 워크플로(순환 없음) |
| **Task / Operator** | 개별 작업 단위 / 작업을 정의하는 클래스 (`BashOperator`, SQL 실행 Operator 등) |
| **TaskFlow API** | `@task` 데코레이터로 Python 함수를 태스크로 만드는 방식 |
| **Scheduler** | 일정과 의존성을 판단해 실행할 작업을 결정 |
| **Executor** | 작업을 어디서·어떻게 실행할지 결정(Local, Celery, Kubernetes 등) |
| **Metadata DB** | DAG 실행 이력과 상태 저장 (PostgreSQL 등) |
| **Data interval** | 해당 실행이 처리하는 데이터 구간 (시작~끝). 멱등한 파라미터화의 핵심 |
| **Connection / Variable** | 외부 시스템 접속 정보 / 설정값 (비밀정보는 Secrets Backend 활용) |
| **XCom** | 태스크 간 **작은** 데이터 전달 수단 |
| **Sensor** | 파일 도착 등 외부 조건을 기다리는 태스크 |
| **Provider** | AWS, GCP, dbt 등 외부 서비스 연동 패키지 |
| **Pool** | 동시 실행 제한 (외부 API·DB 보호) |

> **Airflow 2와 3의 차이 (개념 수준)**
> Airflow 3에서는 DAG 작성용 공개 인터페이스가 `airflow.sdk`로 정리되었고, 웹 서버 구성이 API 서버 중심으로 바뀌었으며, DAG 버전 관리, 이벤트 기반 스케줄링(Asset 개념), 백필 기능 개선 등이 도입되었습니다. 회사에서 쓰는 버전이 2.x일 수 있으므로 공고와 환경을 확인하세요. 아래 예시는 import 경로만 다르고 개념은 같습니다.

### 4.2 설치와 로컬 실행

```bash
# 방법 1: 로컬 학습용 (공식 문서의 설치 안내에서 constraints 파일 사용을 권장)
pip install "apache-airflow==<버전>" --constraint "<공식 constraints URL>"
airflow standalone        # 메타 DB 초기화, 계정 생성, 주요 구성 요소를 한 번에 실행 (학습용)

# 방법 2: 공식 문서의 docker-compose 예시로 실행 (여러 구성 요소를 컨테이너로)
```

> `airflow standalone`은 학습·개발용입니다. 운영 환경에서는 PostgreSQL 같은 외부 메타 DB와 분리된 구성 요소, 적절한 Executor를 사용합니다.

### 4.3 DAG 작성: 일 배치 예시

```python
from datetime import datetime, timedelta

# Airflow 3
from airflow.sdk import dag, task
# Airflow 2.x 에서는: from airflow.decorators import dag, task

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="orders_daily",
    schedule="0 2 * * *",              # 매일 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,                      # 과거 구간 자동 실행 여부 (백필은 명시적으로)
    max_active_runs=1,
    default_args=default_args,
    tags=["orders"],
)
def orders_daily():

    @task
    def extract(data_interval_start=None) -> str:
        # 구간 값을 이용해 같은 구간은 항상 같은 경로에 저장 (멱등)
        path = f"raw/orders/dt={data_interval_start:%Y-%m-%d}/data.json"
        # ... API 호출 후 path 에 저장 ...
        return path                      # 큰 데이터 대신 경로만 전달 (XCom)

    @task
    def transform(raw_path: str) -> str:
        # ... 정제 후 staging 경로 반환 ...
        return raw_path.replace("raw", "staging")

    @task
    def validate(staging_path: str) -> str:
        # 품질 검사 실패 시 예외 → 적재 단계로 넘어가지 않음
        return staging_path

    @task
    def load(staging_path: str) -> None:
        # UPSERT 또는 파티션 덮어쓰기
        ...

    load(validate(transform(extract())))


orders_daily()
```

> 수동 트리거 실행 시 data interval 값이 어떻게 채워지는지는 버전별로 다를 수 있으므로 사용하는 버전의 문서를 확인하세요.

### 4.4 자주 쓰는 기능

**의존성 표현**

```python
extract_task >> transform_task >> [load_a, load_b] >> notify_task
```

**동적 태스크 매핑**: 런타임에 결정되는 개수만큼 태스크 생성

```python
@task
def list_files() -> list[str]:
    return ["a.csv", "b.csv", "c.csv"]

@task
def process(path: str) -> int:
    ...

process.expand(path=list_files())     # 파일 개수만큼 병렬 태스크 생성
```

**분기와 트리거 규칙**

```python
@task.branch
def choose(**context):
    return "full_load" if is_first_run() else "incremental_load"
```

- `trigger_rule`로 앞 작업의 성공·실패 조합에 따라 실행 조건 지정 (예: 실패 시에도 정리 작업 실행)

**센서**: 외부 조건 대기 (파일 도착 등). 오래 대기하는 센서는 워커를 점유하지 않도록 deferrable(비동기 대기) 방식이 있는지 확인

**CLI 예시**

```bash
airflow dags list
airflow dags test orders_daily 2026-01-01    # 스케줄러 없이 DAG 1회 테스트 실행 (개발용)
airflow tasks test orders_daily extract 2026-01-01
```

### 4.5 장단점

**장점**

- 배치 오케스트레이션의 사실상 표준: 자료, 커뮤니티, 채용 수요, Provider(연동 패키지)가 풍부
- 스케줄·재시도·백필·이력 관리가 성숙함
- 다양한 Executor와 운영 방식 (Celery, Kubernetes 등)

**단점 / 주의**

- 구성 요소가 많아 **로컬 환경과 운영 환경 구성이 무거운 편**
- 전통적으로 "작업(Task) 중심"이라 데이터 자산 관점의 계보·품질은 별도 구성이 필요
- DAG 파싱 방식 때문에 코드 작성 규칙(최상위 코드 제한)을 지켜야 함
- 이벤트·스트리밍성 워크로드에는 적합하지 않음 (배치가 주력)

### 4.6 자주 하는 실수

| 실수 | 대응 |
|---|---|
| DAG 파일 최상위에서 DB 조회·API 호출 | 파싱 때마다 실행되어 부하 유발 → 태스크 내부로 이동 |
| 작업 안에서 `datetime.now()` 사용 | 재실행 시 결과가 달라짐 → 실행 구간 값 사용 |
| XCom에 대용량 데이터 전달 | 저장소 경로만 전달 |
| 재시도만 켜고 적재는 멱등하지 않음 | UPSERT/파티션 덮어쓰기로 멱등성 확보 |
| `catchup` 의미를 모르고 기본값 사용 | 과거 구간 대량 자동 실행 가능 → 의도적으로 설정 |
| 비밀번호를 DAG 코드에 직접 작성 | Connection / Secrets Backend 사용 |

---

## 5. Prefect

### 5.1 개념

Prefect는 **일반 Python 함수에 데코레이터를 붙여** 워크플로로 만드는 오케스트레이터입니다. DAG를 별도로 선언하기보다 Python 코드의 실행 흐름 자체가 워크플로가 되고, 조건문·반복문 같은 Python 제어 흐름을 그대로 사용할 수 있어 **동적인 워크플로**에 유연합니다.

| 용어 | 설명 |
|---|---|
| **Flow** | `@flow`가 붙은 함수. 워크플로의 최상위 단위 |
| **Task** | `@task`가 붙은 함수. 재시도·캐싱·동시 실행 등의 기능을 가진 작업 단위 |
| **Deployment** | Flow를 서버(또는 Cloud)에 등록해 스케줄·트리거·원격 실행이 가능하게 만든 것 |
| **Work pool / Worker** | 배포된 Flow 실행을 어디(로컬 프로세스, Docker, Kubernetes 등)에서 할지 정의 / 실제로 실행을 가져가 수행하는 프로세스 |
| **Prefect Server / Cloud** | 실행 이력·스케줄·UI를 제공하는 API 서버(오픈소스 자체 호스팅 / 관리형 서비스) |
| **Automation / Event** | 이벤트 기반 트리거와 알림, 자동 대응 |
| **Variable / Secret** | 설정값 / 비밀정보 관리 |

핵심 특징: **서버 없이도 Flow는 일반 Python 스크립트처럼 실행**됩니다. 서버를 붙이면 스케줄, 이력, UI, 알림이 추가됩니다.

### 5.2 설치와 기본 사용

```bash
pip install prefect
prefect server start        # 로컬에서 서버와 UI 실행 (학습용), 기본 UI 주소는 터미널 안내 참고
```

```python
import httpx
from prefect import flow, task, get_run_logger


@task(retries=3, retry_delay_seconds=[10, 30, 60], timeout_seconds=120)
def extract(date: str) -> list[dict]:
    r = httpx.get("https://api.example.com/orders", params={"date": date}, timeout=30)
    r.raise_for_status()
    return r.json()["items"]


@task
def transform(items: list[dict]) -> list[dict]:
    return [{**i, "amount": float(i["amount"])} for i in items if i.get("order_id")]


@task
def load(rows: list[dict]) -> int:
    # UPSERT 로 멱등 적재
    return len(rows)


@flow(name="orders-daily", log_prints=True)
def orders_daily(date: str):
    logger = get_run_logger()
    items = extract(date)
    rows = transform(items)
    n = load(rows)
    logger.info("loaded %d rows for %s", n, date)


if __name__ == "__main__":
    orders_daily("2026-01-01")      # 그냥 Python 으로 실행해도 동작
```

### 5.3 스케줄링과 배포

**가장 간단한 방법: `serve`** — 현재 프로세스가 스케줄을 받아 실행합니다(소규모·학습용).

```python
if __name__ == "__main__":
    orders_daily.serve(
        name="orders-daily-deployment",
        cron="0 2 * * *",
        parameters={"date": "2026-01-01"},   # 실제로는 실행 시점 기준 날짜를 코드에서 계산하는 설계 권장
    )
```

**운영 방식: Work pool + Worker + `prefect deploy`**

```bash
prefect work-pool create my-pool --type process     # 실행 환경 정의 (docker, kubernetes 등 선택 가능)
prefect deploy                                       # 대화형으로 Flow 배포 정의 (prefect.yaml 생성)
prefect worker start --pool my-pool                  # 작업을 가져가 실행하는 워커
```

### 5.4 동시 실행과 동적 처리

```python
@flow
def process_many(files: list[str]):
    futures = [process_file.submit(f) for f in files]    # 동시 실행 (제출)
    results = [f.result() for f in futures]               # 결과 취합

# 또는 map
process_file.map(files)
```

```python
@flow
def conditional_flow(is_first_run: bool):
    # Python 제어 흐름을 그대로 사용
    if is_first_run:
        full_load()
    else:
        incremental_load()
```

### 5.5 장단점

**장점**

- 진입 장벽이 낮음: 기존 Python 코드에 데코레이터를 붙이는 방식, 로컬 실행·테스트가 쉬움
- 동적 워크플로(반복, 조건, 런타임 결정)를 Python으로 자연스럽게 표현
- 재시도, 타임아웃, 캐싱, 동시 실행, 이벤트 기반 자동화 기능
- 다양한 실행 환경(프로세스, Docker, Kubernetes, 서버리스 등)을 Work pool로 추상화

**단점 / 주의**

- Airflow에 비해 **채용 공고·자료·검증된 운영 사례가 적은 편**
- 자산(데이터) 중심의 계보·상태 관리는 Dagster에 비해 약함
- 배치 데이터 구간(data interval)·백필은 **파라미터 설계를 직접** 해야 함
- 메이저 버전 간(2 → 3) 개념·API 변화가 있어 오래된 튜토리얼과 최신 문서를 혼동하기 쉬움

### 5.6 자주 하는 실수

| 실수 | 대응 |
|---|---|
| 재시도 설정만으로 중복 방지 기대 | 멱등 적재 설계 필요 |
| 실행 시점 `datetime.now()` 로 데이터 구간 결정 | 날짜를 파라미터로 받아 재실행·백필 가능하게 |
| 큰 객체를 Task 간 그대로 전달 | 결과 영속화·직렬화 비용 고려, 저장소 경로 전달 |
| `serve` 를 운영 환경에서 그대로 사용 | 프로세스 종료 시 스케줄 중단. 운영은 work pool/worker 구성 검토 |
| 이전 버전 문서를 따라 함 | 사용하는 메이저 버전 확인 |

---

## 6. Dagster

### 6.1 개념

Dagster는 **"어떤 작업을 실행할까"가 아니라 "어떤 데이터 자산이 존재해야 하는가"** 를 중심에 둡니다. 테이블, 파일, ML 모델 같은 **자산(Asset)** 을 Python 함수로 정의하고, 함수 인자로 다른 자산을 받으면 의존성과 계보 그래프가 자동으로 만들어집니다.

| 용어 | 설명 |
|---|---|
| **Asset** | 파이프라인이 만들어내는 데이터 객체를 생성하는 함수 (`@dg.asset`) |
| **Materialization** | Asset을 실제로 생성/갱신한 실행. 시각·메타데이터가 기록됨 |
| **Job** | 실행할 Asset(또는 Op)의 묶음 |
| **Schedule / Sensor** | 시간 기준 / 외부 조건(파일 도착 등) 기준 실행 |
| **Partition** | 날짜 등 구간으로 Asset을 나눠 관리 (백필의 기본 단위) |
| **Resource** | DB·API 클라이언트 같은 외부 시스템 설정을 코드와 분리해 주입 |
| **IO Manager** | Asset 결과의 저장·로드 방식 |
| **Asset Check** | Asset의 품질 규칙 검사 |
| **Definitions** | 프로젝트의 Asset, Job, Schedule, Resource를 묶는 진입점 |
| **Dagster UI** | Asset 그래프, 실행 이력, 파티션 상태, 로그를 보는 웹 UI |

### 6.2 설치와 실행

```bash
pip install dagster dagster-webserver
dagster dev        # 로컬 개발 서버와 UI 실행
```

프로젝트 생성·구조화 방식과 CLI(`dg` 등)는 최근 버전에서 변화가 있었으므로 사용하는 버전의 공식 "시작하기" 문서를 기준으로 프로젝트를 만드세요.

### 6.3 기본 예시: Asset

```python
import dagster as dg


@dg.asset
def raw_orders() -> list[dict]:
    return [{"order_id": 1, "amount": 100}, {"order_id": 2, "amount": 250}]


@dg.asset
def daily_revenue(raw_orders: list[dict]) -> int:
    # 함수 인자 이름 = 의존하는 Asset 이름 → 자동으로 의존 그래프 생성
    return sum(o["amount"] for o in raw_orders)


defs = dg.Definitions(assets=[raw_orders, daily_revenue])
```

**테스트가 쉽다**: 함수이므로 직접 실행해 검증할 수 있습니다.

```python
def test_assets():
    result = dg.materialize([raw_orders, daily_revenue])
    assert result.success
    assert result.output_for_node("daily_revenue") == 350
```

### 6.4 파티션, 스케줄, 리소스, 품질 검사

```python
import dagster as dg

daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")


class WarehouseResource(dg.ConfigurableResource):
    dsn: str          # 비밀정보는 환경변수로 주입 (dg.EnvVar 사용)

    def upsert(self, table: str, rows: list[dict]) -> int:
        ...           # 실제 DB 적재 로직
        return len(rows)


@dg.asset(partitions_def=daily)
def orders(context: dg.AssetExecutionContext, warehouse: WarehouseResource) -> None:
    date = context.partition_key                       # 예: "2026-01-15" → 구간이 코드에 명시됨
    rows = fetch_orders(date)
    n = warehouse.upsert("orders", rows)
    context.add_output_metadata({"row_count": n, "date": date})


@dg.asset_check(asset=orders)
def orders_not_empty(context: dg.AssetCheckExecutionContext, warehouse: WarehouseResource):
    cnt = warehouse.count("orders")                    # 예시용 메서드
    return dg.AssetCheckResult(passed=cnt > 0, metadata={"count": cnt})


orders_job = dg.define_asset_job("orders_job", selection=[orders], partitions_def=daily)
orders_schedule = dg.build_schedule_from_partitioned_job(orders_job)   # 파티션 주기(일)에 맞춘 스케줄

defs = dg.Definitions(
    assets=[orders],
    asset_checks=[orders_not_empty],
    jobs=[orders_job],
    schedules=[orders_schedule],
    resources={"warehouse": WarehouseResource(dsn=dg.EnvVar("WAREHOUSE_DSN"))},
)
```

UI에서 파티션별 성공/실패 상태를 달력처럼 확인하고, **실패한 파티션만 골라 재실행**하거나 과거 구간을 일괄 백필할 수 있습니다.

### 6.5 센서 예시

```python
@dg.sensor(job=orders_job)
def new_file_sensor(context: dg.SensorEvaluationContext):
    # 외부 조건(예: 새 파일 도착)을 주기적으로 확인하고, 조건이 맞으면 RunRequest 반환
    if new_file_arrived():
        yield dg.RunRequest(run_key=latest_file_name(), partition_key=latest_partition())
```

`run_key`는 같은 조건에 대해 중복 실행을 막는 데 사용됩니다.

### 6.6 장단점

**장점**

- **자산 중심 모델**: 계보, 최신성(freshness), 실행 이력이 UI에 자연스럽게 통합
- 파티션과 백필이 1급 기능
- Resource/IO Manager로 환경(개발/운영) 분리와 **로컬 테스트**가 쉬움
- Asset Check로 품질 검사를 파이프라인에 통합
- dbt 통합이 잘 갖춰져 있음 (dbt 모델을 Asset으로 노출)

**단점 / 주의**

- 개념(Asset, Resource, IO Manager, Definitions 등)을 익히는 시간이 필요
- Airflow에 비해 **생태계·채용 수요·레퍼런스가 작음**
- 최근 버전에서 프로젝트 구조, CLI, 신규 기능 변화가 잦아 **자료의 버전 확인이 필수**
- 단순한 "순서대로 실행" 워크플로에는 모델이 과할 수 있음

### 6.7 자주 하는 실수

| 실수 | 대응 |
|---|---|
| Asset 이름과 함수 인자 이름 불일치 | 인자 이름 = 의존 Asset 이름이 기본 규칙 |
| 큰 데이터를 메모리로 주고받는 IO Manager 사용 | 저장소 기반 IO Manager 또는 경로 전달 방식 사용 |
| 파티션 키를 쓰지 않고 `now()` 사용 | `context.partition_key`로 구간 명시 → 재실행·백필 안전 |
| 비밀정보를 코드에 직접 작성 | `EnvVar` / Secret 관리 |

---

## 7. dbt

### 7.1 개념

dbt(data build tool)는 **웨어하우스 안에서** SQL `SELECT` 문으로 변환 모델을 만들고, 의존성·테스트·문서화를 관리하는 도구입니다. 데이터를 가져오는(Extract/Load) 도구가 아니라 **이미 적재된 데이터를 변환(Transform)** 하는 계층을 담당합니다.

```text
[수집/적재 도구]  →  웨어하우스(raw)  →  dbt 모델 (staging → intermediate → marts)  →  BI / 분석
 Fivetran, Airbyte,        │                     ▲
 Python, Airflow 등        └── SQL 은 웨어하우스 엔진에서 실행(dbt 는 SQL 을 컴파일하고 전송)
```

| 용어 | 설명 |
|---|---|
| **Model** | `.sql` 파일 하나 = `SELECT` 문 하나. 실행하면 테이블/뷰가 생성됨 |
| **`ref()`** | 다른 모델 참조. **의존성 그래프(DAG)와 실행 순서가 자동 결정됨** |
| **`source()`** | 원천(raw) 테이블 정의·참조. 신선도(freshness) 검사 가능 |
| **Materialization** | `view`, `table`, `incremental`, `ephemeral` 등 모델이 물리적으로 만들어지는 방식 |
| **Test** | 데이터 검증. 일반(generic) 테스트(`unique`, `not_null`, `accepted_values`, `relationships`), 단일(singular) SQL 테스트 |
| **Unit test** | 입력 행을 직접 지정해 모델 로직을 검증하는 테스트 (비교적 최근 버전에서 지원) |
| **Seed** | 작은 CSV를 테이블로 적재 (코드/매핑 테이블 등) |
| **Snapshot** | 변경 이력(SCD Type 2) 기록 |
| **Macro** | Jinja 기반 재사용 SQL 함수 |
| **Adapter** | 웨어하우스별 연결·SQL 방언 지원 (Postgres, BigQuery, Snowflake, Redshift, Databricks 등) |
| **profiles.yml** | 접속 정보와 target(dev/prod) 정의 |
| **Docs** | 모델 설명, 컬럼 설명, 계보 그래프 문서 사이트 |

### 7.2 설치와 프로젝트 시작

```bash
pip install dbt-core dbt-postgres       # 사용하는 웨어하우스의 어댑터 설치
dbt init my_project                      # 프로젝트 생성
cd my_project
dbt debug                                # 연결 확인
```

```yaml
# ~/.dbt/profiles.yml  (접속 정보는 저장소에 커밋하지 않기, 환경변수 사용)
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      port: 5432
      dbname: warehouse
      schema: dbt_dev
      threads: 4
    prod:
      type: postgres
      host: "{{ env_var('PROD_HOST') }}"
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      port: 5432
      dbname: warehouse
      schema: analytics
      threads: 8
```

### 7.3 권장 프로젝트 구조

```text
models/
├── staging/          # 원천 1:1 정제 (이름·타입 정리, 가벼운 변환). 보통 view
│   ├── _sources.yml
│   ├── _stg_models.yml
│   └── stg_orders.sql
├── intermediate/     # 복잡한 로직을 단계별로 분리
│   └── int_orders_enriched.sql
└── marts/            # 분석/업무에서 쓰는 최종 모델 (fact, dim). 보통 table 또는 incremental
    ├── fct_orders.sql
    └── dim_customers.sql
```

### 7.4 모델 작성 예시

```yaml
# models/staging/_sources.yml
version: 2
sources:
  - name: raw
    schema: raw
    tables:
      - name: orders
        loaded_at_field: _ingested_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
```

```sql
-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    cast(amount as numeric(12,2)) as amount,
    lower(status)                 as status,
    updated_at
from {{ source('raw', 'orders') }}
```

```sql
-- models/marts/fct_orders.sql  (증분 모델)
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

select
    o.order_id,
    o.customer_id,
    o.amount,
    o.status,
    o.updated_at
from {{ ref('stg_orders') }} o

{% if is_incremental() %}
  -- 이미 적재된 최대 updated_at 이후 데이터만 처리
  where o.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

```yaml
# models/marts/_marts_models.yml
version: 2
models:
  - name: fct_orders
    description: "주문 팩트 테이블 (주문당 1행)"
    columns:
      - name: order_id
        description: "주문 ID"
        data_tests: [unique, not_null]        # 구버전 문서에서는 `tests:` 키를 사용
      - name: status
        data_tests:
          - accepted_values:
              values: ['paid', 'cancelled', 'refunded']
      - name: customer_id
        data_tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

**Materialization 선택 기준**

| 방식 | 특징 | 적합한 경우 |
|---|---|---|
| `view` | 저장 없이 쿼리 시 계산 | 가벼운 staging, 데이터가 작을 때 |
| `table` | 실행 때마다 전체 재생성 | 중간 규모, 단순한 구현 |
| `incremental` | 신규·변경분만 반영 | 큰 fact 테이블 (증분 기준 설계 필요) |
| `ephemeral` | 물리 생성 없이 CTE로 인라인 | 재사용 목적의 작은 중간 로직 |

> 증분 모델에서 **삭제된 행**이나 **늦게 도착한 데이터**는 기본 조건만으로 처리되지 않을 수 있습니다. 필요하면 lookback 구간을 두거나 주기적 full refresh를 검토합니다.

### 7.5 주요 명령어

```bash
dbt run                          # 모델 실행 (테이블/뷰 생성)
dbt test                         # 테스트 실행
dbt build                        # 모델 실행 + 테스트 + seed + snapshot 을 의존 순서대로 (실무에서 가장 흔함)
dbt compile                      # SQL 컴파일 결과만 생성 (실행하지 않음)
dbt run --select fct_orders      # 특정 모델만
dbt run --select +fct_orders     # 상위(업스트림) 의존 모델 포함
dbt run --select fct_orders+     # 하위(다운스트림) 모델 포함
dbt run --target prod            # prod target 으로 실행
dbt run --full-refresh           # 증분 모델을 전체 재생성
dbt source freshness             # 소스 신선도 검사
dbt docs generate && dbt docs serve   # 문서·계보 사이트 생성/확인
```

CI에서는 변경된 모델과 그 하위 모델만 검증하는 방식(상태 비교 기반 선택, `state:modified+` 등)을 활용할 수 있습니다.

### 7.6 장단점

**장점**

- SQL만 알면 시작할 수 있고, **변환 로직을 코드(Git)로 관리**하여 리뷰·테스트·배포 가능
- `ref()` 기반 **의존성·계보 자동화**, 문서화
- 내장 테스트로 품질 검사를 변환과 함께 관리
- 웨어하우스·레이크하우스 어댑터가 다양하고 생태계가 큼

**단점 / 한계**

- **E, L은 하지 않음**: 수집·적재 도구가 따로 필요
- **스케줄링·알림은 직접 해결**해야 함 (오케스트레이터 또는 상용 클라우드 서비스)
- 복잡한 절차적 로직, 대규모 Python 처리에는 적합하지 않음 (SQL 중심)
- 증분 모델 설계가 부정확하면 데이터 불일치 발생
- 도구 자체의 라이선스·제품 구성(Core / 상용 서비스 / 신규 엔진 등)은 변화가 있을 수 있으므로 확인 필요

### 7.7 자주 하는 실수

| 실수 | 대응 |
|---|---|
| 모든 모델을 `table`로 만들어 비용 증가 | staging은 view, 큰 fact만 incremental 등 계층별 전략 |
| 테스트 없이 모델만 작성 | 최소 키 모델에 `unique`, `not_null` 적용 |
| 원천 테이블 이름을 SQL에 직접 하드코딩 | `source()`, `ref()` 사용 |
| 증분 모델의 `unique_key` 누락/불일치 | 중복 적재 → 키 정의 확인 |
| 접속 정보를 저장소에 커밋 | `profiles.yml`은 저장소 밖 + 환경변수 |
| dev/prod 스키마 미분리 | target별 스키마 분리 |

---

## 8. 같은 파이프라인을 세 오케스트레이터로 비교

**시나리오**: 매일 전날 주문 데이터를 API에서 수집 → 정제 → 품질 검사 → 웨어하우스 적재 (멱등)

| 관점 | Airflow | Prefect | Dagster |
|---|---|---|---|
| 정의 방식 | `@dag` + `@task` 함수, DAG 파일 | `@flow` + `@task` 함수, 일반 Python 스크립트 | `@asset` 함수 + `Definitions` |
| 의존성 표현 | `a >> b` 또는 함수 호출 연결 | 함수 호출 순서 | 함수 인자 이름 |
| 구간(날짜) 처리 | data interval(스케줄러가 제공) | 파라미터로 직접 전달·계산 | 파티션 키(`context.partition_key`) |
| 재시도 | `retries`, `retry_delay` (task/default_args) | `@task(retries=..., retry_delay_seconds=...)` | RetryPolicy / op·asset 설정 |
| 스케줄 | `schedule="0 2 * * *"` | `serve(cron=...)` 또는 deployment | `ScheduleDefinition` / 파티션 스케줄 |
| 백필 | 구간 단위 DAG run 실행 | 파라미터로 날짜를 바꿔 실행(직접 구성) | 파티션 범위를 UI/CLI로 선택해 실행 |
| 품질 검사 | 별도 태스크로 구현 | 별도 태스크로 구현 | **Asset Check**로 통합 |
| 계보 | 제한적 | 제한적 | 자산 그래프로 기본 제공 |
| 로컬 실행 | `airflow standalone`, `dags test` | **`python script.py`** | `dagster dev`, 함수 직접 호출 테스트 |
| 코드 예시 위치 | 4.3절 | 5.2절 | 6.4절 |

**느낌 차이 요약**

- Airflow: "**언제 어떤 작업을 어떤 순서로** 실행할까?" (스케줄·운영 성숙도)
- Prefect: "**내 Python 코드를 그대로** 신뢰할 수 있는 워크플로로" (유연성·개발 속도)
- Dagster: "**어떤 데이터 자산이** 최신인가, 어디서 왔는가?" (계보·품질·파티션)

---

## 9. 조합 패턴: 실무에서는 어떻게 함께 쓰는가

### 9.1 Airflow + dbt

가장 흔한 조합 중 하나입니다.

```text
Airflow DAG (매일 02:00)
  ├─ extract_orders (Python)            → raw 적재
  ├─ validate_raw (품질 검사)
  ├─ dbt_build (dbt 실행)               → staging → marts + 테스트
  └─ notify (성공/실패 알림)
```

**방법 1: 단순 호출 (BashOperator)**

```python
from airflow.providers.standard.operators.bash import BashOperator   # Airflow 2.x 는 airflow.operators.bash

dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command="cd /opt/dbt/my_project && dbt build --target prod",
)
```

단점: dbt 모델 단위의 개별 태스크·재시도·UI 가시성이 없음(전체가 한 태스크).

**방법 2: dbt 프로젝트를 Airflow 태스크 그래프로 변환하는 통합 도구 사용** (예: Astronomer Cosmos 등) → 모델별 태스크, 모델 단위 재시도·가시성 확보. 도구의 호환 버전을 확인해야 합니다.

### 9.2 Dagster + dbt

`dagster-dbt` 통합으로 dbt 모델이 **Dagster Asset으로 자동 노출**됩니다. 수집 Asset(Python) → dbt 모델 Asset → 후속 Asset까지 **하나의 계보 그래프**로 보이고, 하나의 UI에서 실행·파티션·상태 관리가 가능합니다. dbt를 중심에 두면서 오케스트레이션 통합을 원하는 경우 강점으로 자주 언급됩니다.

```python
# 개념 예시 (세부 API는 버전에 따라 다름)
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

my_dbt_project = DbtProject(project_dir="path/to/my_project")

@dbt_assets(manifest=my_dbt_project.manifest_path)
def my_dbt_models(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

### 9.3 Prefect + dbt

`prefect-dbt` 통합 또는 단순히 `subprocess`/dbt CLI를 Task 안에서 호출하는 방식으로 연결합니다. Prefect Flow에서 수집 → dbt 실행 → 알림을 하나의 코드 흐름으로 작성할 수 있습니다.

```python
import subprocess
from prefect import flow, task

@task(retries=1)
def run_dbt():
    subprocess.run(["dbt", "build", "--target", "prod"], check=True, cwd="my_project")

@flow
def pipeline():
    extract_and_load()
    run_dbt()
```

### 9.4 Celery와의 관계

| 상황 | 관계 |
|---|---|
| **Airflow의 CeleryExecutor** | Airflow가 태스크 실행을 Celery 워커에 분산시키는 방식. 이때 Celery는 Airflow 내부의 "실행 계층" |
| 웹 서비스에서 **사용자 요청 → 비동기 작업** | Celery 단독 사용 (오케스트레이터 불필요) |
| 웹 서비스가 **데이터 파이프라인을 트리거** | 웹 앱 → Airflow/Prefect/Dagster API 호출로 실행 요청 |

두 영역을 혼동하지 않는 것이 중요합니다: **Celery = "작업 실행 분산"**, **Airflow/Prefect/Dagster = "작업 흐름 관리"**.

### 9.5 권장 참조 아키텍처 (입문용)

```text
[공개 API / 파일]
      │  Python 수집 (재시도·타임아웃·페이지네이션)
      ▼
[S3 또는 로컬 raw]  ← 원본 보존 (Bronze)
      │
      ▼
[PostgreSQL / 웨어하우스 raw 스키마]  ← 멱등 적재 (UPSERT)
      │
      ▼
[dbt: staging → marts + 테스트]  ← 변환·품질 (Silver/Gold)
      ▲
      │ 스케줄·의존성·재시도·알림
[Airflow 또는 Dagster 또는 Prefect]
      │
      ▼
[Docker Compose 로 로컬 재현 / GitHub Actions CI]
```

---

## 10. 선택 가이드

### 10.1 질문으로 고르기

```text
Q1. 웹 서비스의 비동기 작업(이메일, 알림, 이미지 처리)이 목적인가?
    └─ 예 → Celery (또는 유사한 태스크 큐)

Q2. SQL로 웨어하우스 안에서 데이터를 변환·테스트하고 싶은가?
    └─ 예 → dbt  (+ 스케줄링은 아래 오케스트레이터 중 하나)

Q3. 배치 파이프라인의 스케줄·재시도·백필을 관리해야 하는가?
    ├─ 채용 시장/기존 인프라/자료의 풍부함이 중요 → Airflow
    ├─ Python 코드 중심, 빠른 개발, 동적 워크플로가 중요 → Prefect
    └─ 데이터 자산 계보·파티션·품질 통합이 중요, dbt 중심 → Dagster
```

### 10.2 상황별 추천

| 상황 | 추천 | 이유 |
|---|---|---|
| 데이터 엔지니어 **신입 취업 준비** | Airflow 1순위 + dbt | 채용 공고 빈도와 자료 풍부 |
| 소규모 팀, 빠르게 자동화 | Prefect | 도입이 가볍고 Python 코드 그대로 사용 |
| dbt 중심의 분석 엔지니어링 팀 | Dagster 또는 Airflow(+Cosmos) | dbt 통합, 계보 가시성 |
| 이미 Airflow 운영 중인 조직 | Airflow 유지 | 전환 비용, 운영 노하우 |
| 웹 백엔드 비동기 작업 | Celery | 목적 적합 |
| SQL 변환 로직의 품질·문서화가 부족 | dbt 도입 | 테스트·문서·계보 |
| 이벤트 기반 자동화·동적 실행이 많음 | Prefect 또는 Dagster 센서 | 유연한 트리거 |

### 10.3 평가 기준 점수표(주관적 가이드, 팀 상황에 따라 다름)

| 기준 | Airflow | Prefect | Dagster |
|---|:-:|:-:|:-:|
| 채용·자료·커뮤니티 | ◎ | ○ | ○ |
| 로컬 개발·테스트 편의성 | ○ | ◎ | ◎ |
| 배치 스케줄·백필 성숙도 | ◎ | ○ | ◎ |
| 계보·자산 가시성 | △ | △ | ◎ |
| 동적·이벤트 워크플로 유연성 | ○ | ◎ | ○ |
| 운영 구성 단순성 | △ | ○ | ○ |
| dbt 통합 | ○ | ○ | ◎ |

---

## 11. 신입을 위한 학습 순서

```text
1단계  SQL / Python / Git 기초
2단계  Airflow 로 end-to-end 배치 파이프라인 1개 완성 (재시도, 멱등성, 로깅)
3단계  dbt 로 변환 계층 구성 (staging → marts, 테스트, 문서)
4단계  Docker Compose 로 로컬 재현 + CI 에서 테스트
5단계  Dagster 또는 Prefect 로 같은 파이프라인을 다시 구현해 비교
6단계  (필요 시) Celery 로 웹 앱 비동기 작업 경험
```

**권장 포트폴리오**: 하나의 공개 API 데이터를 `Python 수집 → PostgreSQL raw → dbt 변환·테스트 → Airflow 스케줄` 로 구성하고, README에 **"왜 Airflow를 골랐고 Dagster/Prefect와 무엇이 달랐는지"** 비교를 한 단락 남기면 설계 판단력을 보여 줄 수 있습니다.

---

## 12. 면접 대비 질문

| 질문 | 답변 핵심 |
|---|---|
| Celery와 Airflow의 차이는? | Celery는 태스크 큐(작업 분산 실행), Airflow는 워크플로 오케스트레이터(스케줄·의존성·이력·백필). Airflow가 Celery를 Executor로 쓰기도 함 |
| dbt는 오케스트레이터인가? | 아니다. SQL 변환·테스트 도구이며, 스케줄링은 외부 오케스트레이터가 담당 |
| Airflow의 `catchup`이란? | 과거 누락 구간 DAG run을 자동 생성할지 여부. 의도 없이 켜면 대량 실행 가능 |
| 태스크 재시도 시 중복을 어떻게 막는가? | 멱등 설계(UPSERT, 파티션 덮어쓰기, 스테이징 교체). 재시도 설정만으로는 불충분 |
| Airflow의 XCom에 큰 데이터를 넣으면 안 되는 이유? | 메타DB 부하. 저장소에 쓰고 경로만 전달 |
| Dagster의 Asset 중심 모델의 장점은? | 데이터 관점의 계보·최신성·파티션·품질 검사를 통합 관리 |
| Prefect가 Airflow보다 유리한 상황은? | Python 코드 중심, 동적 워크플로, 가벼운 도입·로컬 개발 |
| dbt의 `ref()`가 하는 일은? | 모델 참조를 통해 의존성 그래프와 실행 순서를 자동 결정, 환경별 스키마 해석 |
| dbt 증분 모델에서 주의할 점은? | `unique_key`, 증분 조건(경계/늦은 데이터), 삭제 행 처리, 주기적 full refresh 검토 |
| `dbt run`과 `dbt build`의 차이는? | `run`은 모델만 실행, `build`는 모델·테스트·seed·snapshot을 의존 순서대로 함께 실행 |
| Celery에서 `acks_late`의 위험은? | 워커 중단 시 태스크 재전달 → 비멱등 작업이면 중복 실행 |
| 왜 하나의 도구만 고집하지 않고 조합하는가? | 역할이 다름: 수집(Python) / 변환(dbt) / 조율(오케스트레이터) / 비동기 실행(Celery) |

---

## 13. 공통 실수와 점검표

### 공통 원칙

- [ ] **멱등성**: 재시도·재실행·백필 시 결과가 같도록 설계했는가?
- [ ] **구간 파라미터화**: `now()` 대신 실행 구간(날짜·파티션)을 입력으로 받는가?
- [ ] **원본 보존**: 변환 로직이 틀렸을 때 원본에서 다시 계산할 수 있는가?
- [ ] **품질 검사**: 성공 상태 = 올바른 데이터가 아님을 알고, 검사 규칙을 파이프라인에 포함했는가?
- [ ] **비밀정보**: 코드·저장소에 키와 비밀번호를 넣지 않았는가?
- [ ] **타임아웃·재시도**: 무한 대기와 일시 오류에 대한 대비가 있는가?
- [ ] **관측성**: 행 수·소요 시간·실패 알림이 기록되고 확인되는가?
- [ ] **환경 분리**: dev/prod 설정이 분리되어 있는가?
- [ ] **재현성**: `docker compose up` 등으로 다른 사람이 같은 환경을 띄울 수 있는가?

### 도구별 한 줄 체크

| 도구 | 반드시 확인할 것 |
|---|---|
| Celery | 태스크 멱등성, time limit, 브로커 visibility timeout, 큰 데이터 비전달, Beat 단일 실행 |
| Airflow | DAG 최상위 코드 최소화, `catchup`, XCom 크기, data interval 사용, Secrets Backend |
| Prefect | 날짜 파라미터화, 운영 시 work pool/worker 구성, 메이저 버전 문서 확인 |
| Dagster | 파티션 키 사용, Resource/EnvVar로 설정 분리, Asset Check 구성 |
| dbt | `source/ref` 사용, 키 테스트, 증분 `unique_key`와 조건, target별 스키마 분리 |

---

## 마무리

- **Celery**는 "작업을 분산 실행"하는 도구, **Airflow·Prefect·Dagster**는 "작업 흐름을 관리"하는 도구, **dbt**는 "웨어하우스 안의 SQL 변환을 관리"하는 도구입니다.
- 신입 단계에서는 **Airflow + dbt**로 하나의 파이프라인을 끝까지 완성하고, Dagster·Prefect는 같은 파이프라인을 다시 구현하며 **차이를 설명할 수 있는 수준**이면 충분합니다.
- 도구 이름보다 **멱등성, 품질 검사, 장애 복구, 설계 근거**를 설명할 수 있는 것이 더 큰 경쟁력입니다.

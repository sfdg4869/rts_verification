# QA Portal Server - 전체 프로젝트 분석

## 📋 목차
1. [프로젝트 개요](#프로젝트-개요)
2. [아키텍처 구조](#아키텍처-구조)
3. [핵심 모듈 분석](#핵심-모듈-분석)
4. [API 엔드포인트 분류](#api-엔드포인트-분류)
5. [DB 연결 흐름](#db-연결-흐름)
6. [파일 간 연관성](#파일-간-연관성)

---

## 1. 프로젝트 개요

### 프로젝트 목적
Oracle Database의 실시간 성능 모니터링 및 데이터 비교 시스템
- **Target DB**: 모니터링 대상 Oracle Database (실시간 데이터)
- **Repo DB**: 과거 데이터 저장소 (Oracle 또는 PostgreSQL)
- **MongoDB**: DB 접속 정보 및 설정 관리

### 기술 스택
- **Backend**: Flask (Python)
- **Database**: Oracle (oracledb), PostgreSQL (psycopg2), MongoDB (pymongo)
- **API Documentation**: Flasgger (Swagger)
- **AI/ML**: Google Gemini API
- **Frontend**: HTML/JavaScript (SSE 기반 실시간 업데이트)

---

## 2. 아키텍처 구조

```
qa-portal-server-main/
├── app.py                          # 애플리케이션 진입점
├── app/
│   ├── __init__.py                 # Flask 앱 팩토리
│   ├── shared_db.py                # 전역 DB 연결 관리
│   ├── models/
│   │   └── db_config.py            # MongoDB 설정 모델
│   ├── services/
│   │   ├── db_config_service.py    # MongoDB CRUD 서비스
│   │   ├── oracle_service.py       # Oracle 연결 서비스
│   │   ├── postgresql_service.py   # PostgreSQL 연결 서비스
│   │   └── ssh_utils.py            # SSH 유틸리티
│   └── routes/
│       ├── __init__.py             # Blueprint 등록
│       ├── rts_routes.py           # DB 설정 API
│       ├── rts_compare.py          # 실시간 DB 비교 API
│       ├── rts_data_routes.py      # Target/Repo 데이터 조회 API
│       ├── rts_chart.py            # 차트 데이터 API
│       ├── mongodb_config.py       # MongoDB 설정 관리 API
│       ├── db_selector.py          # DB 선택 웹 인터페이스
│       ├── statsec_gemma_auto.py   # AI 리포트 생성 API
│       ├── delta_cleanup.py        # Delta 검증/정리 API
│       ├── target_statsec_setup.py # Target DB 초당 로깅 설정
│       ├── data_monitor_1min.py # 데이터 수집 모니터링(1분)
│       └── ... (기타 라우트)
├── setup_db_from_json.py          # DB 설정 테스트 스크립트
└── requirements.txt                # Python 패키지 의존성
```

---

## 3. 핵심 모듈 분석

### 3.1 애플리케이션 진입점

#### `app.py`
- **역할**: Flask 애플리케이션 시작점
- **주요 기능**:
  - `create_app()` 호출로 앱 인스턴스 생성
  - `/db-manager` 라우트 추가 (DB 설정 관리 웹 페이지)
  - 개발 서버 실행 (`debug=True`)
- **연관 파일**: `app/__init__.py`

#### `app/__init__.py`
- **역할**: Flask 앱 팩토리 패턴 구현
- **주요 함수**:
  - `create_app()`: Flask 앱 인스턴스 생성 및 설정
  - `_auto_load_db_setup()`: 애플리케이션 시작 시 DB 설정 자동 로드
- **주요 기능**:
  - CORS 설정 (Cross-Origin Resource Sharing)
  - Swagger UI 통합 (API 문서화)
  - 모든 Blueprint 자동 등록
  - 정적 HTML 페이지 라우트 설정 (`/realtime-chart`, `/realtime-compare`)
- **연관 파일**: 
  - `app/routes/__init__.py` (Blueprint 목록)
  - `app/shared_db.py` (DB 설정 로드)

---

### 3.2 데이터베이스 연결 관리

#### `app/shared_db.py` ⭐ **핵심**
- **역할**: 전역 DB 연결 관리 모듈
- **주요 변수**:
  - `_db_connections`: Target/Repo DB 연결 정보 저장 (메모리)
  - `_default_profile_id`: 현재 사용 중인 MongoDB 프로필 ID
  - `_config_file`: JSON 백업 파일 경로 (`db_config.json`)

- **주요 함수**:
  ```python
  # DB 설정 로드/저장
  _load_config_from_mongodb()      # MongoDB에서 기본 프로필 로드
  _load_config_from_json()         # JSON 파일에서 로드 (백업)
  _save_config()                   # JSON 파일에 저장
  
  # DB 연결
  get_connection(db_type)          # Target/Repo DB 연결 객체 반환
  _connect_oracle_db(config)       # Oracle 연결 생성
  _connect_postgres_db(config)     # PostgreSQL 연결 생성
  _build_oracle_dsn(config)        # Oracle DSN 생성
  
  # 설정 관리
  set_db_config(db_type, config)   # DB 설정 저장 (메모리 + JSON)
  get_db_config(db_type)           # DB 설정 조회
  is_target_db_configured()        # Target DB 설정 여부 확인
  is_repo_db_configured()          # Repo DB 설정 여부 확인
  
  # 프로필 관리
  set_connection_profile(profile_id)  # MongoDB 프로필 선택
  get_current_profile_id()            # 현재 프로필 ID 반환
  ```

- **DB 타입 추론 로직**:
  ```python
  _infer_db_engine(cfg, default)   # db_type, port, service 정보로 DB 타입 추론
  _normalize_service_type(value)   # 'sid' 또는 'service_name' 표준화
  ```

- **연관 파일**:
  - `app/services/db_config_service.py` (MongoDB 연동)
  - `app/routes/rts_routes.py` (DB 설정 API)
  - 거의 모든 라우트 파일 (DB 연결 사용)

#### `app/models/db_config.py`
- **역할**: MongoDB 연결 설정 및 데이터 모델 정의
- **주요 클래스**:
  - `DBConfigModel`: DB 설정 데이터 모델
    - `create_target_config()`: Target DB 설정 생성
    - `create_repo_config()`: Repo DB 설정 생성
    - `create_connection_profile()`: 연결 프로필 생성
    - `validate_target_config()`: Target DB 설정 유효성 검증
    - `validate_repo_config()`: Repo DB 설정 유효성 검증

- **주요 변수**:
  ```python
  MONGODB_CONFIG = {
      'uri': _build_mongodb_uri(),           # MongoDB 연결 URI (.env에서 로드)
      'database': os.getenv('MONGODB_DATABASE', 'repo_test'),
      'collections': {
          'db_configs': os.getenv('MONGODB_DB_CONFIGS_COLLECTION', 'test'),
          'connection_profiles': os.getenv('MONGODB_CONNECTION_PROFILES_COLLECTION', 'connection_profiles')
      }
  }
  ```

- **MongoDB URI 빌드 로직**:
  ```python
  _build_mongodb_uri()  # .env 변수에서 MongoDB URI 구성
  # 우선순위: MONGODB_URI > 개별 필드 조합 (username/password/host/port)
  ```

- **연관 파일**:
  - `app/services/db_config_service.py` (MONGODB_CONFIG 사용)
  - `.env` (환경 변수 설정)

#### `app/services/db_config_service.py` ⭐ **핵심**
- **역할**: MongoDB 기반 DB 설정 관리 서비스
- **주요 클래스**:
  - `DBConfigService`: MongoDB CRUD 서비스 클래스
    - `__init__()`: MongoDB 연결 초기화
    - `is_connected()`: MongoDB 연결 상태 확인
    - `test_connection()`: MongoDB ping 테스트

- **Target/Repo DB 설정 관리**:
  ```python
  save_target_config(config)       # Target DB 설정 저장
  get_target_config(name)          # Target DB 설정 조회
  save_repo_config(config)         # Repo DB 설정 저장
  get_repo_config(name)            # Repo DB 설정 조회
  get_all_db_configs()             # 모든 DB 설정 조회 (MongoDB entries 배열 순회)
  ```

- **연결 프로필 관리**:
  ```python
  save_connection_profile(profile)          # 프로필 저장
  get_connection_profile(profile_id)        # 프로필 조회 (Repo 설정 포함)
  get_default_connection_profile()          # 기본 프로필 조회
  set_default_profile(profile_id)           # 기본 프로필 설정
  get_connection_profiles()                 # 모든 프로필 목록 조회
  update_connection_profile(profile_id, data)  # 프로필 수정
  ```

- **DB 선택 관리**:
  ```python
  select_target_db(config_id)      # Target DB 선택
  select_repo_db(config_id)        # Repo DB 선택
  get_current_target_db()          # 현재 선택된 Target DB 조회
  get_current_repo_db()            # 현재 선택된 Repo DB 조회
  set_target_db(config_id)         # Target DB 설정 (JSON 파일 업데이트)
  set_repo_db(config_id)           # Repo DB 설정 (JSON 파일 업데이트)
  ```

- **DB 접속 정보 CRUD**:
  ```python
  create_db_entry(entry_data)      # DB 접속 정보 생성 (UUID 생성)
  update_db_entry(entry_id, data)  # DB 접속 정보 수정
  delete_db_entry(entry_id)        # DB 접속 정보 삭제
  _check_name_host_duplicate()     # name+host 조합 중복 체크
  ```

- **전역 인스턴스**:
  ```python
  db_config_service = DBConfigService()  # 싱글톤 인스턴스
  ```

- **연관 파일**:
  - `app/models/db_config.py` (MONGODB_CONFIG, DBConfigModel)
  - `app/shared_db.py` (프로필 로드/선택)
  - `app/routes/mongodb_config.py` (API 엔드포인트)
  - `app/routes/db_selector.py` (DB 선택 UI)

---

### 3.3 API 라우트 (Blueprint)

#### `app/routes/__init__.py`
- **역할**: 모든 Blueprint를 중앙 관리
- **Blueprint 목록** (총 22개):
  ```python
  blueprints = [
      server_bp,                   # 서버 상태
      demo_bp,                     # 데모 API
      rts_bp,                      # DB 설정 (/rts)
      bp_data,                     # Target/Repo 데이터 조회
      bp_compare,                  # 실시간 DB 비교
      bp_chart,                    # 차트 데이터
      bp_stat_sec_util,            # 통계/보안 유틸리티
      bp_stat_delta_query,         # 통계 델타 쿼리
      bp_statsec_gemma_auto,       # DB 자동조회 Gemma AI 리포트
      bp_web_login_test,           # 웹 로그인 테스트
      bp_delta_cleanup,            # Delta 검증 및 정리
      bp_target_statsec_setup,     # Target DB 초당 로깅 설정
      bp_repo_sec_logging,         # Repo DB sec_logging.json 생성
      bp_data_monitoring,          # 데이터 수집 모니터링
      data_monitor_bp,             # 데이터 누락 모니터링
      data_monitor_10min_bp,       # 10분 단위 데이터 모니터링
      data_monitor_hourly_bp,      # 1시간/일별 데이터 모니터링
      mongodb_config_bp,           # MongoDB 설정 관리
      db_selector_bp,              # DB 선택기
      db_selection_bp,             # DB 선택 API
      postgresql_bp,               # PostgreSQL 연결
      oracle_bp                    # Oracle 연결
  ]
  ```

#### `app/routes/rts_routes.py`
- **URL Prefix**: `/rts`
- **역할**: DB 연결 정보 설정 및 상태 확인
- **주요 API**:
  ```
  POST   /rts/db/setup        # Target/Repo DB 설정 (JSON 및 MongoDB 저장)
  GET    /rts/db/status       # DB 설정 상태 확인
  POST   /rts/db/reload       # db_config.json에서 설정 재로드
  ```
- **주요 함수**:
  - `setup_db_connections()`: DB 연결 정보 설정
  - `get_db_status()`: DB 설정 상태 반환
  - `reload_db_config()`: JSON 파일에서 설정 재로드
  - `_connect_target(db_config)`: Target DB 연결
  - `_connect_repo(db_config)`: Repo DB 연결
- **연관 파일**:
  - `app/shared_db.py` (설정 저장/조회)
  - `setup_db_from_json.py` (테스트 스크립트)

#### `app/routes/rts_compare.py` ⭐ **핵심**
- **URL Prefix**: `/rts/compare`
- **역할**: Target DB와 Repo DB의 실시간 데이터 비교
- **주요 API**:
  ```
  GET    /rts/compare/db_status          # DB 설정 상태 확인
  GET    /rts/compare/stream             # 실시간 DB 비교 스트리밍 (SSE, 30초)
  GET    /rts/compare                    # 전체 DB 비교 데이터 (30초간 수집)
  POST   /rts/compare/repo-data          # Repo DB 데이터 수동 조회
  ```

- **핵심 기능**:
  1. **실시간 스트리밍 비교** (`stream_compare_db_data()`):
     - Server-Sent Events (SSE) 사용
     - 30초간 매 interval(기본 1초)마다 데이터 수집
     - Target/Repo 병렬 조회
     - 실시간 차이 계산 및 스트리밍
  
  2. **30초간 데이터 수집 및 비교** (`compare_db_data()`):
     - 30회 데이터 수집 (기본 1초 간격)
     - Target/Repo 순차 조회 (Target 먼저 → Repo 조회)
     - 전체 수집 후 JSON 응답 반환
  
  3. **Repo DB 병렬 조회** (`_fetch_repo_data_parallel()`):
     - ThreadPoolExecutor 사용 (max_workers=10)
     - STAT_METRICS (9개) + EVENT_METRICS (5개) 병렬 조회
     - 각 지표별 독립적인 DB 연결 사용
     - 조회 시간 측정 (elapsed_time)

- **주요 쿼리**:
  ```python
  # Target DB 쿼리 (실시간 값)
  target_query = """
      SELECT 'logons current' AS name, value AS sigma_value FROM v$sysstat WHERE name = 'logons current'
      UNION ALL
      SELECT stat_name AS name, SUM(value) AS sigma_value 
      FROM v$sysstat WHERE stat_name IN (...)
      GROUP BY stat_name
      UNION ALL
      SELECT event AS name, SUM(time_waited_micro)/1000000 AS sigma_value 
      FROM v$system_event WHERE event IN (...)
      GROUP BY event
  """
  
  # Repo DB 쿼리 (개별 지표, 병렬 처리)
  _fetch_stat_metric(db_id, stat_name)    # 각 STAT 지표별 조회
  _fetch_event_metric(db_id, event_name)  # 각 EVENT 지표별 조회
  # 쿼리: ORDER BY a.time DESC, LIMIT 1 (최신 값)
  ```

- **데이터 구조**:
  ```python
  # 응답 예시
  {
      "target_data": [{
          "collection_seq": 1,
          "timestamp": "2026-01-07 16:13:40",
          "elapsed_time": 0.123,
          "data": [{"name": "logons current", "sigma_value": 10}, ...]
      }],
      "repo_data": [{
          "collection_seq": 1,
          "timestamp": "2026-01-07 16:13:41",
          "elapsed_time": 0.456,
          "data": [{"name": "logons current", "time": "2026-01-07 16:00:00", "sigma_value": 8}, ...]
      }],
      "comparison_results": [{
          "collection_seq": 1,
          "differences": [
              {"name": "logons current", "target_value": 10, "repo_value": 8, "diff": 2, "diff_percentage": 20.0, "within_threshold": True}
          ]
      }]
  }
  ```

- **연관 파일**:
  - `app/shared_db.py` (DB 연결)
  - `realtime_compare_viewer.html` (웹 UI)

#### `app/routes/rts_data_routes.py`
- **URL Prefix**: `/rts/data`
- **역할**: Target/Repo DB 원시 데이터 조회
- **주요 API**:
  ```
  GET    /rts/data/target        # Target DB 데이터 조회
  GET    /rts/data/repo          # Repo DB 데이터 조회
  ```
- **연관 파일**: `app/shared_db.py`

#### `app/routes/mongodb_config.py`
- **URL Prefix**: `/api/v1`
- **역할**: MongoDB 기반 Repo DB 접속 정보 관리 (CRUD API)
- **주요 API**:
  ```
  POST   /api/v1/rts/mongodb_configs         # Repo DB 접속 정보 생성 (UUID 자동 생성)
  GET    /api/v1/rts/mongodb_configs         # 모든 Repo DB 접속 정보 조회
  PUT    /api/v1/rts/mongodb_configs/:id     # Repo DB 접속 정보 수정
  DELETE /api/v1/rts/mongodb_configs/:id     # Repo DB 접속 정보 삭제
  ```

- **필수 필드**:
  ```python
  REQUIRED_FIELDS = [
      'name', 'host', 'database', 'db_user', 'db_password',
      'db_port', 'db_type', 'ssh_user', 'ssh_password',
      'ssh_port', 'os_type', 'dg_home'
  ]
  ```

- **주요 함수**:
  - `post_rts_mongodb_configs()`: Repo DB 접속 정보 생성
  - `get_rts_mongodb_configs_all()`: 모든 Repo DB 접속 정보 조회
  - `put_rts_mongodb_configs_config_id(config_id)`: Repo DB 접속 정보 수정
  - `delete_rts_mongodb_configs_config_id(config_id)`: Repo DB 접속 정보 삭제
  - `_validate_required_fields(data)`: 필수 필드 검증
  - `_sanitize_entry(entry)`: 비밀번호 필드 제거 (응답용)

- **연관 파일**:
  - `app/services/db_config_service.py` (MongoDB CRUD)
  - `db_config_manager.html` (웹 UI)

#### `app/routes/db_selector.py`
- **URL Prefix**: `/rts/db-selector`
- **역할**: DB 선택 웹 인터페이스 및 API
- **주요 API**:
  ```
  GET    /rts/db-selector/                       # DB 선택 웹 페이지
  GET    /rts/db-selector/available-dbs          # 사용 가능한 모든 DB 목록 조회
  POST   /rts/db-selector/select-target          # Target DB 선택
  POST   /rts/db-selector/select-repo            # Repo DB 선택
  GET    /rts/db-selector/current-selection      # 현재 선택된 DB 정보 조회
  ```

- **주요 기능**:
  - MongoDB에서 모든 DB 설정 조회 (`get_all_db_configs()`)
  - 선택된 DB를 `db_config.json`에 저장
  - `app/shared_db.py`의 메모리 설정 업데이트

- **연관 파일**:
  - `app/services/db_config_service.py` (DB 목록 조회)
  - `app/shared_db.py` (설정 저장)
  - `templates/db_selector.html` (웹 UI)

#### `app/routes/statsec_gemma_auto.py`
- **URL Prefix**: `/api/v1/rts/statsec`
- **역할**: Google Gemini API를 사용한 통계/보안 AI 리포트 생성
- **주요 API**:
  ```
  POST   /api/v1/rts/statsec/report/gemma-all   # Gemma AI 종합 리포트 생성
  ```

- **주요 기능**:
  - Target DB에서 v$sysstat, v$system_event 데이터 조회
  - Gemini API로 AI 분석 리포트 생성
  - 파일로 저장 (`.txt`, `.csv`)

- **연관 파일**:
  - `app/shared_db.py` (DB 연결)
  - Google Gemini API

#### `app/routes/delta_cleanup.py`
- **URL Prefix**: `/rts/delta`
- **역할**: Delta 데이터 검증 및 정리 작업
- **주요 API**:
  ```
  POST   /rts/delta/verify-and-clean      # Delta 데이터 검증 및 정리
  ```

- **주요 기능**:
  - Repo DB에서 Delta 데이터 검증
  - 음수 값, 이상치 데이터 정리

- **연관 파일**: `app/shared_db.py`

#### `app/routes/target_statsec_setup.py`
- **URL Prefix**: `/rts/target`
- **역할**: Target DB에 초당 로깅 패키지/프로시저 설치
- **주요 API**:
  ```
  POST   /rts/target/setup-sec-logging    # Target DB에 초당 로깅 설정
  ```

- **주요 기능**:
  - Target DB에 패키지 설치
  - 초당 로깅 JOB 생성
  - SSH 연결하여 스크립트 실행

- **연관 파일**:
  - `app/services/ssh_utils.py` (SSH 연결)
  - `app/shared_db.py` (DB 연결)

#### `app/routes/data_monitor_1min.py`
- **URL Prefix**: `/api/data-collection`
- **역할**: Repo DB의 데이터 수집 현황 모니터링
- **주요 API**:
  ```
  GET    /api/data-collection/status         # 데이터 수집 현황 조회
  POST   /api/data-collection/report         # Gemma AI 데이터 수집 리포트 생성
  ```

- **주요 기능**:
  - Repo DB에서 데이터 수집 누락 확인
  - 1분/10분/1시간/일별 데이터 수집 현황 조회
  - Gemini API로 AI 분석 리포트 생성

- **연관 파일**: Repo DB 직접 연결 (환경 변수)

---

### 3.4 서비스 레이어

#### `app/services/oracle_service.py`
- **역할**: Oracle DB 연결 및 쿼리 실행 유틸리티
- **주요 함수**:
  - `connect_oracle()`: Oracle DB 연결
  - `execute_query()`: 쿼리 실행 및 결과 반환

#### `app/services/postgresql_service.py`
- **역할**: PostgreSQL DB 연결 및 쿼리 실행 유틸리티
- **주요 함수**:
  - `connect_postgresql()`: PostgreSQL DB 연결
  - `execute_query()`: 쿼리 실행 및 결과 반환

#### `app/services/ssh_utils.py`
- **역할**: SSH 연결 및 원격 명령 실행 유틸리티
- **주요 함수**:
  - `ssh_connect()`: SSH 연결 생성
  - `execute_remote_command()`: 원격 명령 실행

---

### 3.5 테스트 및 유틸리티

#### `setup_db_from_json.py`
- **역할**: `db_setup.json` 파일에서 DB 설정을 읽어 `/rts/db/setup` API 호출
- **주요 함수**:
  - `setup_db_from_json()`: JSON 파일 로드 및 API 호출
- **사용 목적**: 로컬 테스트 및 자동화
- **연관 파일**:
  - `db_setup.json` (DB 설정 파일)
  - `app/routes/rts_routes.py` (API 엔드포인트)

---

## 4. API 엔드포인트 분류

### 4.1 DB 설정 관리
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/rts/db/setup` | POST | Target/Repo DB 설정 | rts_routes |
| `/rts/db/status` | GET | DB 설정 상태 확인 | rts_routes |
| `/rts/db/reload` | POST | 설정 재로드 | rts_routes |
| `/api/v1/rts/mongodb_configs` | POST | Repo DB 접속 정보 생성 | mongodb_config |
| `/api/v1/rts/mongodb_configs` | GET | Repo DB 접속 정보 조회 | mongodb_config |
| `/api/v1/rts/mongodb_configs/:id` | PUT | Repo DB 접속 정보 수정 | mongodb_config |
| `/api/v1/rts/mongodb_configs/:id` | DELETE | Repo DB 접속 정보 삭제 | mongodb_config |

### 4.2 DB 선택
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/rts/db-selector/` | GET | DB 선택 웹 페이지 | db_selector |
| `/rts/db-selector/available-dbs` | GET | 사용 가능한 DB 목록 | db_selector |
| `/rts/db-selector/select-target` | POST | Target DB 선택 | db_selector |
| `/rts/db-selector/select-repo` | POST | Repo DB 선택 | db_selector |
| `/rts/db-selector/current-selection` | GET | 현재 선택된 DB 조회 | db_selector |

### 4.3 실시간 데이터 비교
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/rts/compare/db_status` | GET | DB 설정 상태 확인 | rts_compare |
| `/rts/compare/stream` | GET | 실시간 스트리밍 비교 (SSE) | rts_compare |
| `/rts/compare` | GET | 30초 데이터 수집 후 비교 | rts_compare |
| `/rts/compare/repo-data` | POST | Repo DB 데이터 수동 조회 | rts_compare |

### 4.4 데이터 조회
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/rts/data/target` | GET | Target DB 데이터 조회 | rts_data_routes |
| `/rts/data/repo` | GET | Repo DB 데이터 조회 | rts_data_routes |

### 4.5 AI 리포트 생성
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/api/v1/rts/statsec/report/gemma-all` | POST | Gemma AI 종합 리포트 | statsec_gemma_auto |
| `/api/data-collection/report` | POST | 데이터 수집 AI 리포트 | data_monitor_1min |

### 4.6 Target DB 설정
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/rts/target/setup-sec-logging` | POST | 초당 로깅 설정 | target_statsec_setup |

### 4.7 Delta 관리
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/rts/delta/verify-and-clean` | POST | Delta 검증 및 정리 | delta_cleanup |

### 4.8 데이터 수집 모니터링
| 엔드포인트 | 메서드 | 역할 | Blueprint |
|-----------|--------|------|-----------|
| `/api/data-collection/status` | GET | 데이터 수집 현황 | data_monitor_1min |

---

## 5. DB 연결 흐름

### 5.1 애플리케이션 시작 시
```
1. app.py 실행
   ↓
2. app/__init__.py의 create_app() 호출
   ↓
3. _auto_load_db_setup() 실행
   ↓
4. app/shared_db.py 모듈 로드
   ↓
5. _load_config_from_mongodb() 자동 실행
   ↓
6. MongoDB에서 기본 프로필 로드 (is_default=True)
   ↓
7. Repo DB 설정을 _db_connections['repo']에 저장
   (Target DB는 APM_DB_INFO에서 선택해야 함)
```

### 5.2 DB 설정 플로우
```
[방법 1: API로 설정]
POST /rts/db/setup (JSON body: target_db, repo_db)
   ↓
shared_db.set_db_config('target', config)
shared_db.set_db_config('repo', config)
   ↓
_db_connections에 저장 (메모리)
   ↓
db_config.json에 백업 저장
   ↓
(선택) MongoDB에도 저장

[방법 2: MongoDB 프로필 사용]
MongoDB connection_profiles 컬렉션에서 기본 프로필 로드
   ↓
get_connection_profile(profile_id)
   ↓
repo_config_id로 entries 배열에서 Repo DB 설정 찾기
   ↓
shared_db._db_connections['repo']에 저장
```

### 5.3 DB 연결 사용
```
[API에서 DB 사용]
from app.shared_db import get_connection

with get_connection('target') as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT ...")
        results = cur.fetchall()
```

### 5.4 DB 타입별 연결 로직
```
get_connection(db_type)
   ↓
get_db_config(db_type)  # _db_connections에서 설정 가져오기
   ↓
_infer_db_engine(config, default)  # DB 타입 추론
   ↓
[Oracle]
_build_oracle_dsn(config)
   ↓
_normalize_service_type()  # 'sid' 또는 'service_name'
   ↓
oracledb.makedsn()
   ↓
oracledb.connect()

[PostgreSQL]
psycopg2.connect()
```

---

## 6. 파일 간 연관성

### 6.1 계층 구조
```
[Layer 1: 진입점]
app.py
   ↓
[Layer 2: 앱 팩토리]
app/__init__.py
   ↓
[Layer 3: Blueprint 관리]
app/routes/__init__.py
   ↓
[Layer 4: API 라우트]
app/routes/rts_routes.py
app/routes/rts_compare.py
app/routes/mongodb_config.py
app/routes/db_selector.py
... (기타 라우트)
   ↓
[Layer 5: 서비스 & 모델]
app/shared_db.py  ←→  app/services/db_config_service.py
                  ↓
                app/models/db_config.py
   ↓
[Layer 6: 데이터베이스]
Oracle DB (Target/Repo)
PostgreSQL DB (Repo)
MongoDB (설정 저장소)
```

### 6.2 핵심 연관 관계

#### `app/shared_db.py` (중앙 허브)
```
연결 대상:
- app/__init__.py (DB 설정 자동 로드)
- app/services/db_config_service.py (MongoDB 프로필 로드)
- app/routes/rts_routes.py (DB 설정 API)
- app/routes/rts_compare.py (DB 연결 사용)
- app/routes/rts_data_routes.py (DB 연결 사용)
- app/routes/statsec_gemma_auto.py (DB 연결 사용)
- app/routes/delta_cleanup.py (DB 연결 사용)
- app/routes/target_statsec_setup.py (DB 연결 사용)
- ... (거의 모든 라우트)
```

#### `app/services/db_config_service.py` (MongoDB 서비스)
```
연결 대상:
- app/models/db_config.py (MONGODB_CONFIG, DBConfigModel)
- app/shared_db.py (프로필 로드)
- app/routes/mongodb_config.py (CRUD API)
- app/routes/db_selector.py (DB 목록 조회)
```

#### `app/routes/rts_compare.py` (실시간 비교)
```
연결 대상:
- app/shared_db.py (DB 연결)
- realtime_compare_viewer.html (웹 UI)
```

#### `app/routes/mongodb_config.py` (MongoDB 설정 API)
```
연결 대상:
- app/services/db_config_service.py (CRUD 서비스)
- db_config_manager.html (웹 UI)
```

#### `app/routes/db_selector.py` (DB 선택 UI)
```
연결 대상:
- app/services/db_config_service.py (DB 목록 조회)
- app/shared_db.py (선택된 DB 저장)
- templates/db_selector.html (웹 UI)
```

### 6.3 데이터 흐름 예시

#### 예시 1: DB 설정 및 비교
```
1. 사용자가 setup_db_from_json.py 실행
   ↓
2. POST /rts/db/setup (db_setup.json 내용 전송)
   ↓
3. rts_routes.py: set_db_config('target', config)
                 set_db_config('repo', config)
   ↓
4. shared_db.py: _db_connections에 저장 + db_config.json 백업
   ↓
5. 사용자가 GET /rts/compare/stream?db_id=1 호출
   ↓
6. rts_compare.py: _connect_target(), _connect_repo() 호출
   ↓
7. shared_db.py: get_connection('target'), get_connection('repo')
   ↓
8. Oracle DB 연결 → 쿼리 실행 → 결과 반환
   ↓
9. SSE로 실시간 스트리밍 응답
```

#### 예시 2: MongoDB에서 Repo DB 설정 생성 및 사용
```
1. 사용자가 POST /api/v1/rts/mongodb_configs (Repo DB 정보)
   ↓
2. mongodb_config.py: db_config_service.create_db_entry(data)
   ↓
3. db_config_service.py: MongoDB에 UUID 생성하여 저장
   ↓
4. 사용자가 db_selector 웹 페이지에서 Repo DB 선택
   ↓
5. db_selector.py: POST /rts/db-selector/select-repo
   ↓
6. db_config_service.get_all_db_configs() → 선택된 config 찾기
   ↓
7. shared_db.set_db_config('repo', config)
   ↓
8. db_config.json 업데이트
   ↓
9. 이후 /rts/compare 등의 API에서 Repo DB 사용 가능
```

---

## 7. 주요 클래스 및 함수 요약

### 클래스
- `DBConfigModel` (app/models/db_config.py)
  - Target/Repo DB 설정 데이터 모델
  - 연결 프로필 모델
  
- `DBConfigService` (app/services/db_config_service.py)
  - MongoDB CRUD 서비스
  - 전역 싱글톤 인스턴스: `db_config_service`

### 전역 변수 (app/shared_db.py)
- `_db_connections`: Target/Repo DB 연결 정보 (메모리)
- `_default_profile_id`: 현재 사용 중인 MongoDB 프로필 ID
- `_config_file`: JSON 백업 파일 경로

### 핵심 함수
- `create_app()`: Flask 앱 팩토리 (app/__init__.py)
- `_auto_load_db_setup()`: DB 설정 자동 로드 (app/__init__.py)
- `_load_config_from_mongodb()`: MongoDB 프로필 로드 (app/shared_db.py)
- `get_connection(db_type)`: DB 연결 객체 반환 (app/shared_db.py)
- `set_db_config(db_type, config)`: DB 설정 저장 (app/shared_db.py)
- `get_all_db_configs()`: 모든 DB 설정 조회 (app/services/db_config_service.py)
- `stream_compare_db_data()`: 실시간 DB 비교 스트리밍 (app/routes/rts_compare.py)
- `_fetch_repo_data_parallel()`: Repo DB 병렬 조회 (app/routes/rts_compare.py)

---

## 8. 설정 파일

### `.env`
```bash
# MongoDB 연결 설정
MONGODB_URI=mongodb://...
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_USERNAME=admin
MONGODB_PASSWORD=...
MONGODB_AUTH_SOURCE=admin
MONGODB_DATABASE=repo_test
MONGODB_DB_CONFIGS_COLLECTION=test
MONGODB_CONNECTION_PROFILES_COLLECTION=connection_profiles

# Repo DB 연결 설정 (data_monitor_1min.py 등에서 사용)
REPO_DB_USER=C##TEST_2507
REPO_DB_PASSWORD=TEST_2507
REPO_DB_HOST=10.20.132.40
REPO_DB_PORT=1521
REPO_DB_SERVICE=orcl

# Gemini API
GEMMA_API_KEY=AIzaSyB...
```

### `db_config.json` (백업용)
```json
{
  "target": {
    "host": "10.20.132.101",
    "port": 1521,
    "service": "oracle19",
    "user": "C##TEST_2507",
    "password": "TEST_2507"
  },
  "repo": {
    "host": "10.20.132.40",
    "port": 1521,
    "service": "orcl",
    "user": "C##TEST_2507",
    "password": "TEST_2507"
  }
}
```

### `db_setup.json` (테스트용)
```json
{
  "target_db": {
    "host": "10.20.132.101",
    "port": 1521,
    "service": "oracle19",
    "user": "C##TEST_2507",
    "password": "TEST_2507"
  },
  "repo_db": {
    "host": "10.20.132.40",
    "port": 1521,
    "service": "orcl",
    "user": "C##TEST_2507",
    "password": "TEST_2507"
  }
}
```

---

## 9. 실행 방법

### 서버 실행
```bash
# 가상환경 활성화 (Windows)
activate

# Flask 서버 실행
python app.py
```

### API 테스트
```bash
# Python 스크립트로 DB 설정
python setup_db_from_json.py

# 또는 curl로 직접 호출 (PowerShell)
$body = Get-Content db_setup.json -Raw
Invoke-RestMethod -Uri "http://localhost:5000/rts/db/setup" -Method POST -Body $body -ContentType "application/json"
```

### 주요 URL
- Swagger UI: `http://localhost:5000/apidocs`
- DB Manager: `http://localhost:5000/db-manager`
- DB Selector: `http://localhost:5000/rts/db-selector/`
- 실시간 비교: `http://localhost:5000/realtime-compare`
- 실시간 차트: `http://localhost:5000/realtime-chart`

---

## 10. 프로젝트 특징

### 장점
1. **명확한 계층 구조**: 진입점 → 팩토리 → Blueprint → 서비스 → 모델
2. **중앙 집중식 DB 관리**: `app/shared_db.py`가 모든 DB 연결 관리
3. **MongoDB 기반 설정 관리**: 유연한 DB 접속 정보 저장 및 프로필 관리
4. **실시간 모니터링**: SSE 기반 실시간 스트리밍
5. **병렬 처리**: Repo DB 조회 시 ThreadPoolExecutor 사용
6. **AI 통합**: Google Gemini API로 자동 리포트 생성
7. **Swagger 통합**: 모든 API 자동 문서화

### 개선 가능 영역
1. **파일 정리**: 백업/테스트 파일 정리 (`*_bak.py`, `*_clean.py`, `test_*.py`)
2. **에러 처리**: 일부 API에서 예외 처리 강화 필요
3. **로깅**: 구조화된 로깅 시스템 도입
4. **테스트**: 단위 테스트 및 통합 테스트 추가
5. **설정 관리**: 환경별 설정 파일 분리 (dev, prod)
6. **보안**: 비밀번호 암호화, API 인증/인가

---

이 문서는 프로젝트의 전체 구조와 각 파일의 역할, 연관성을 정리한 것입니다.
추가 질문이나 특정 부분에 대한 상세 분석이 필요하면 말씀해 주세요.

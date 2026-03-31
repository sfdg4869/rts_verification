# Repo DB 저장 및 연결 흐름

## 📋 전체 흐름 개요

```
1. Repo DB 정보 입력 및 저장
   ↓
2. MongoDB (repo_test.test)에 저장
   ↓
3. Repo DB 선택 (shared_db에 메모리 저장)
   ↓
4. PostgreSQL 연결 생성
   ↓
5. APM_DB_INFO 테이블 조회
   ↓
6. Target DB 목록 반환
```

## 🔄 상세 동작 과정

### 1️⃣ Repo DB 정보 저장

**API**: `POST /rts/mongodb/configs`

```json
{
  "name": "Production Repo",
  "host": "10.10.47.72",
  "database": "SH_REPO",
  "db_user": "postgres",
  "db_password": "postgres",
  "db_port": 5433,
  "db_type": "PostgreSQL",
  ...
}
```

**저장 위치**: 
- MongoDB: `repo_test` 데이터베이스
- 컬렉션: `test`
- 구조: `entries` 배열에 저장
- 반환: `config_id` (예: `"entry_0"`)

### 2️⃣ Repo DB 선택 및 메모리 저장

**API**: `POST /rts/mongodb/repo-db/select`

```json
{
  "config_id": "entry_0"
}
```

**동작 과정**:

```
1. MongoDB에서 config_id로 Repo DB 정보 조회
   ↓
2. shared_db.set_db_config('repo', repo_config) 호출
   ↓
3. _db_connections['repo']에 저장 (메모리)
   {
     'host': '10.10.47.72',
     'port': 5433,
     'user': 'postgres',
     'password': 'postgres',
     'database': 'SH_REPO',
     'service': 'SH_REPO'
   }
   ↓
4. PostgreSQLService로 APM_DB_INFO 조회
   ↓
5. Target DB 목록 반환
```

### 3️⃣ Repo DB 연결 및 정보 읽기

**연결 함수**: `shared_db.get_connection('repo')`

**동작 과정**:

```python
# 1. 메모리에서 설정 읽기
config = get_db_config('repo')  # _db_connections['repo'] 반환

# 2. PostgreSQL 연결 생성
import psycopg2
connection = psycopg2.connect(
    host=config['host'],           # 10.10.47.72
    port=config['port'],            # 5433
    database=config['database'],   # SH_REPO
    user=config['user'],            # postgres
    password=config['password'],    # postgres
    sslmode='disable'
)

# 3. 쿼리 실행
cursor = connection.cursor()
cursor.execute("SELECT * FROM apm_db_info")
results = cursor.fetchall()
```

## 📊 데이터 흐름도

```
┌─────────────────────────────────────────┐
│  사용자 입력: Repo DB 정보              │
│  (host, port, database, user, password) │
└─────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  POST /rts/mongodb/configs               │
│  → MongoDB에 저장                        │
│  → repo_test.test.entries[]              │
│  → config_id: "entry_0" 반환            │
└─────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  POST /rts/mongodb/repo-db/select        │
│  → config_id로 MongoDB에서 조회          │
│  → shared_db.set_db_config('repo', ...)  │
│  → _db_connections['repo']에 저장        │
└─────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  PostgreSQL 연결 생성                    │
│  → psycopg2.connect(...)                │
│  → Repo DB에 연결                        │
└─────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  APM_DB_INFO 테이블 조회                 │
│  → SELECT * FROM apm_db_info             │
│  → Target DB 목록 반환                   │
└─────────────────────────────────────────┘
```

## 🔑 주요 함수 및 위치

### 1. 저장 관련
- **파일**: `app/services/db_config_service.py`
- **함수**: `create_db_entry()`
- **저장 위치**: MongoDB `repo_test.test.entries[]`

### 2. 선택 및 메모리 저장
- **파일**: `app/shared_db.py`
- **함수**: `set_db_config('repo', config)`
- **저장 위치**: `_db_connections['repo']` (메모리)

### 3. 연결 생성
- **파일**: `app/shared_db.py`
- **함수**: `get_connection('repo')`
- **연결 방식**: PostgreSQL (psycopg2)

### 4. 정보 읽기
- **파일**: `app/services/postgresql_service.py`
- **함수**: `get_apm_db_info()`
- **쿼리**: `SELECT * FROM apm_db_info`

## 💡 사용 예시

### 예시 1: Repo DB 생성 후 자동 연결

```python
# 1. Repo DB 생성
POST /rts/mongodb/configs
{
  "host": "10.10.47.72",
  "database": "SH_REPO",
  "db_user": "postgres",
  "db_password": "postgres",
  "db_port": 5433
}

# 응답: 자동으로 선택되고 Target DB 목록 조회됨
{
  "status": "success",
  "config_id": "entry_0",
  "selected_repo": {...},
  "target_dbs": [...],
  "count": 5
}
```

### 예시 2: 기존 Repo DB 선택

```python
# 1. Repo DB 선택
POST /rts/mongodb/repo-db/select
{
  "config_id": "entry_0"
}

# 응답: Target DB 목록 자동 조회
{
  "status": "success",
  "selected_repo": {...},
  "target_dbs": [...],
  "count": 5
}
```

### 예시 3: 다른 API에서 Repo DB 사용

```python
# 다른 라우트에서 사용
from app.shared_db import get_connection

def some_api():
    # Repo DB 연결
    conn = get_connection('repo')  # PostgreSQL 연결 반환
    
    # 쿼리 실행
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM some_table")
    results = cursor.fetchall()
    
    # 연결 종료
    conn.close()
```

## ⚠️ 주의사항

1. **메모리 저장**: `_db_connections['repo']`는 메모리에만 저장되므로 애플리케이션 재시작 시 초기화됩니다.
2. **MongoDB에서 로드**: 애플리케이션 시작 시 기본 프로파일이 있으면 자동으로 로드됩니다.
3. **PostgreSQL 연결**: `psycopg2-binary` 패키지가 설치되어 있어야 합니다.
4. **데이터베이스 필드**: PostgreSQL 연결 시 `database` 필드가 필수입니다.


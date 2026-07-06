# Repo DB → Target DB 연결 로드맵

## 🗺️ 전체 프로세스 로드맵

```
┌─────────────────────────────────────────────────────────────────┐
│                    STEP 1: Repo DB 저장                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  POST /rts/mongodb/configs          │
        │  Repo DB 정보 입력                   │
        │  {                                  │
        │    "name": "Production Repo",      │
        │    "host": "10.10.47.72",           │
        │    "database": "SH_REPO",           │
        │    "db_user": "postgres",           │
        │    "db_password": "postgres",       │
        │    "db_port": 5433,                 │
        │    ...                              │
        │  }                                  │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  MongoDB 저장                       │
        │  Database: repo_test                │
        │  Collection: test                   │
        │  Structure: entries[]               │
        │  → config_id: "entry_0" 반환        │
        └─────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 2: Repo DB 선택 및 메모리 저장                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  POST /rts/mongodb/repo-db/select    │
        │  {                                  │
        │    "config_id": "entry_0"           │
        │  }                                  │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  MongoDB에서 Repo DB 정보 조회      │
        │  repo_test.test.entries[0]         │
        │  → host, port, database, user, ... │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  shared_db.set_db_config('repo')    │
        │  → _db_connections['repo'] 저장    │
        │  메모리에 Repo DB 설정 저장          │
        └─────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 3: Repo DB 연결 및 APM_DB_INFO 조회                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  PostgreSQL 연결 생성               │
        │  shared_db.get_connection('repo')  │
        │  → psycopg2.connect(...)           │
        │  → Repo DB에 연결                   │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  APM_DB_INFO 테이블 조회             │
        │  SELECT * FROM apm_db_info          │
        │  → Target DB 목록 반환              │
        │  [                                  │
        │    {                                │
        │      "db_id": 1,                    │
        │      "instance_name": "ORCL",       │
        │      "host_ip": "10.10.47.98",      │
        │      "sid": "ORA19C",               │
        │      "lsnr_port": 1521,             │
        │      "db_user": "maxgauge",         │
        │      "db_password": "password",     │
        │      ...                            │
        │    },                               │
        │    ...                              │
        │  ]                                  │
        └─────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 4: Target DB 선택 및 연결                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  POST /rts/mongodb/target-db/select │
        │  {                                  │
        │    "db_id": 1                       │
        │  }                                  │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  APM_DB_INFO에서 db_id로 조회        │
        │  → Target DB 정보 추출              │
        │  {                                  │
        │    "host": "10.10.47.98",           │
        │    "port": 1521,                    │
        │    "sid": "ORA19C",                 │
        │    "user": "maxgauge",              │
        │    "password": "password"           │
        │  }                                  │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  shared_db.set_db_config('target')   │
        │  → _db_connections['target'] 저장   │
        │  메모리에 Target DB 설정 저장        │
        └─────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  Oracle 연결 생성                    │
        │  shared_db.get_connection('target') │
        │  → oracledb.connect(...)           │
        │  → Target DB에 연결                 │
        └─────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 5: Repo DB & Target DB 연결 완료              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  두 DB 모두 연결 완료                │
        │  - Repo DB: PostgreSQL 연결         │
        │  - Target DB: Oracle 연결           │
        │  → 데이터 조회 및 작업 수행 가능      │
        └─────────────────────────────────────┘
```

## 📊 상세 데이터 흐름도

```
┌──────────────────────────────────────────────────────────────┐
│  MongoDB (repo_test.test)                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  entries: [                                          │   │
│  │    {                                                  │   │
│  │      "name": "Production Repo",                       │   │
│  │      "host": "10.10.47.72",                          │   │
│  │      "database": "SH_REPO",                          │   │
│  │      "user": "postgres",                              │   │
│  │      "password": "postgres",                          │   │
│  │      "port": 5433,                                    │   │
│  │      ...                                              │   │
│  │    }                                                  │   │
│  │  ]                                                    │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                        │
                        │ config_id: "entry_0"
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  shared_db._db_connections (메모리)                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  'repo': {                                           │   │
│  │    'host': '10.10.47.72',                            │   │
│  │    'port': 5433,                                      │   │
│  │    'database': 'SH_REPO',                            │   │
│  │    'user': 'postgres',                                │   │
│  │    'password': 'postgres'                             │   │
│  │  }                                                    │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                        │
                        │ PostgreSQL 연결
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  Repo DB (PostgreSQL)                                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Database: SH_REPO                                   │   │
│  │  ┌──────────────────────────────────────────────┐   │   │
│  │  │  Table: apm_db_info                           │   │   │
│  │  │  ┌────┬──────────────┬──────────┬─────┬───┐  │   │   │
│  │  │  │db_id│instance_name│host_ip   │sid  │...│  │   │   │
│  │  │  ├────┼──────────────┼──────────┼─────┼───┤  │   │   │
│  │  │  │ 1  │ORCL          │10.47.98 │ORA19│...│  │   │   │
│  │  │  │ 2  │ORCL2         │10.47.99 │ORA20│...│  │   │   │
│  │  │  │ ...│...           │...      │...  │...│  │   │   │
│  │  │  └────┴──────────────┴──────────┴─────┴───┘  │   │   │
│  │  └──────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                        │
                        │ db_id: 1 선택
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  shared_db._db_connections (메모리)                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  'target': {                                        │   │
│  │    'host': '10.10.47.98',                           │   │
│  │    'port': 1521,                                     │   │
│  │    'sid': 'ORA19C',                                 │   │
│  │    'user': 'maxgauge',                               │   │
│  │    'password': 'password',                           │   │
│  │    'db_id': 1                                        │   │
│  │  }                                                   │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                        │
                        │ Oracle 연결
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  Target DB (Oracle)                                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  SID: ORA19C                                          │   │
│  │  Host: 10.10.47.98:1521                              │   │
│  │  → 연결 완료                                          │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

## 🔄 단계별 상세 설명

### STEP 1: Repo DB 저장
```
사용자 입력
    ↓
POST /rts/mongodb/configs
    ↓
MongoDB 저장 (repo_test.test.entries[])
    ↓
config_id 반환 ("entry_0")
    ↓
자동으로 Repo DB 선택 및 Target DB 목록 조회
```

### STEP 2: Repo DB 선택
```
config_id 전달
    ↓
MongoDB에서 Repo DB 정보 조회
    ↓
shared_db._db_connections['repo']에 저장
    ↓
메모리에 설정 완료
```

### STEP 3: Repo DB 연결 및 APM_DB_INFO 조회
```
get_connection('repo') 호출
    ↓
_db_connections['repo']에서 설정 읽기
    ↓
PostgreSQL 연결 생성 (psycopg2)
    ↓
SELECT * FROM apm_db_info 실행
    ↓
Target DB 목록 반환
```

### STEP 4: Target DB 선택
```
db_id 선택 (예: 1)
    ↓
APM_DB_INFO에서 해당 db_id 조회
    ↓
Target DB 정보 추출
    ↓
shared_db._db_connections['target']에 저장
    ↓
메모리에 설정 완료
```

### STEP 5: Target DB 연결
```
get_connection('target') 호출
    ↓
_db_connections['target']에서 설정 읽기
    ↓
Oracle 연결 생성 (oracledb)
    ↓
Target DB 연결 완료
```

## 🎯 API 호출 시퀀스

```
1. POST /rts/mongodb/configs
   → Repo DB 저장
   → 자동 선택 및 Target DB 목록 조회
   → 응답: { config_id, selected_repo, target_dbs }

2. (선택사항) POST /rts/mongodb/repo-db/select
   → 다른 Repo DB 선택
   → Target DB 목록 자동 조회
   → 응답: { selected_repo, target_dbs }

3. POST /rts/mongodb/target-db/select
   → Target DB 선택
   → shared_db에 저장
   → 응답: { target_db }

4. 이후 다른 API에서 사용
   → get_connection('repo') → PostgreSQL 연결
   → get_connection('target') → Oracle 연결
```

## 📝 메모리 저장 구조

```python
_db_connections = {
    'repo': {
        'host': '10.10.47.72',
        'port': 5433,
        'database': 'SH_REPO',
        'user': 'postgres',
        'password': 'postgres',
        'service': 'SH_REPO'
    },
    'target': {
        'host': '10.10.47.98',
        'port': 1521,
        'sid': 'ORA19C',
        'user': 'maxgauge',
        'password': 'password',
        'db_id': 1
    }
}
```

## 🔗 연결 체인

```
MongoDB (영구 저장)
    │
    │ config_id로 조회
    ↓
shared_db._db_connections (메모리 저장)
    │
    │ get_connection() 호출
    ↓
실제 DB 연결
    ├─→ Repo DB (PostgreSQL)
    │   └─→ APM_DB_INFO 조회
    │       └─→ Target DB 정보 추출
    │
    └─→ Target DB (Oracle)
        └─→ 데이터 조회 및 작업
```

## 💡 핵심 포인트

1. **MongoDB**: Repo DB 정보 영구 저장
2. **메모리**: 현재 선택된 Repo/Target DB 설정 (빠른 접근)
3. **PostgreSQL**: Repo DB 연결 → APM_DB_INFO 조회
4. **Oracle**: Target DB 연결 → 데이터 조회

## 🚀 사용 예시

### 전체 프로세스 한 번에
```json
// 1. Repo DB 생성 및 자동 선택
POST /rts/mongodb/configs
{
  "host": "10.10.47.72",
  "database": "SH_REPO",
  "db_user": "postgres",
  "db_password": "postgres",
  "db_port": 5433
}

// 응답: Target DB 목록까지 자동 조회
{
  "config_id": "entry_0",
  "selected_repo": {...},
  "target_dbs": [
    {"db_id": 1, "instance_name": "ORCL", ...},
    {"db_id": 2, "instance_name": "ORCL2", ...}
  ]
}

// 2. Target DB 선택
POST /rts/mongodb/target-db/select
{
  "db_id": 1
}

// 3. 이제 두 DB 모두 연결됨
// → get_connection('repo') → PostgreSQL
// → get_connection('target') → Oracle
```


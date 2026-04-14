# 신규 Repo 점검 (Step 1~6) 실행 쿼리 정리

이 문서는 `/api/v2/rts/check/run-repo-new` 및 `/api/v2/rts/check/run-repo-new-job` 실행 시
`app/services/new_repo_check_service.py`에서 수행하는 Step별 쿼리를 정리한 내용입니다.

## Repo 엔진별 Step 5 입력 (API / UI)

- `GET /api/v2/rts/check/repo-status` 응답에 `engine`(`oracle` | `postgresql` 등), `schema_name`(Repo 설정값)이 포함됩니다. `rts_check.html`은 이 값으로 Step 5 입력란을 분기합니다.
- `POST .../run-repo-new`, `POST .../run-repo-new-job` JSON 본문:
  - **Oracle Repo**: `repo_db_id_list` — 비우면 Target의 `db_id`가 기본으로 사용됩니다.
  - **PostgreSQL Repo**: 요청 JSON의 **`schema_name`** (구호환: `repo_pg_schema`) — 비우면 Repo 연결 설정의 `schema_name`을 사용합니다. `tests/pg_step5.txt`의 `${schema_name}` 치환에 쓰입니다.

## Step 1. Target 권한 확인

- 목적: 테스트 수행에 필요한 권한(ALTER SYSTEM, DBMS_LOCK, DBMS_UTILITY) 확인
- 실행 쿼리:

```sql
SELECT * FROM USER_SYS_PRIVS WHERE privilege = 'ALTER SYSTEM';
```

```sql
SELECT 'USER_TAB_PRIVS' AS source, privilege AS dbms_lock_privilege
FROM user_tab_privs
WHERE table_name = 'DBMS_LOCK' AND privilege = 'EXECUTE'
UNION all
SELECT 'DBA_TAB_PRIVS' AS source, privilege AS dbms_lock_privilege
FROM dba_tab_privs
WHERE grantee = 'PUBLIC' AND table_name = 'DBMS_LOCK' AND privilege = 'EXECUTE'
UNION all
SELECT 'USER_SYS_PRIVS' AS source, privilege AS dbms_lock_privilege
FROM user_sys_privs
WHERE privilege = 'EXECUTE ANY PROCEDURE';
```

```sql
SELECT 'USER_TAB_PRIVS' AS source, privilege AS dbms_utility_privilege
FROM user_tab_privs
WHERE table_name = 'DBMS_UTILITY' AND privilege = 'EXECUTE'
UNION all
SELECT 'DBA_TAB_PRIVS' AS source, privilege AS dbms_utility_privilege
FROM dba_tab_privs
WHERE grantee = 'PUBLIC' AND table_name = 'DBMS_UTILITY' AND privilege = 'EXECUTE'
UNION all
SELECT 'USER_SYS_PRIVS' AS source, privilege AS dbms_utility_privilege
FROM user_sys_privs
WHERE privilege = 'EXECUTE ANY PROCEDURE';
```

## Step 2. 테스트 프로시저 생성

- 목적: `qs_sql_test_proc1`~`qs_sql_test_proc5` 생성
- 우선순위:
  1) `tests/step2.txt` 원문 SQL 실행  
  2) 파일이 없거나 비어있으면 서비스 내 fallback `CREATE OR REPLACE PROCEDURE ...`

## Step 3. PLSQL FOR LOOP 실행

- 목적: 프로시저 반복 호출(기본 100회) + CHECKPOINT 수행
- 실행 전 기준값 수집 쿼리(실행수 delta 계산용):

```sql
SELECT sql_id, executions, sql_fulltext
FROM v$sql
WHERE 1=1
  AND (
        upper(sql_text) LIKE upper('DECLARE%test_func_list %')
        OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%')
        OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%')
        OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%')
        OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%')
        OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%')
  );
```

- 본 실행 SQL:
  1) `tests/step3.txt` (없으면 `tests/case3.txt`)  
  2) 파일이 없으면 서비스 내 fallback DECLARE 블록

## Step 4. Target v$sql 결과 조회

- 목적: 실제 실행된 SQL_ID/시그니처(sql_hash, sql_addr, plan_hash) 및 실행 메트릭 수집
- 우선순위:
  1) `tests/step4.txt` (없으면 `tests/case4.txt`)  
  2) 파일이 없으면 서비스 내 fallback `SELECT ... FROM v$sql ...`

- 비고:
  - 조회 결과로 `case_actual_sql_ids`, `case_signatures` 생성
  - `case_sql_ids`: Target `v$sql`에서 잡힌 **실제** sql_id(케이스키별). `case_sql_ids_expected`: 문서상 기대 sql_id.
  - `step5_sql_id_per_case`: Step 5 Repo 쿼리의 `IN (...)` / 치환에 쓰인 **케이스별 sql_id**(화면·매칭은 이 값과 Repo 행의 `sql_id`만 일치시킴).
  - Step3 대비 `case_exec_before/after/delta` 계산

## Step 5. Repo 수집 결과 조회 (Oracle/PG 분기)

- 목적: Repo 수집 테이블(`ORA_SQL_ELAPSE`, `ORA_SQL_STAT_10MIN`) 검증
- 공통 입력값:
  - `${db_id_list}`: 예) `255` 또는 `255,256`
  - `${partition_date}`: 예) `260413`
  - `${logging_time}`: 예) `2026-04-13 22:00:00`

### 5-1. Oracle Repo

- 우선순위:
  1) `tests/ora_step5.txt` (없으면 `tests/ora_case5.txt`)  
  2) 파일이 없으면 서비스 내 fallback SQL
- 템플릿 내 SQL_ID는 Step4 실제 SQL_ID로 치환되어 실행됨.

예시 형태:

```sql
... a.partition_key > TO_NUMBER('${partition_date}' || '000')
AND a.time >= TO_TIMESTAMP('${logging_time}', 'YYYY-MM-DD HH24:MI:SS')
AND a.sql_id IN ('9fbyurzh8tr4c', ...)
```

### 5-2. PostgreSQL Repo

- 우선순위:
  1) `tests/pg_step5.txt` (없으면 `tests/pg_case5.txt`)  
  2) 파일이 없으면 서비스 내 fallback SQL
- `${schema_name}`/`${partition_date}`/`${logging_time}`를 치환하여 실행.
- 스키마는 요청의 **`schema_name`**(또는 구호환 `repo_pg_schema`)가 있으면 그 값(검증 후), 없으면 Repo 연결 설정의 `schema_name`을 사용합니다.

## Step 6. SYS 정리

- 목적: Shared Pool purge + 테스트 프로시저 drop
- 우선순위:
  1) `tests/step6.txt` 원문 SQL 블록 실행 (DECLARE 블록 분리 실행)  
  2) 파일이 없으면 최소 Drop fallback

- 비고:
  - `maxgauge` 하드코딩 텍스트는 실행 시 Target 사용자로 치환됨
  - `sys_password` 미입력 시 Step6은 skip

## 참고 파일

- 서비스: `app/services/new_repo_check_service.py`
- 템플릿:
  - `tests/step2.txt`
  - `tests/step3.txt`
  - `tests/step4.txt`
  - `tests/ora_step5.txt`
  - `tests/pg_step5.txt`
  - `tests/step6.txt`

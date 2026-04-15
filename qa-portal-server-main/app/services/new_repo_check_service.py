import time
import threading
import os
import re
from datetime import datetime, date, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List, Optional
import oracledb

from app.services.oracle_service import OracleService
from app.shared_db import get_connection, release_connection, _infer_db_engine, get_db_config

def _fetch_dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0].lower() for d in (cur.description or [])]
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        row_dict: Dict[str, Any] = {}
        for i in range(len(cols)):
            v = r[i]
            if hasattr(v, "read"):
                try:
                    v = v.read()
                except Exception:
                    v = str(v)
            if isinstance(v, (datetime, date, dt_time)):
                v = v.isoformat()
            elif isinstance(v, Decimal):
                v = float(v)
            row_dict[cols[i]] = v
        out.append(row_dict)
    return out


def _query_rows_with_retry(cursor, sql: str, max_attempts: int = 4, wait_seconds: int = 8) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for i in range(max_attempts):
        cursor.execute(sql)
        rows = _fetch_dict_rows(cursor)
        if rows:
            return rows
        if i < max_attempts - 1:
            time.sleep(wait_seconds)
    return rows


def _sql_id_literal_list(sql_ids: List[str]) -> str:
    clean = []
    for sid in sql_ids:
        s = str(sid or "").strip().lower()
        if s and s not in clean:
            clean.append(s)
    if not clean:
        return "'47kdtjwcwv4kx','7wy9vfzb9u8zu'"
    return ",".join([f"'{s}'" for s in clean])


def _decode_sql_id_order_expr(column: str, ordered_ids: List[str]) -> str:
    clean = []
    for sid in ordered_ids:
        s = str(sid or "").strip().lower()
        if s and s not in clean:
            clean.append(s)
    if not clean:
        return "999"
    parts = [f"'{sid}', {idx}" for idx, sid in enumerate(clean, start=1)]
    return f"decode({column}, " + ", ".join(parts) + ", 999)"


def _number_literal_list(values: List[Any]) -> str:
    nums: List[str] = []
    for v in values:
        try:
            n = int(v)
        except Exception:
            continue
        s = str(n)
        if s not in nums:
            nums.append(s)
    return ",".join(nums)


def _read_sql_template(filename: str) -> str:
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path = os.path.join(root_dir, "tests", filename)
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def _extract_oracle_create_procedures(sql_text: str) -> List[str]:
    if not sql_text.strip():
        return []
    pattern = re.compile(
        r"CREATE\s+OR\s+REPLACE\s+PROCEDURE[\s\S]*?END\s*;",
        flags=re.IGNORECASE,
    )
    return [m.group(0).strip() for m in pattern.finditer(sql_text)]


def _split_oracle_blocks(sql_text: str) -> List[str]:
    """
    Oracle PL/SQL DECLARE…END; 블록을 올바르게 분리한다.
    BEGIN / IF / LOOP / CASE 로 depth+1, END 로 depth-1.
    END IF / END LOOP / END CASE 는 단일 닫기 토큰으로 처리.
    내부 세미콜론이 있어도 블록 전체를 보존한다.
    """
    if not sql_text.strip():
        return []

    # 라인 주석 제거, 블록 주석 제거
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        lines.append(line)
    text = "\n".join(lines)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

    # BEGIN / IF / LOOP / CASE → depth+1 / END → depth-1
    # DECLARE 자체는 depth 변화 없음 (BEGIN 이 뒤따라 옴)
    _OPENERS = {"BEGIN", "IF", "LOOP", "CASE"}
    _END_SUFFIXES = {"IF", "LOOP", "CASE"}  # END IF / END LOOP / END CASE
    _TOK = re.compile(r"\b(DECLARE|BEGIN|END|IF|LOOP|CASE)\b", re.IGNORECASE)

    blocks: List[str] = []
    pos = 0
    text_len = len(text)

    while pos < text_len:
        # 다음 DECLARE 위치 탐색
        m = re.search(r"\bDECLARE\b", text[pos:], re.IGNORECASE)
        if not m:
            break

        block_start = pos + m.start()
        scan = block_start + len("DECLARE")
        depth = 0

        while scan < text_len:
            tm = _TOK.search(text, scan)
            if not tm:
                scan = text_len
                break

            word = tm.group(1).upper()

            if word == "DECLARE":
                # 중첩 DECLARE: depth 변화 없음, BEGIN 이 depth 올림
                scan = tm.end()

            elif word in _OPENERS:
                depth += 1
                scan = tm.end()

            elif word == "END":
                # END 다음에 IF / LOOP / CASE 가 오면 그 토큰을 건너뜀 (닫기 키워드)
                depth -= 1
                scan = tm.end()

                if depth == 0:
                    # END 뒤 선택적 키워드 + 세미콜론까지 포함
                    end_extra = re.match(
                        r"\s*(?:IF|LOOP|CASE)?\s*;", text[scan:], re.IGNORECASE
                    )
                    if end_extra:
                        scan += end_extra.end()
                    block = text[block_start:scan].strip()
                    # 세미콜론으로 끝나지 않으면 추가
                    if not block.endswith(";"):
                        block += ";"
                    blocks.append(block)
                    pos = scan
                    break
                else:
                    # END IF / END LOOP 처리: 다음 키워드가 IF/LOOP/CASE 이면 건너뜀
                    next_kw = re.match(r"\s*(\w+)", text[scan:])
                    if next_kw and next_kw.group(1).upper() in _END_SUFFIXES:
                        scan += next_kw.end()
            else:
                scan = tm.end()
        else:
            # DECLARE를 찾았지만 닫는 END가 없음
            break

    return blocks


def _extract_case_key_from_sql_text(sql_text: str) -> Optional[str]:
    txt = str(sql_text or "").upper()
    if not txt:
        return None
    if txt.startswith("BEGIN QS_SQL_TEST_PROC1"):
        return "case2_proc1"
    if txt.startswith("BEGIN QS_SQL_TEST_PROC2"):
        return "case3_proc2"
    if txt.startswith("BEGIN QS_SQL_TEST_PROC3"):
        return "case4_proc3"
    if txt.startswith("BEGIN QS_SQL_TEST_PROC4"):
        return "case5_proc4"
    if txt.startswith("BEGIN QS_SQL_TEST_PROC5"):
        return "case6_proc5"
    return None

def run_new_repo_check(
    db_id: int,
    target_config: Dict[str, Any],
    sys_password: Optional[str] = None,
    progress_callback=None,
    case4_loops: int = 200,
    case5_loops: int = 30,
    case5_rows: int = 50000,
    repo_db_id_list: Optional[str] = None,
    repo_partition_date: Optional[str] = None,
    repo_logging_time: Optional[str] = None,
    repo_schema_name: Optional[str] = None,
    pol_repo_config_id: Optional[str] = None,
    pol_repo_schema_name: Optional[str] = None,
    pol_repo_db_id_list: Optional[str] = None,
    stop_after_step4: bool = False,
) -> Dict[str, Any]:
    def _progress(done: int, total: int, step_name: str, step_status: str) -> None:
        if progress_callback:
            progress_callback(done, total, step_name, step_status)
    
    def _uniq(items: List[Any]) -> List[Any]:
        out: List[Any] = []
        for it in items:
            if it is None:
                continue
            if it not in out:
                out.append(it)
        return out

    def _safe_pg_schema(raw: Any) -> str:
        s = str(raw or "").strip()
        if not s:
            return "public"
        ok = (s[0].isalpha() or s[0] == "_") and all(ch.isalnum() or ch == "_" for ch in s)
        return s if ok else "public"

    def _safe_db_id_list(raw: Optional[str], default_db_id: int) -> str:
        src = str(raw or "").strip()
        if not src:
            return str(int(default_db_id))
        vals = []
        for token in src.split(","):
            t = token.strip()
            if t.isdigit():
                vals.append(str(int(t)))
        return ",".join(vals) if vals else str(int(default_db_id))

    case_order = [
        "case2_proc1",
        "case3_proc2",
        "case4_proc3",
        "case5_proc4",
        "case6_proc5",
    ]
    fixed_sql_ids = [
        "fbf2t9pw12ynm",
        "ga6tfrmnrzwax",
        "af5w9c5uq9mf5",
        "9yv10yjy19dva",
        "9t1uh0g3vjnd7",
    ]

    overall_status = "pass"
    error_msg = ""
    result_data: Dict[str, Any] = {
        "target_vsql": [],
        "repo_elapse": [],
        "repo_stat": [],
        "repo_elapse_vsql": [],
        "repo_stat_vsql": [],
        "repo_elapse_pol": [],
        "repo_stat_pol": [],
        "case_sql_ids": {},
        "case_actual_sql_ids": {},
        "case_signatures": {},
        "permission_checks": {},
        "logging_time": "",
        "partition_date": "",
        "repo_engine": "",
        "repo_schema": "",
        "case_exec_before": {},
        "case_exec_after": {},
        "case_exec_delta": {},
        "step5_sql_id_per_case": {},
    }

    target_user = str(target_config.get("user") or "").strip()
    target_host = target_config.get("host")
    target_port = int(target_config.get("port") or 1521)
    target_sid  = target_config.get("sid")
    target_password = target_config.get("password")
    from app.services.dg_password_service import decrypt_dg_password
    target_password = decrypt_dg_password(target_password)

    # Connection Pool 대신 직접 연결 사용 (장시간 PL/SQL 실행 시 Pool과 충돌 방지)
    try:
        dsn = oracledb.makedsn(target_host, target_port, sid=target_sid)
        conn = oracledb.connect(user=target_user, password=target_password, dsn=dsn)
    except Exception as e:
        _progress(1, 6, "target_connect", "fail")
        return {"overall_status": "error", "error": f"Target DB ({target_user}) 연결 실패: {e}", "data": result_data}
    _progress(1, 6, "target_connect", "pass")
    target_svc = None  # 직접 연결이므로 svc 불필요

    logging_anchor = datetime.now()

    try:
        cur = conn.cursor()

        # Step 1: 권한 확인
        _progress(1, 6, "target_permission_check(ALTER_SYSTEM)", "running")
        alter_sql = "SELECT * FROM USER_SYS_PRIVS WHERE privilege = 'ALTER SYSTEM'"
        lock_sql = (
            "SELECT 'USER_TAB_PRIVS' AS source, privilege AS dbms_lock_privilege "
            "FROM user_tab_privs WHERE table_name = 'DBMS_LOCK' AND privilege = 'EXECUTE' "
            "UNION all "
            "SELECT 'DBA_TAB_PRIVS' AS source, privilege AS dbms_lock_privilege "
            "FROM dba_tab_privs WHERE grantee = 'PUBLIC' AND table_name = 'DBMS_LOCK' AND privilege = 'EXECUTE' "
            "UNION all "
            "SELECT 'USER_SYS_PRIVS' AS source, privilege AS dbms_lock_privilege "
            "FROM user_sys_privs WHERE privilege = 'EXECUTE ANY PROCEDURE'"
        )
        util_sql = (
            "SELECT 'USER_TAB_PRIVS' AS source, privilege AS dbms_utility_privilege "
            "FROM user_tab_privs WHERE table_name = 'DBMS_UTILITY' AND privilege = 'EXECUTE' "
            "UNION all "
            "SELECT 'DBA_TAB_PRIVS' AS source, privilege AS dbms_utility_privilege "
            "FROM dba_tab_privs WHERE grantee = 'PUBLIC' AND table_name = 'DBMS_UTILITY' AND privilege = 'EXECUTE' "
            "UNION all "
            "SELECT 'USER_SYS_PRIVS' AS source, privilege AS dbms_utility_privilege "
            "FROM user_sys_privs WHERE privilege = 'EXECUTE ANY PROCEDURE'"
        )
        cur.execute(alter_sql)
        alter_rows = _fetch_dict_rows(cur)
        _progress(1, 6, "target_permission_check(DBMS_LOCK)", "running")
        cur.execute(lock_sql)
        lock_rows = _fetch_dict_rows(cur)
        _progress(1, 6, "target_permission_check(DBMS_UTILITY)", "running")
        cur.execute(util_sql)
        util_rows = _fetch_dict_rows(cur)
        result_data["permission_checks"] = {
            "alter_system_ok": bool(alter_rows),
            "dbms_lock_ok": bool(lock_rows),
            "dbms_utility_ok": bool(util_rows),
            "alter_rows": alter_rows,
            "dbms_lock_rows": lock_rows,
            "dbms_utility_rows": util_rows,
        }
        if not (bool(alter_rows) and bool(lock_rows) and bool(util_rows)):
            _progress(1, 6, "target_permission_check", "fail")
            return {
                "overall_status": "fail",
                "error": "필수 권한(ALTER SYSTEM / DBMS_LOCK / DBMS_UTILITY) 부족",
                "data": result_data,
            }
        _progress(1, 6, "target_permission_check", "pass")

        # Step 2: 테스트 프로시저 생성
        for p in ("qs_sql_test_proc1", "qs_sql_test_proc2", "qs_sql_test_proc3", "qs_sql_test_proc4", "qs_sql_test_proc5"):
            try:
                cur.execute(f"DROP PROCEDURE {p}")
            except Exception:
                pass
        step2_sql = _read_sql_template("step2.txt")
        proc_blocks = _extract_oracle_create_procedures(step2_sql)
        if proc_blocks:
            for block in proc_blocks:
                cur.execute(block)
        else:
            cur.execute("""
                CREATE OR REPLACE PROCEDURE qs_sql_test_proc1 as
                    v_start TIMESTAMP;
                BEGIN
                    v_start := SYSTIMESTAMP;
                    WHILE (SYSTIMESTAMP - v_start) < INTERVAL '3' SECOND LOOP
                        NULL;
                    END LOOP;
                END;
            """)
            cur.execute("""
                CREATE OR REPLACE PROCEDURE qs_sql_test_proc2 as
                    v_start TIMESTAMP;
                BEGIN
                    v_start := SYSTIMESTAMP;
                    WHILE (SYSTIMESTAMP - v_start) < INTERVAL '1' SECOND LOOP
                        NULL;
                    END LOOP;
                END;
            """)
            cur.execute("""
                CREATE OR REPLACE PROCEDURE qs_sql_test_proc3 as
                    v_start TIMESTAMP;
                BEGIN
                    v_start := SYSTIMESTAMP;
                    WHILE (SYSTIMESTAMP - v_start) < INTERVAL '0.05' SECOND LOOP
                        NULL;
                    END LOOP;
                END;
            """)
            cur.execute("""
                CREATE OR REPLACE PROCEDURE qs_sql_test_proc4 as
                    v_start TIMESTAMP;
                BEGIN
                    v_start := SYSTIMESTAMP;
                    WHILE (SYSTIMESTAMP - v_start) < INTERVAL '0.01' SECOND LOOP
                        NULL;
                    END LOOP;
                END;
            """)
            cur.execute("""
                CREATE OR REPLACE PROCEDURE qs_sql_test_proc5 as
                    v_start TIMESTAMP;
                BEGIN
                    v_start := SYSTIMESTAMP;
                    WHILE (SYSTIMESTAMP - v_start) < INTERVAL '0.001' SECOND LOOP
                        NULL;
                    END LOOP;
                END;
            """)
        conn.commit()
        _progress(2, 6, "target_create_test_procedures", "pass")

        # Step 3: PLSQL FOR LOOP 실행
        logging_anchor = datetime.now()
        _progress(3, 6, "target_run_plsql_loop(start)", "running")
        cur.execute("""
            SELECT sql_id, executions, sql_fulltext
            FROM v$sql
            WHERE 1=1
              AND (
                    upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%')
              )
        """)
        pre_rows = _fetch_dict_rows(cur)
        pre_exec: Dict[str, int] = {}
        for row in pre_rows:
            case_key = _extract_case_key_from_sql_text(row.get("sql_fulltext"))
            if not case_key:
                continue
            try:
                exec_val = int(row.get("executions") or 0)
            except Exception:
                exec_val = 0
            pre_exec[case_key] = max(pre_exec.get(case_key, 0), exec_val)
        result_data["case_exec_before"] = pre_exec
        hb_stop = threading.Event()

        def _loop_heartbeat():
            started = time.time()
            while not hb_stop.wait(5):
                elapsed = int(time.time() - started)
                _progress(3, 6, f"target_run_plsql_loop(running {elapsed}s)", "running")

        hb_thread = threading.Thread(target=_loop_heartbeat, daemon=True, name="repo-new-step3-heartbeat")
        hb_thread.start()
        step3_sql = _read_sql_template("step3.txt") or _read_sql_template("case3.txt") or (
            "DECLARE\n"
            "        TYPE test_func_list IS TABLE OF VARCHAR(30);\n"
            "        v_funcs test_func_list := test_func_list('qs_sql_test_proc1', 'qs_sql_test_proc2', 'qs_sql_test_proc3', 'qs_sql_test_proc4', 'qs_sql_test_proc5');\n"
            "        test_execution NUMBER := 100;\n"
            "BEGIN\n"
            "        FOR i IN 1..v_funcs.COUNT LOOP\n"
            "                FOR j IN 1..test_execution LOOP\n"
            "                        EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';\n"
            "                        EXECUTE IMMEDIATE 'BEGIN ' || v_funcs(i) || '; END;';\n"
            "                END LOOP;\n"
            "        END LOOP;\n"
            "END;"
        )
        cur.execute(step3_sql)
        hb_stop.set()
        conn.commit()
        _progress(3, 6, "target_run_plsql_loop", "pass")

        # Step 4: v$sql 조회
        case4_sql = _read_sql_template("step4.txt") or _read_sql_template("case4.txt")
        if case4_sql.strip():
            case4_sql_exec = case4_sql.strip()
            if case4_sql_exec.endswith(";"):
                case4_sql_exec = case4_sql_exec[:-1]
            cur.execute(case4_sql_exec)
        else:
            cur.execute("""
                SELECT
                    sql_id,
                    plan_hash_value,
                    executions,
                    (elapsed_time/1000000) AS elapsed_us_to_sec,
                    CASE WHEN executions = 0 THEN NULL ELSE ((elapsed_time/executions)/1000000) END AS per_elapse_sec,
                    hash_value AS sql_hash,
                    RAWTOHEX(address) AS sql_addr,
                    sql_fulltext
                FROM v$sql
                WHERE 1=1
                AND (
                    upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%')
                )
                ORDER BY
                    CASE
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%') THEN 1
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%') THEN 2
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%') THEN 3
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%') THEN 4
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%') THEN 5
                        ELSE 6
                    END,
                    plan_hash_value
            """)
        target_rows = _fetch_dict_rows(cur)
        result_data["target_vsql"] = target_rows

        expected_case_sql_ids: Dict[str, str] = {
            "case2_proc1": "fbf2t9pw12ynm",
            "case3_proc2": "ga6tfrmnrzwax",
            "case4_proc3": "af5w9c5uq9mf5",
            "case5_proc4": "9yv10yjy19dva",
            "case6_proc5": "9t1uh0g3vjnd7",
        }
        case_sql_ids: Dict[str, str] = {}
        case_signatures: Dict[str, Dict[str, Any]] = {}
        post_exec: Dict[str, int] = {}
        for row in target_rows:
            txt = str(row.get("sql_fulltext") or "").upper()
            sid = str(row.get("sql_id") or "").strip().lower()
            try:
                exec_val = int(row.get("executions") or 0)
            except Exception:
                exec_val = 0
            sig = {
                "sql_id": sid or None,
                "sql_hash": row.get("sql_hash"),
                "sql_addr": str(row.get("sql_addr") or "").upper() or None,
                "sql_plan_hash": row.get("plan_hash_value"),
            }
            if txt.startswith("BEGIN QS_SQL_TEST_PROC1") and "case2_proc1" not in case_sql_ids:
                case_sql_ids["case2_proc1"] = sid
                case_signatures["case2_proc1"] = sig
                post_exec["case2_proc1"] = max(post_exec.get("case2_proc1", 0), exec_val)
            elif txt.startswith("BEGIN QS_SQL_TEST_PROC2") and "case3_proc2" not in case_sql_ids:
                case_sql_ids["case3_proc2"] = sid
                case_signatures["case3_proc2"] = sig
                post_exec["case3_proc2"] = max(post_exec.get("case3_proc2", 0), exec_val)
            elif txt.startswith("BEGIN QS_SQL_TEST_PROC3") and "case4_proc3" not in case_sql_ids:
                case_sql_ids["case4_proc3"] = sid
                case_signatures["case4_proc3"] = sig
                post_exec["case4_proc3"] = max(post_exec.get("case4_proc3", 0), exec_val)
            elif txt.startswith("BEGIN QS_SQL_TEST_PROC4") and "case5_proc4" not in case_sql_ids:
                case_sql_ids["case5_proc4"] = sid
                case_signatures["case5_proc4"] = sig
                post_exec["case5_proc4"] = max(post_exec.get("case5_proc4", 0), exec_val)
            elif txt.startswith("BEGIN QS_SQL_TEST_PROC5") and "case6_proc5" not in case_sql_ids:
                case_sql_ids["case6_proc5"] = sid
                case_signatures["case6_proc5"] = sig
                post_exec["case6_proc5"] = max(post_exec.get("case6_proc5", 0), exec_val)
        result_data["case_sql_ids"] = case_sql_ids
        result_data["case_actual_sql_ids"] = case_sql_ids
        result_data["case_sql_ids_expected"] = expected_case_sql_ids
        result_data["case_signatures"] = case_signatures
        result_data["case_exec_after"] = post_exec
        deltas: Dict[str, int] = {}
        for key in case_order:
            b = int(pre_exec.get(key, 0))
            a = int(post_exec.get(key, b))
            deltas[key] = max(0, a - b)
        result_data["case_exec_delta"] = deltas
        _progress(4, 6, "target_vsql_query", "pass")

    except Exception as e:
        overall_status = "error"
        error_msg = f"Step 1~4 Target DB Error: {str(e)}"
        _progress(4, 6, "target_step_error", "fail")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    if overall_status == "error":
        return {"overall_status": overall_status, "error": error_msg, "data": result_data}

    # Step 1~4만 실행 후 반환
    if stop_after_step4:
        result_data["stop_after_step4"] = True
        return {"overall_status": overall_status, "error": error_msg, "data": result_data}

    # Step 5: Repo 수집 결과 조회 (Oracle/PG 분기)
    try:
        repo_cfg = get_db_config("repo") or {}
        repo_engine = _infer_db_engine(repo_cfg, "postgresql")
        result_data["repo_engine"] = repo_engine
        schema_override = str(repo_schema_name or "").strip()
        schema_name = _safe_pg_schema(schema_override if schema_override else repo_cfg.get("schema_name"))
        result_data["repo_schema"] = schema_name

        actual_case_ids = result_data.get("case_actual_sql_ids") or {}
        expected_to_actual: Dict[str, str] = {}
        effective_sql_ids: List[str] = []
        step5_sql_id_per_case: Dict[str, str] = {}
        for idx, case_key in enumerate(case_order):
            expected_id = fixed_sql_ids[idx]
            actual_id = str(actual_case_ids.get(case_key) or expected_id).strip().lower()
            expected_to_actual[expected_id] = actual_id
            effective_sql_ids.append(actual_id)
            step5_sql_id_per_case[case_key] = actual_id
        effective_sql_ids = _uniq(effective_sql_ids) or fixed_sql_ids
        result_data["effective_sql_ids"] = effective_sql_ids
        result_data["step5_sql_id_per_case"] = step5_sql_id_per_case

        sql_id_filter = _sql_id_literal_list(effective_sql_ids)
        sql_id_order_expr = _decode_sql_id_order_expr("a.sql_id", effective_sql_ids)

        def _apply_actual_sql_ids(sql: str) -> str:
            out = sql
            for expected_id, actual_id in expected_to_actual.items():
                out = out.replace(expected_id, actual_id)
            return out

        default_partition_date = logging_anchor.strftime("%y%m%d")
        partition_date = str(repo_partition_date or default_partition_date).strip()
        if not (partition_date.isdigit() and len(partition_date) == 6):
            partition_date = default_partition_date
        default_logging_time = logging_anchor.strftime("%Y-%m-%d %H:%M:%S")
        logging_time_str = str(repo_logging_time or default_logging_time).strip()
        if len(logging_time_str) < 19:
            logging_time_str = default_logging_time
        db_id_list_str = _safe_db_id_list(repo_db_id_list, int(db_id))
        result_data["partition_date"] = partition_date
        result_data["logging_time"] = logging_time_str
        result_data["db_id_list"] = db_id_list_str

        def _build_step5_queries(
            engine: str,
            schema: str,
            id_list_str: str,
            part_date: str,
            log_time_str: str,
            id_filter: str,
            id_order_expr: str,
        ):
            """engine에 맞는 (q_elapse, q_stat) 쌍을 반환한다."""
            def _apply(sql: str) -> str:
                out = sql
                for exp_id, act_id in expected_to_actual.items():
                    out = out.replace(exp_id, act_id)
                return out

            if engine == "oracle":
                tmpl = _read_sql_template("ora_step5.txt") or _read_sql_template("ora_case5.txt")
                if tmpl.strip():
                    parts = [p.strip() for p in tmpl.split(";") if p.strip().upper().startswith("SELECT")]
                    if len(parts) >= 2:
                        qe = _apply(
                            parts[0]
                            .replace("${db_id_list}", id_list_str)
                            .replace("${partition_date}", part_date)
                            .replace("${logging_time}", log_time_str)
                        )
                        qs = _apply(
                            parts[1]
                            .replace("${db_id_list}", id_list_str)
                            .replace("${partition_date}", part_date)
                            .replace("${logging_time}", log_time_str)
                        )
                        return qe, qs
                qe = f"""
                    SELECT a.db_id, b.instance_name,
                           a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           count(*) AS execution_count,
                           sum(a.ELAPSE)/1000 AS total_elapse_sec,
                           (sum(a.ELAPSE)/count(*))/1000 AS per_elapse_ms_to_sec
                    FROM ora_sql_elapse a, apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({id_list_str})
                      AND a.partition_key > TO_NUMBER('{part_date}' || '000')
                      AND a.time >= TO_TIMESTAMP('{log_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                      AND a.sql_id in ({id_filter})
                    GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                    ORDER BY a.db_id, {id_order_expr}, a.sql_plan_hash
                """
                qs = f"""
                    SELECT a.db_id, b.instance_name,
                           a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           a.execution_count,
                           (a.elapsed_time/100) AS elapse_cs_to_sec,
                           CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100) END AS per_elapse_sec
                    FROM ora_sql_stat_10min a, apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({id_list_str})
                      AND a.partition_key > TO_NUMBER('{part_date}' || '000')
                      AND a.time >= TO_TIMESTAMP('{log_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                      AND a.sql_id in ({id_filter})
                    ORDER BY a.time, a.db_id, {id_order_expr}, a.time, a.sql_plan_hash
                """
                return qe, qs
            else:
                tmpl = _read_sql_template("pg_step5.txt") or _read_sql_template("pg_case5.txt")
                if tmpl.strip():
                    parts = [p.strip() for p in tmpl.split(";") if p.strip().upper().startswith("SELECT")]
                    if len(parts) >= 2:
                        qe = _apply(
                            parts[0]
                            .replace("${schema_name}", schema)
                            .replace("${partition_date}", part_date)
                            .replace("${logging_time}", f"'{log_time_str}'")
                        )
                        qs = _apply(
                            parts[1]
                            .replace("${schema_name}", schema)
                            .replace("${partition_date}", part_date)
                            .replace("${logging_time}", f"'{log_time_str}'")
                        )
                        return qe, qs
                pg_case_order = (
                    "CASE a.sql_id "
                    "WHEN '9fbyurzh8tr4c' THEN 1 "
                    "WHEN 'fbf2t9pw12ynm' THEN 2 "
                    "WHEN 'ga6tfrmnrzwax' THEN 3 "
                    "WHEN 'af5w9c5uq9mf5' THEN 4 "
                    "WHEN '9yv10yjy19dva' THEN 5 "
                    "WHEN '9t1uh0g3vjnd7' THEN 6 "
                    "ELSE 999 END"
                )
                qe = f"""
                    SELECT a.db_id, b.instance_name,
                           a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           count(*) as execution_count,
                           sum(a.elapse)/1000.0 AS total_elapse_sec,
                           (sum(a.elapse)/count(*))/1000.0 AS per_elapse_ms_to_sec
                    FROM {schema}.ora_sql_elapse a, public.apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({id_list_str})
                      AND a.partition_key > {part_date}000
                      AND a.time >= '{log_time_str}'::timestamp
                      AND a.sql_id in ({id_filter})
                    GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                    ORDER BY a.db_id, {pg_case_order}, a.sql_plan_hash
                """
                qs = f"""
                    SELECT a.db_id, b.instance_name,
                           a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           a.execution_count,
                           (a.elapsed_time/100.0) AS elapse_cs_to_sec,
                           CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100.0) END AS per_elapse_sec
                    FROM {schema}.ora_sql_stat_10min a, public.apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({id_list_str})
                      AND a.partition_key > {part_date}000
                      AND a.time >= '{log_time_str}'::timestamp
                      AND a.sql_id in ({id_filter})
                    ORDER BY a.db_id, {pg_case_order}, a.time, a.sql_plan_hash
                """
                return qe, qs

        repo_conn = get_connection("repo")
        repo_cur = repo_conn.cursor()

        q_elapse, q_stat = _build_step5_queries(
            engine=repo_engine,
            schema=schema_name,
            id_list_str=db_id_list_str,
            part_date=partition_date,
            log_time_str=logging_time_str,
            id_filter=sql_id_filter,
            id_order_expr=sql_id_order_expr,
        )

        result_data["repo_elapse"] = _query_rows_with_retry(repo_cur, q_elapse, max_attempts=4, wait_seconds=8)
        # ORA_SQL_STAT_10MIN은 MaxGauge가 10분 주기로 수집하므로 최대 ~10분 대기 (20회×30초)
        result_data["repo_stat"] = _query_rows_with_retry(repo_cur, q_stat, max_attempts=20, wait_seconds=30)
        result_data["repo_elapse_vsql"] = result_data["repo_elapse"]
        result_data["repo_stat_vsql"] = result_data["repo_stat"]
        _progress(5, 6, "repo_query_oracle_pg", "pass")
    except Exception as e:
        overall_status = "error"
        error_msg = f"Step 5 Repo DB Error: {str(e)}"
        _progress(5, 6, "repo_query_oracle_pg", "fail")
    finally:
        try:
            repo_cur.close()
        except Exception:
            pass
        try:
            release_connection("repo", repo_conn)
        except Exception:
            pass

    # Step 5b: POL Repo 조회 (pol_repo_config_id 제공 시, VSQL과 독립적으로 실행)
    if pol_repo_config_id and overall_status != "error":
        pol_conn = None
        pol_cur = None
        try:
            from app.shared_db import connect_repo_by_config_id
            pol_conn, pol_cfg = connect_repo_by_config_id(pol_repo_config_id)
            pol_cur = pol_conn.cursor()
            pol_engine = _infer_db_engine(pol_cfg, "postgresql")

            pol_schema = _safe_pg_schema(pol_repo_schema_name or pol_cfg.get("schema_name"))
            pol_id_list = _safe_db_id_list(pol_repo_db_id_list, int(db_id))

            pol_q_elapse, pol_q_stat = _build_step5_queries(
                engine=pol_engine,
                schema=pol_schema,
                id_list_str=pol_id_list,
                part_date=partition_date,
                log_time_str=logging_time_str,
                id_filter=sql_id_filter,
                id_order_expr=sql_id_order_expr,
            )
            result_data["repo_elapse_pol"] = _query_rows_with_retry(pol_cur, pol_q_elapse, max_attempts=4, wait_seconds=8)
            result_data["repo_stat_pol"] = _query_rows_with_retry(pol_cur, pol_q_stat, max_attempts=20, wait_seconds=30)
            result_data["pol_repo_engine"] = pol_engine
            result_data["pol_repo_schema"] = pol_schema
            result_data["pol_repo_db_id_list"] = pol_id_list
        except Exception as e:
            result_data["pol_repo_error"] = str(e)
        finally:
            try:
                pol_cur.close()
            except Exception:
                pass
            try:
                pol_conn.close()
            except Exception:
                pass

    # Step 6: SYS 정리
    clean_msg = "Cleanup attempted."
    if sys_password:
        try:
            dsn = oracledb.makedsn(target_cfg["host"], target_cfg["port"], sid=target_cfg["sid"])
            sys_conn = oracledb.connect(user="sys", password=sys_password, dsn=dsn, mode=oracledb.SYSDBA)
            scur = sys_conn.cursor()
            step6_sql = _read_sql_template("step6.txt")
            blocks = _split_oracle_blocks(step6_sql)
            if blocks:
                for b in blocks:
                    exec_sql = b.replace("testuser_name := 'maxgauge';", f"testuser_name := '{target_user}';")
                    exec_sql = exec_sql.replace("test_db_user varchar2(20) := 'maxgauge';", f"test_db_user varchar2(20) := '{target_user}';")
                    if exec_sql.strip().endswith(";"):
                        exec_sql = exec_sql.strip()[:-1]
                    scur.execute(exec_sql)
            else:
                scur.execute(f"""
                    DECLARE
                        testuser_name varchar2(64) := '{target_user}';
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc1';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc2';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc3';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc4';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc5';
                    END;
                """)
            sys_conn.commit()
            scur.close()
            sys_conn.close()
            clean_msg = "SYS shared pool purge + drop procedures success"
            _progress(6, 6, "target_cleanup_sys", "pass")
        except Exception as e:
            clean_msg = f"SYS cleanup failed: {e}"
            _progress(6, 6, "target_cleanup_sys", "fail")
    else:
        clean_msg = "SYS password not provided; cleanup skipped"
        _progress(6, 6, "target_cleanup_sys", "skip")

    result_data["cleanup_info"] = clean_msg

    return {
        "overall_status": overall_status,
        "error": error_msg,
        "data": result_data,
    }


def run_step5_repo_only(
    db_id: int,
    repo_partition_date: Optional[str] = None,
    repo_logging_time: Optional[str] = None,
    repo_schema_name: Optional[str] = None,
    repo_db_id_list: Optional[str] = None,
    pol_repo_config_id: Optional[str] = None,
    pol_repo_schema_name: Optional[str] = None,
    pol_repo_db_id_list: Optional[str] = None,
    target_config: Optional[Dict[str, Any]] = None,
    sys_password: Optional[str] = None,
    progress_callback=None,
) -> Dict[str, Any]:
    """
    Step 5+6 독립 실행: Target 프로시저 없이 Repo DB에서
    ORA_SQL_ELAPSE / ORA_SQL_STAT_10MIN 조회 후 SYS 정리(선택)를 수행합니다.
    """
    def _progress(done: int, total: int, step_name: str, step_status: str) -> None:
        if progress_callback:
            progress_callback(done, total, step_name, step_status)

    def _safe_pg_schema(raw: Any) -> str:
        s = str(raw or "").strip()
        if not s:
            return "public"
        ok = (s[0].isalpha() or s[0] == "_") and all(ch.isalnum() or ch == "_" for ch in s)
        return s if ok else "public"

    def _safe_db_id_list(raw: Optional[str], default_db_id: int) -> str:
        src = str(raw or "").strip()
        if not src:
            return str(int(default_db_id))
        vals = [str(int(t.strip())) for t in src.split(",") if t.strip().isdigit()]
        return ",".join(vals) if vals else str(int(default_db_id))

    case_order = [
        "case2_proc1", "case3_proc2", "case4_proc3", "case5_proc4", "case6_proc5",
    ]
    fixed_sql_ids = [
        "fbf2t9pw12ynm", "ga6tfrmnrzwax", "af5w9c5uq9mf5", "9yv10yjy19dva", "9t1uh0g3vjnd7",
    ]

    result_data: Dict[str, Any] = {
        "target_vsql": [],
        "repo_elapse": [], "repo_stat": [],
        "repo_elapse_vsql": [], "repo_stat_vsql": [],
        "repo_elapse_pol": [], "repo_stat_pol": [],
        "case_sql_ids": {k: v for k, v in zip(case_order, fixed_sql_ids)},
        "case_actual_sql_ids": {},
        "case_signatures": {},
        "step5_sql_id_per_case": {k: v for k, v in zip(case_order, fixed_sql_ids)},
        "effective_sql_ids": fixed_sql_ids,
        "permission_checks": {},
        "logging_time": "", "partition_date": "",
        "repo_engine": "", "repo_schema": "",
        "case_exec_before": {}, "case_exec_after": {}, "case_exec_delta": {},
    }

    overall_status = "pass"
    error_msg = ""

    logging_anchor = datetime.now()

    # partition_date / logging_time 처리
    default_partition_date = logging_anchor.strftime("%y%m%d")
    partition_date = str(repo_partition_date or default_partition_date).strip()
    if not (partition_date.isdigit() and len(partition_date) == 6):
        partition_date = default_partition_date
    default_logging_time = logging_anchor.strftime("%Y-%m-%d %H:%M:%S")
    logging_time_str = str(repo_logging_time or default_logging_time).strip()
    if len(logging_time_str) < 19:
        logging_time_str = default_logging_time

    result_data["partition_date"] = partition_date
    result_data["logging_time"] = logging_time_str

    sql_id_filter = _sql_id_literal_list(fixed_sql_ids)
    sql_id_order_expr = _decode_sql_id_order_expr("a.sql_id", fixed_sql_ids)
    db_id_list_str = _safe_db_id_list(repo_db_id_list, int(db_id))
    result_data["db_id_list"] = db_id_list_str

    # ── Target DB > V$SQL 조회 (target_config 제공 시) ──────────────
    _progress(0, 3, "target_vsql_query", "running")
    if target_config:
        t_conn = None
        try:
            from app.services.dg_password_service import decrypt_dg_password
            t_user = str(target_config.get("user") or "").strip()
            t_pw = decrypt_dg_password(target_config.get("password") or "")
            t_dsn = oracledb.makedsn(
                target_config["host"],
                int(target_config.get("port") or 1521),
                sid=target_config.get("sid") or target_config.get("service_name") or "",
            )
            t_conn = oracledb.connect(user=t_user, password=t_pw, dsn=t_dsn)
            t_cur = t_conn.cursor()
            case4_sql = _read_sql_template("step4.txt") or _read_sql_template("case4.txt")
            if case4_sql.strip():
                q4 = case4_sql.strip().rstrip(";")
                t_cur.execute(q4)
            else:
                t_cur.execute("""
                    SELECT sql_id, plan_hash_value, executions,
                           (elapsed_time/1000000) AS elapsed_us_to_sec,
                           CASE WHEN executions = 0 THEN NULL
                                ELSE ((elapsed_time/executions)/1000000) END AS per_elapse_sec,
                           hash_value AS sql_hash,
                           RAWTOHEX(address) AS sql_addr,
                           sql_fulltext
                    FROM v$sql
                    WHERE upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%')
                       OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%')
                       OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%')
                       OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%')
                       OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%')
                    ORDER BY
                        CASE WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%') THEN 1
                             WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%') THEN 2
                             WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%') THEN 3
                             WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%') THEN 4
                             WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%') THEN 5
                             ELSE 6 END, plan_hash_value
                """)
            result_data["target_vsql"] = _fetch_dict_rows(t_cur)
            t_cur.close()
            _progress(1, 3, "target_vsql_query", "pass")
        except Exception as e:
            result_data["target_vsql_error"] = str(e)
            _progress(1, 3, "target_vsql_query", "fail")
        finally:
            try:
                if t_conn:
                    t_conn.close()
            except Exception:
                pass
    else:
        _progress(1, 3, "target_vsql_query", "skip")

    # ── VSQL Repo 조회 ──────────────────────────────────────────────
    _progress(1, 3, "repo_query_vsql", "running")
    repo_conn = None
    repo_cur = None
    try:
        repo_cfg = get_db_config("repo") or {}
        repo_engine = _infer_db_engine(repo_cfg, "postgresql")
        result_data["repo_engine"] = repo_engine
        schema_override = str(repo_schema_name or "").strip()
        schema_name = _safe_pg_schema(schema_override if schema_override else repo_cfg.get("schema_name"))
        result_data["repo_schema"] = schema_name

        pg_case_order = (
            "CASE a.sql_id "
            + " ".join([f"WHEN '{sid}' THEN {i}" for i, sid in enumerate(fixed_sql_ids, 1)])
            + " ELSE 999 END"
        )

        if repo_engine == "oracle":
            tmpl = _read_sql_template("ora_step5.txt") or _read_sql_template("ora_case5.txt")
            if tmpl.strip():
                parts = [p.strip() for p in tmpl.split(";") if p.strip().upper().startswith("SELECT")]
                if len(parts) >= 2:
                    q_elapse = parts[0].replace("${db_id_list}", db_id_list_str).replace("${partition_date}", partition_date).replace("${logging_time}", logging_time_str)
                    q_stat   = parts[1].replace("${db_id_list}", db_id_list_str).replace("${partition_date}", partition_date).replace("${logging_time}", logging_time_str)
                else:
                    q_elapse = q_stat = None
            else:
                q_elapse = q_stat = None
            if not q_elapse:
                q_elapse = f"""
                    SELECT a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           count(*) AS execution_count,
                           sum(a.ELAPSE)/1000 AS total_elapse_sec,
                           (sum(a.ELAPSE)/count(*))/1000 AS per_elapse_ms_to_sec
                    FROM ora_sql_elapse a, apm_db_info b
                    WHERE a.db_id = b.db_id AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > TO_NUMBER('{partition_date}' || '000')
                      AND a.time >= TO_TIMESTAMP('{logging_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                      AND a.sql_id in ({sql_id_filter})
                    GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                    ORDER BY a.db_id, {sql_id_order_expr}, a.sql_plan_hash
                """
                q_stat = f"""
                    SELECT a.db_id, b.instance_name, a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           a.execution_count,
                           (a.elapsed_time/100) AS elapse_cs_to_sec,
                           CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100) END AS per_elapse_sec
                    FROM ora_sql_stat_10min a, apm_db_info b
                    WHERE a.db_id = b.db_id AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > TO_NUMBER('{partition_date}' || '000')
                      AND a.time >= TO_TIMESTAMP('{logging_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                      AND a.sql_id in ({sql_id_filter})
                    ORDER BY a.time, a.db_id, {sql_id_order_expr}, a.time, a.sql_plan_hash
                """
        else:
            tmpl = _read_sql_template("pg_step5.txt") or _read_sql_template("pg_case5.txt")
            if tmpl.strip():
                parts = [p.strip() for p in tmpl.split(";") if p.strip().upper().startswith("SELECT")]
                if len(parts) >= 2:
                    q_elapse = parts[0].replace("${schema_name}", schema_name).replace("${partition_date}", partition_date).replace("${logging_time}", f"'{logging_time_str}'")
                    q_stat   = parts[1].replace("${schema_name}", schema_name).replace("${partition_date}", partition_date).replace("${logging_time}", f"'{logging_time_str}'")
                else:
                    q_elapse = q_stat = None
            else:
                q_elapse = q_stat = None
            if not q_elapse:
                q_elapse = f"""
                    SELECT a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           count(*) as execution_count,
                           sum(a.elapse)/1000.0 AS total_elapse_sec,
                           (sum(a.elapse)/count(*))/1000.0 AS per_elapse_ms_to_sec
                    FROM {schema_name}.ora_sql_elapse a, public.apm_db_info b
                    WHERE a.db_id = b.db_id AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > {partition_date}000
                      AND a.time >= '{logging_time_str}'::timestamp
                      AND a.sql_id in ({sql_id_filter})
                    GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                    ORDER BY a.db_id, {pg_case_order}, a.sql_plan_hash
                """
                q_stat = f"""
                    SELECT a.db_id, b.instance_name, a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           a.execution_count,
                           (a.elapsed_time/100.0) AS elapse_cs_to_sec,
                           CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100.0) END AS per_elapse_sec
                    FROM {schema_name}.ora_sql_stat_10min a, public.apm_db_info b
                    WHERE a.db_id = b.db_id AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > {partition_date}000
                      AND a.time >= '{logging_time_str}'::timestamp
                      AND a.sql_id in ({sql_id_filter})
                    ORDER BY a.db_id, {pg_case_order}, a.time, a.sql_plan_hash
                """

        repo_conn = get_connection("repo")
        repo_cur = repo_conn.cursor()
        result_data["repo_elapse"] = _query_rows_with_retry(repo_cur, q_elapse, max_attempts=4, wait_seconds=8)
        result_data["repo_stat"]   = _query_rows_with_retry(repo_cur, q_stat,   max_attempts=4, wait_seconds=10)
        result_data["repo_elapse_vsql"] = result_data["repo_elapse"]
        result_data["repo_stat_vsql"]   = result_data["repo_stat"]
        _progress(2, 3, "repo_query_vsql", "pass")
    except Exception as e:
        overall_status = "error"
        error_msg = f"VSQL Repo Error: {str(e)}"
        _progress(2, 3, "repo_query_vsql", "fail")
    finally:
        try:
            repo_cur.close()
        except Exception:
            pass
        try:
            release_connection("repo", repo_conn)
        except Exception:
            pass

    # ── POL Repo 조회 ──────────────────────────────────────────────
    if pol_repo_config_id and overall_status != "error":
        pol_conn = None
        pol_cur = None
        try:
            from app.shared_db import connect_repo_by_config_id
            pol_conn, pol_cfg = connect_repo_by_config_id(pol_repo_config_id)
            pol_cur = pol_conn.cursor()
            pol_engine = _infer_db_engine(pol_cfg, "postgresql")
            pol_schema = _safe_pg_schema(pol_repo_schema_name or pol_cfg.get("schema_name"))
            pol_id_list = _safe_db_id_list(pol_repo_db_id_list, int(db_id))
            result_data["pol_repo_engine"] = pol_engine
            result_data["pol_repo_schema"] = pol_schema
            result_data["pol_repo_db_id_list"] = pol_id_list

            pg_case_order_pol = (
                "CASE a.sql_id "
                + " ".join([f"WHEN '{sid}' THEN {i}" for i, sid in enumerate(fixed_sql_ids, 1)])
                + " ELSE 999 END"
            )

            if pol_engine == "oracle":
                tmpl = _read_sql_template("ora_step5.txt") or _read_sql_template("ora_case5.txt")
                if tmpl.strip():
                    parts = [p.strip() for p in tmpl.split(";") if p.strip().upper().startswith("SELECT")]
                    if len(parts) >= 2:
                        pq_e = parts[0].replace("${db_id_list}", pol_id_list).replace("${partition_date}", partition_date).replace("${logging_time}", logging_time_str)
                        pq_s = parts[1].replace("${db_id_list}", pol_id_list).replace("${partition_date}", partition_date).replace("${logging_time}", logging_time_str)
                    else:
                        pq_e = pq_s = None
                else:
                    pq_e = pq_s = None
                if not pq_e:
                    pq_e = f"""
                        SELECT a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                               count(*) AS execution_count,
                               sum(a.ELAPSE)/1000 AS total_elapse_sec,
                               (sum(a.ELAPSE)/count(*))/1000 AS per_elapse_ms_to_sec
                        FROM ora_sql_elapse a, apm_db_info b
                        WHERE a.db_id = b.db_id AND a.db_id IN ({pol_id_list})
                          AND a.partition_key > TO_NUMBER('{partition_date}' || '000')
                          AND a.time >= TO_TIMESTAMP('{logging_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                          AND a.sql_id in ({sql_id_filter})
                        GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                        ORDER BY a.db_id, {sql_id_order_expr}, a.sql_plan_hash
                    """
                    pq_s = f"""
                        SELECT a.db_id, b.instance_name, a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                               a.execution_count,
                               (a.elapsed_time/100) AS elapse_cs_to_sec,
                               CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100) END AS per_elapse_sec
                        FROM ora_sql_stat_10min a, apm_db_info b
                        WHERE a.db_id = b.db_id AND a.db_id IN ({pol_id_list})
                          AND a.partition_key > TO_NUMBER('{partition_date}' || '000')
                          AND a.time >= TO_TIMESTAMP('{logging_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                          AND a.sql_id in ({sql_id_filter})
                        ORDER BY a.time, a.db_id, {sql_id_order_expr}, a.time, a.sql_plan_hash
                    """
            else:
                tmpl = _read_sql_template("pg_step5.txt") or _read_sql_template("pg_case5.txt")
                if tmpl.strip():
                    parts = [p.strip() for p in tmpl.split(";") if p.strip().upper().startswith("SELECT")]
                    if len(parts) >= 2:
                        pq_e = parts[0].replace("${schema_name}", pol_schema).replace("${partition_date}", partition_date).replace("${logging_time}", f"'{logging_time_str}'")
                        pq_s = parts[1].replace("${schema_name}", pol_schema).replace("${partition_date}", partition_date).replace("${logging_time}", f"'{logging_time_str}'")
                    else:
                        pq_e = pq_s = None
                else:
                    pq_e = pq_s = None
                if not pq_e:
                    pq_e = f"""
                        SELECT a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                               count(*) as execution_count,
                               sum(a.elapse)/1000.0 AS total_elapse_sec,
                               (sum(a.elapse)/count(*))/1000.0 AS per_elapse_ms_to_sec
                        FROM {pol_schema}.ora_sql_elapse a, public.apm_db_info b
                        WHERE a.db_id = b.db_id AND a.db_id IN ({pol_id_list})
                          AND a.partition_key > {partition_date}000
                          AND a.time >= '{logging_time_str}'::timestamp
                          AND a.sql_id in ({sql_id_filter})
                        GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                        ORDER BY a.db_id, {pg_case_order_pol}, a.sql_plan_hash
                    """
                    pq_s = f"""
                        SELECT a.db_id, b.instance_name, a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                               a.execution_count,
                               (a.elapsed_time/100.0) AS elapse_cs_to_sec,
                               CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100.0) END AS per_elapse_sec
                        FROM {pol_schema}.ora_sql_stat_10min a, public.apm_db_info b
                        WHERE a.db_id = b.db_id AND a.db_id IN ({pol_id_list})
                          AND a.partition_key > {partition_date}000
                          AND a.time >= '{logging_time_str}'::timestamp
                          AND a.sql_id in ({sql_id_filter})
                        ORDER BY a.db_id, {pg_case_order_pol}, a.time, a.sql_plan_hash
                    """

            result_data["repo_elapse_pol"] = _query_rows_with_retry(pol_cur, pq_e, max_attempts=4, wait_seconds=8)
            result_data["repo_stat_pol"]   = _query_rows_with_retry(pol_cur, pq_s, max_attempts=4, wait_seconds=10)
            _progress(3, 3, "repo_query_pol", "pass")
        except Exception as e:
            result_data["pol_repo_error"] = str(e)
            _progress(3, 3, "repo_query_pol", "fail")
        finally:
            try:
                pol_cur.close()
            except Exception:
                pass
            try:
                pol_conn.close()
            except Exception:
                pass
    else:
        _progress(3, 3, "repo_query_pol", "skip")

    # ── Step 6: SYS 정리 (target_config + sys_password 제공 시) ────────
    clean_msg = "SYS password not provided; cleanup skipped"
    if sys_password and target_config:
        try:
            from app.services.dg_password_service import decrypt_dg_password
            tpw = decrypt_dg_password(target_config.get("password", ""))
            target_user = str(target_config.get("user") or "").strip()
            dsn6 = oracledb.makedsn(target_config["host"], int(target_config.get("port") or 1521), sid=target_config["sid"])
            sys_conn = oracledb.connect(user="sys", password=sys_password, dsn=dsn6, mode=oracledb.SYSDBA)
            scur = sys_conn.cursor()
            step6_sql = _read_sql_template("step6.txt")
            blocks = _split_oracle_blocks(step6_sql)
            if blocks:
                for b in blocks:
                    exec_sql = b.replace("testuser_name := 'maxgauge';", f"testuser_name := '{target_user}';")
                    exec_sql = exec_sql.replace("test_db_user varchar2(20) := 'maxgauge';", f"test_db_user varchar2(20) := '{target_user}';")
                    if exec_sql.strip().endswith(";"):
                        exec_sql = exec_sql.strip()[:-1]
                    scur.execute(exec_sql)
            else:
                scur.execute(f"""
                    DECLARE testuser_name varchar2(64) := '{target_user}';
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc1';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc2';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc3';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc4';
                        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || testuser_name || '.qs_sql_test_proc5';
                    END;
                """)
            sys_conn.commit()
            scur.close()
            sys_conn.close()
            clean_msg = "SYS shared pool purge + drop procedures success"
        except Exception as e:
            clean_msg = f"SYS cleanup failed: {e}"
    result_data["cleanup_info"] = clean_msg

    return {
        "overall_status": overall_status,
        "error": error_msg,
        "data": result_data,
    }

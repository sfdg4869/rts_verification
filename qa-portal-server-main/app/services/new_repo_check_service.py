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
    if not sql_text.strip():
        return []
    lines = []
    for line in sql_text.splitlines():
        t = line.strip()
        if t.startswith("--") or t.startswith("/*") or t.startswith("*/"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    parts = re.split(r";\s*\n", cleaned)
    blocks: List[str] = []
    for p in parts:
        q = p.strip()
        if not q:
            continue
        if q.upper().startswith("DECLARE"):
            blocks.append(q + ";")
    return blocks


def _extract_case_key_from_sql_text(sql_text: str) -> Optional[str]:
    txt = str(sql_text or "").upper()
    if not txt:
        return None
    if "DECLARE" in txt and "TEST_FUNC_LIST" in txt:
        return "case1_plsql"
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
        "case1_plsql",
        "case2_proc1",
        "case3_proc2",
        "case4_proc3",
        "case5_proc4",
        "case6_proc5",
    ]
    fixed_sql_ids = [
        "9fbyurzh8tr4c",
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
                    upper(sql_text) LIKE upper('DECLARE%test_func_list %')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%')
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
                    upper(sql_text) LIKE upper('DECLARE%test_func_list %')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%')
                    OR upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%')
                )
                ORDER BY
                    CASE
                        WHEN upper(sql_text) LIKE upper('DECLARE%test_func_list %') THEN 1
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc1%') THEN 2
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc2%') THEN 3
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc3%') THEN 4
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc4%') THEN 5
                        WHEN upper(sql_text) LIKE upper('BEGIN qs_sql_test_proc5%') THEN 6
                        ELSE 7
                    END,
                    plan_hash_value
            """)
        target_rows = _fetch_dict_rows(cur)
        result_data["target_vsql"] = target_rows

        expected_case_sql_ids: Dict[str, str] = {
            "case1_plsql": "9fbyurzh8tr4c",
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
            if ("DECLARE" in txt and "TEST_FUNC_LIST" in txt) and "case1_plsql" not in case_sql_ids:
                case_sql_ids["case1_plsql"] = sid
                case_signatures["case1_plsql"] = sig
                post_exec["case1_plsql"] = max(post_exec.get("case1_plsql", 0), exec_val)
            elif txt.startswith("BEGIN QS_SQL_TEST_PROC1") and "case2_proc1" not in case_sql_ids:
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

        repo_conn = get_connection("repo")
        repo_cur = repo_conn.cursor()

        if repo_engine == "oracle":
            ora_case5 = _read_sql_template("ora_step5.txt") or _read_sql_template("ora_case5.txt")
            if ora_case5.strip():
                parts = [p.strip() for p in ora_case5.split(";") if p.strip().upper().startswith("SELECT")]
                if len(parts) >= 2:
                    q_elapse = parts[0]
                    q_stat = parts[1]
                    q_elapse = (
                        q_elapse.replace("${db_id_list}", db_id_list_str)
                        .replace("${partition_date}", partition_date)
                        .replace("${logging_time}", logging_time_str)
                    )
                    q_elapse = _apply_actual_sql_ids(q_elapse)
                    q_stat = (
                        q_stat.replace("${db_id_list}", db_id_list_str)
                        .replace("${partition_date}", partition_date)
                        .replace("${logging_time}", logging_time_str)
                    )
                    q_stat = _apply_actual_sql_ids(q_stat)
                else:
                    q_elapse = ""
                    q_stat = ""
            else:
                q_elapse = ""
                q_stat = ""
            if not q_elapse or not q_stat:
                q_elapse = f"""
                    SELECT a.db_id, b.instance_name,
                           a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           count(*) AS execution_count,
                           (sum(a.ELAPSE)/count(*))/1000 AS per_elapse_ms_to_sec
                    FROM ora_sql_elapse a, apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > TO_NUMBER('{partition_date}' || '000')
                      AND a.time >= TO_TIMESTAMP('{logging_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                      AND a.sql_id in ({sql_id_filter})
                    GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                    ORDER BY a.db_id, {sql_id_order_expr}, a.sql_plan_hash
                """
                q_stat = f"""
                    SELECT a.db_id, b.instance_name,
                           a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           a.execution_count,
                           (a.elapsed_time/100) AS elapse_cs_to_sec,
                           CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100) END AS per_elapse_sec
                    FROM ora_sql_stat_10min a, apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > TO_NUMBER('{partition_date}' || '000')
                      AND a.time >= TO_TIMESTAMP('{logging_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                      AND a.sql_id in ({sql_id_filter})
                    ORDER BY a.time, a.db_id, {sql_id_order_expr}, a.time, a.sql_plan_hash
                """
        else:
            pg_case5 = _read_sql_template("pg_step5.txt") or _read_sql_template("pg_case5.txt")
            if pg_case5.strip():
                parts = [p.strip() for p in pg_case5.split(";") if p.strip().upper().startswith("SELECT")]
                if len(parts) >= 2:
                    q_elapse = parts[0]
                    q_stat = parts[1]
                    q_elapse = (
                        q_elapse.replace("${schema_name}", schema_name)
                        .replace("${partition_date}", partition_date)
                        .replace("${logging_time}", f"'{logging_time_str}'")
                    )
                    q_elapse = _apply_actual_sql_ids(q_elapse)
                    q_stat = (
                        q_stat.replace("${schema_name}", schema_name)
                        .replace("${partition_date}", partition_date)
                        .replace("${logging_time}", f"'{logging_time_str}'")
                    )
                    q_stat = _apply_actual_sql_ids(q_stat)
                else:
                    q_elapse = ""
                    q_stat = ""
            else:
                q_elapse = ""
                q_stat = ""
            if not q_elapse or not q_stat:
                q_elapse = f"""
                    SELECT a.db_id, b.instance_name,
                           a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           count(*) as execution_count,
                           (sum(a.elapse)/count(*))/1000.0 AS per_elapse_ms_to_sec
                    FROM {schema_name}.ora_sql_elapse a, public.apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > {partition_date}000
                      AND a.time >= '{logging_time_str}'::timestamp
                      AND a.sql_id in ({sql_id_filter})
                    GROUP BY a.db_id, b.instance_name, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash
                    ORDER BY a.db_id,
                        CASE a.sql_id
                            WHEN '9fbyurzh8tr4c' THEN 1
                            WHEN 'fbf2t9pw12ynm' THEN 2
                            WHEN 'ga6tfrmnrzwax' THEN 3
                            WHEN 'af5w9c5uq9mf5' THEN 4
                            WHEN '9yv10yjy19dva' THEN 5
                            WHEN '9t1uh0g3vjnd7' THEN 6
                            ELSE 999
                        END,
                        a.sql_plan_hash
                """
                q_stat = f"""
                    SELECT a.db_id, b.instance_name,
                           a.time, a.sql_id, a.sql_hash, a.sql_addr, a.sql_plan_hash,
                           a.execution_count,
                           (a.elapsed_time/100.0) AS elapse_cs_to_sec,
                           CASE WHEN a.execution_count < 1 THEN NULL ELSE ((a.elapsed_time/a.execution_count)/100.0) END AS per_elapse_sec
                    FROM {schema_name}.ora_sql_stat_10min a, public.apm_db_info b
                    WHERE a.db_id = b.db_id
                      AND a.db_id IN ({db_id_list_str})
                      AND a.partition_key > {partition_date}000
                      AND a.time >= '{logging_time_str}'::timestamp
                      AND a.sql_id in ({sql_id_filter})
                    ORDER BY a.db_id,
                        CASE a.sql_id
                            WHEN '9fbyurzh8tr4c' THEN 1
                            WHEN 'fbf2t9pw12ynm' THEN 2
                            WHEN 'ga6tfrmnrzwax' THEN 3
                            WHEN 'af5w9c5uq9mf5' THEN 4
                            WHEN '9yv10yjy19dva' THEN 5
                            WHEN '9t1uh0g3vjnd7' THEN 6
                            ELSE 999
                        END,
                        a.time,
                        a.sql_plan_hash
                """

        result_data["repo_elapse"] = _query_rows_with_retry(repo_cur, q_elapse, max_attempts=4, wait_seconds=8)
        # ORA_SQL_STAT_10MIN은 MaxGauge가 10분 주기로 수집하므로 최대 ~10분 대기 (20회×30초)
        result_data["repo_stat"] = _query_rows_with_retry(repo_cur, q_stat, max_attempts=20, wait_seconds=30)
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

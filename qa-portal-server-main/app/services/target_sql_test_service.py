"""
Target DB SQL 미니테스트 서비스
선택된 Target DB(db_id)에 권한 확인/프로시저 생성/반복 실행 후 v$sql 결과를 조회한다.
"""

import time
from typing import Any, Dict, List, Optional, Tuple

import oracledb

TEST_SQL_ID = "3b8uva7q2cf5a"


def _elapsed_ms(start: float) -> int:
    return int((time.time() - start) * 1000)


def _step(name: str, status: str, evidence: str, duration_ms: int) -> Dict[str, Any]:
    return {
        "step": name,
        "status": status,
        "evidence": evidence,
        "duration_ms": duration_ms,
    }


def _rows_to_text(rows: List[Dict[str, Any]], limit: int = 10) -> str:
    if not rows:
        return "[]"
    out: List[str] = []
    for row in rows[:limit]:
        parts = [f"{k}={row.get(k)}" for k in row.keys()]
        out.append("{ " + ", ".join(parts) + " }")
    return "\n".join(out)


def _get_apm_db_row_with_secret(db_id: int) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    from app.shared_db import get_connection, release_connection, get_db_config, _infer_db_engine

    cfg = get_db_config("repo")
    if not cfg:
        return False, None, "Repo DB not configured"

    engine = _infer_db_engine(cfg, "postgresql")
    conn = None
    try:
        conn = get_connection("repo")
        cur = conn.cursor()
        if engine == "oracle":
            cur.execute(
                "SELECT db_id, instance_name, host_ip, db_user, db_password, sid, lsnr_port "
                "FROM apm_db_info WHERE db_id = :1",
                [db_id],
            )
        else:
            cur.execute(
                "SELECT db_id, instance_name, host_ip, db_user, db_password, sid, lsnr_port "
                "FROM apm_db_info WHERE db_id = %s",
                [db_id],
            )
        row = cur.fetchone()
        cur.close()
        if not row:
            return False, None, f"db_id={db_id} not found in APM_DB_INFO"
        return True, {
            "db_id": row[0],
            "instance_name": row[1],
            "host_ip": row[2],
            "db_user": row[3],
            "db_password": row[4],
            "sid": row[5],
            "lsnr_port": row[6],
        }, ""
    except Exception as e:
        return False, None, str(e)
    finally:
        if conn:
            release_connection("repo", conn)


def _fetch_dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in (cur.description or [])]
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def run_target_sql_test(db_id: Optional[int] = None, target_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from app.services.oracle_service import OracleService

    total_t0 = time.time()
    row: Optional[Dict[str, Any]] = None
    if target_config:
        row = {
            "db_id": target_config.get("db_id", "-"),
            "instance_name": target_config.get("instance_name", "manual"),
            "host_ip": target_config.get("host"),
            "db_user": target_config.get("user"),
            "db_password": target_config.get("password"),
            "sid": target_config.get("sid"),
            "lsnr_port": target_config.get("port", 1521),
        }
        missing = [k for k in ("host_ip", "db_user", "db_password", "sid") if not row.get(k)]
        if missing:
            return {
                "overall_status": "error",
                "steps": [],
                "error": f"target_config missing fields: {missing}",
            }
    else:
        if db_id is None:
            return {"overall_status": "error", "steps": [], "error": "db_id or target_config is required"}
        ok, row, err = _get_apm_db_row_with_secret(int(db_id))
        if not ok or not row:
            return {"overall_status": "error", "steps": [], "error": err}

    result: Dict[str, Any] = {
        "overall_status": "pass",
        "db_id": row["db_id"],
        "host_ip": row["host_ip"],
        "instance_name": row["instance_name"],
        "conf_name": row["instance_name"],
        "steps": [],
    }

    target_cfg = {
        "host": row["host_ip"],
        "port": int(row.get("lsnr_port") or 1521),
        "user": row["db_user"],
        "password": row["db_password"],
        "sid": row["sid"],
        "service_type": "sid",
        "db_type": "oracle",
    }

    svc = OracleService(target_cfg)
    conn = svc.connect()
    if conn is None:
        # OracleService.connect()는 내부에서 에러를 로깅만 하고 None을 반환하므로,
        # 여기서 직접 한 번 더 시도해 상세 ORA 메시지를 사용자에게 전달한다.
        detail = "Target DB 연결 실패"
        try:
            dsn, user, password = svc._get_credentials()  # noqa: SLF001
            try_conn = oracledb.connect(user=user, password=password, dsn=dsn)
            try_conn.close()
        except Exception as e:
            detail = f"Target DB 연결 실패: {e}"
        result["overall_status"] = "error"
        result["error"] = detail
        result["total_duration_ms"] = _elapsed_ms(total_t0)
        return result

    try:
        cur = conn.cursor()

        # 1) ALTER SYSTEM 권한 확인
        t0 = time.time()
        try:
            cur.execute("SELECT * FROM USER_SYS_PRIVS WHERE privilege = 'ALTER SYSTEM'")
            rows = _fetch_dict_rows(cur)
            status = "pass" if rows else "fail"
            ev = "ALTER SYSTEM privilege rows:\n" + _rows_to_text(rows)
            result["steps"].append(_step("target_check_alter_system", status, ev, _elapsed_ms(t0)))
        except Exception as e:
            result["steps"].append(_step("target_check_alter_system", "fail", str(e), _elapsed_ms(t0)))

        # 2) DBMS_UTILITY 권한 확인
        t0 = time.time()
        try:
            cur.execute(
                "SELECT 'USER_TAB_PRIVS' AS source, privilege AS DBMS_UTILITY_PRIVILEGE "
                "FROM user_tab_privs "
                "WHERE table_name = 'DBMS_UTILITY' AND privilege = 'EXECUTE' "
                "UNION ALL "
                "SELECT 'DBA_TAB_PRIVS' AS source, privilege AS DBMS_UTILITY_PRIVILEGE "
                "FROM dba_tab_privs "
                "WHERE grantee = 'PUBLIC' AND table_name = 'DBMS_UTILITY' AND privilege = 'EXECUTE' "
                "UNION ALL "
                "SELECT 'USER_SYS_PRIVS' AS source, privilege AS DBMS_UTILITY_PRIVILEGE "
                "FROM user_sys_privs "
                "WHERE privilege = 'EXECUTE ANY PROCEDURE'"
            )
            rows = _fetch_dict_rows(cur)
            status = "pass" if rows else "fail"
            ev = "DBMS_UTILITY privilege rows:\n" + _rows_to_text(rows)
            result["steps"].append(_step("target_check_dbms_utility_priv", status, ev, _elapsed_ms(t0)))
        except Exception as e:
            result["steps"].append(_step("target_check_dbms_utility_priv", "fail", str(e), _elapsed_ms(t0)))

        # 3) 프로시저 생성
        t0 = time.time()
        try:
            cur.execute(
                "CREATE OR REPLACE PROCEDURE QS_SQL_MINITEST_PROC as "
                "v_start_time NUMBER; "
                "v_end_time NUMBER; "
                "BEGIN "
                "v_start_time := DBMS_UTILITY.get_time; "
                "v_end_time := 1000; "
                "WHILE (DBMS_UTILITY.get_time - v_start_time) < v_end_time LOOP "
                "NULL; "
                "END LOOP; "
                "END;"
            )
            conn.commit()
            result["steps"].append(_step("target_create_minitest_proc", "pass", "QS_SQL_MINITEST_PROC created", _elapsed_ms(t0)))
        except Exception as e:
            result["steps"].append(_step("target_create_minitest_proc", "fail", str(e), _elapsed_ms(t0)))

        # 4) 테스트 블록 실행
        t0 = time.time()
        try:
            cur.execute(
                "BEGIN "
                "FOR i IN 1..5 LOOP "
                "EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT'; "
                "EXECUTE IMMEDIATE 'BEGIN QS_SQL_MINITEST_PROC; END;'; "
                "END LOOP; "
                "END;"
            )
            conn.commit()
            result["steps"].append(_step("target_run_minitest_block", "pass", "loop block executed (5 times)", _elapsed_ms(t0)))
        except Exception as e:
            result["steps"].append(_step("target_run_minitest_block", "fail", str(e), _elapsed_ms(t0)))

        # 5) v$sql 조회 (요청하신 7번 표시용)
        # 앞선 블록 실행 중 세션이 끊길 수 있어, 조회는 별도 재연결 세션으로 수행
        t0 = time.time()
        try:
            query_conn = svc.connect()
            if query_conn is None:
                raise RuntimeError("v$sql 조회용 재연결 실패")
            query_cur = query_conn.cursor()
            query_cur.execute(
                "SELECT sql_id, "
                "plan_hash_value, "
                "executions, "
                "(elapsed_time/1000000) AS elapse_us_to_sec, "
                "(CASE WHEN executions > 0 THEN (elapsed_time/executions)/1000000 ELSE NULL END) AS per_elapse_sec, "
                "DBMS_LOB.SUBSTR(sql_fulltext, 1000, 1) AS sql_fulltext "
                "FROM v$sql WHERE SQL_id = :1",
                [TEST_SQL_ID],
            )
            rows = _fetch_dict_rows(query_cur)
            query_cur.close()
            svc.release_connection(query_conn)
            pass_hit = False
            for r in rows:
                try:
                    exec_v = int(r.get("EXECUTIONS") or 0)
                except Exception:
                    exec_v = 0
                try:
                    elapsed_v = float(r.get("ELAPSE_US_TO_SEC") or 0)
                except Exception:
                    elapsed_v = 0.0
                if exec_v >= 5 and elapsed_v >= 50:
                    pass_hit = True
                    break

            status = "pass" if pass_hit else "fail"
            ev = (
                f"v$sql SQL_ID={TEST_SQL_ID} "
                f"(pass condition: executions>=5 AND elapse_us_to_sec>=50)\nrows:\n"
                + _rows_to_text(rows)
            )
            result["steps"].append(_step("target_query_result_step7", status, ev, _elapsed_ms(t0)))
        except Exception as e:
            result["steps"].append(_step("target_query_result_step7", "fail", str(e), _elapsed_ms(t0)))

        cur.close()
    finally:
        svc.release_connection(conn)

    if any(s["status"] == "fail" for s in result["steps"]):
        result["overall_status"] = "fail"
    result["total_duration_ms"] = _elapsed_ms(total_t0)
    return result


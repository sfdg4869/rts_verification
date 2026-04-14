"""
Repo DB 점검 서비스
요청된 6개 항목(기존 1~4 + 신규 5~6)을 점검한다.
"""

import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional


def _step_result(name: str, status: str, evidence: str, duration_ms: int) -> Dict[str, Any]:
    return {
        "step": name,
        "status": status,
        "evidence": evidence,
        "duration_ms": duration_ms,
    }


def _elapsed_ms(start: float) -> int:
    return int((time.time() - start) * 1000)


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        hit = lower_map.get(cand.lower())
        if hit:
            return hit
    for c in cols:
        cl = c.lower()
        for cand in candidates:
            if cand.lower() in cl:
                return c
    return None


def _table_columns(cursor, table_name: str) -> List[str]:
    cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")
    return [d[0] for d in (cursor.description or [])]


def _numeric_expr(engine: str, col: str) -> str:
    """문자/숫자 혼합 컬럼을 안전하게 숫자로 변환하는 SQL 표현식."""
    if engine == "oracle":
        return (
            f"COALESCE(TO_NUMBER(REPLACE(REGEXP_SUBSTR(TO_CHAR({col}), '[0-9]+([.,][0-9]+)?'), ',', '')), 0)"
        )
    return f"COALESCE(NULLIF(REGEXP_REPLACE(({col})::text, '[^0-9.]', '', 'g'), '')::numeric, 0)"


def _has_rows(cursor, engine: str, table: str, where_sql: str, params: List[Any]) -> bool:
    if engine == "oracle":
        if where_sql:
            sql = f"SELECT 1 FROM {table}{where_sql} AND ROWNUM = 1"
        else:
            sql = f"SELECT 1 FROM {table} WHERE ROWNUM = 1"
    else:
        sql = f"SELECT 1 FROM {table}{where_sql} LIMIT 1"
    cursor.execute(sql, params)
    return cursor.fetchone() is not None


def _fetch_full_rows(
    cursor,
    engine: str,
    table: str,
    where_sql: str,
    params: List[Any],
    limit: int = 10,
) -> List[Dict[str, Any]]:
    base_sql = f"SELECT * FROM {table}{where_sql}"
    if engine == "oracle":
        sql = f"SELECT * FROM ({base_sql}) WHERE ROWNUM <= {limit}"
    else:
        sql = f"{base_sql} LIMIT {limit}"

    cursor.execute(sql, params)
    rows = cursor.fetchall() or []
    cols = [d[0] for d in (cursor.description or [])]
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def _fmt_rows(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "[]"
    lines: List[str] = []
    for row in rows:
        parts = [f"{k}={row[k]}" for k in row.keys()]
        lines.append("{ " + ", ".join(parts) + " }")
    return "\n".join(lines)


def _normalize_partition_date(partition_date: Optional[Any]) -> int:
    if partition_date is None:
        return int(datetime.now().strftime("%y%m%d"))
    raw = str(partition_date).strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 8:
        digits = digits[2:]
    if len(digits) != 6:
        return int(datetime.now().strftime("%y%m%d"))
    return int(digits)


def _resolve_partition_key_min(cursor, engine: str, partition_date: Optional[Any], sql_id: str) -> int:
    if partition_date is not None and str(partition_date).strip():
        return int(f"{_normalize_partition_date(partition_date)}000")

    try:
        if engine == "oracle":
            cursor.execute("SELECT MAX(partition_key) FROM ora_sql_elapse WHERE sql_id = :1", [sql_id])
        else:
            cursor.execute("SELECT MAX(partition_key) FROM ora_sql_elapse WHERE sql_id = %s", [sql_id])
        row = cursor.fetchone()
        max_pk = int(row[0]) if row and row[0] is not None else 0
        if max_pk > 0:
            return (max_pk // 1000) * 1000
    except Exception:
        pass

    try:
        if engine == "oracle":
            cursor.execute("SELECT MAX(partition_key) FROM ora_sql_stat_10min WHERE sql_id = :1", [sql_id])
        else:
            cursor.execute("SELECT MAX(partition_key) FROM ora_sql_stat_10min WHERE sql_id = %s", [sql_id])
        row = cursor.fetchone()
        max_pk = int(row[0]) if row and row[0] is not None else 0
        if max_pk > 0:
            return (max_pk // 1000) * 1000
    except Exception:
        pass

    return int(f"{datetime.now().strftime('%y%m%d')}000")


def _fetch_rows_with_columns(cursor, limit: int = 50) -> List[Dict[str, Any]]:
    rows = cursor.fetchmany(limit) or []
    cols = [d[0] for d in (cursor.description or [])]
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def run_repo_check(
    db_id: Optional[int] = None,
    recent_minutes: int = 30,
    partition_date: Optional[Any] = None,
    sql_id: str = "3b8uva7q2cf5a",
    progress_callback: Optional[Callable[[int, int, str, str], None]] = None,
) -> Dict[str, Any]:
    from app.shared_db import get_connection, release_connection, get_db_config, _infer_db_engine

    total_t0 = time.time()
    cfg = get_db_config("repo")
    if not cfg:
        return {"error": "Repo DB not configured", "steps": [], "overall_status": "error"}

    result: Dict[str, Any] = {
        "db_id": db_id,
        "recent_minutes": recent_minutes,
        "partition_date": _normalize_partition_date(partition_date) if partition_date is not None else None,
        "sql_id": sql_id,
        "steps": [],
        "overall_status": "pass",
        "repo": {
            "host": cfg.get("host", ""),
            "port": cfg.get("port", ""),
            "database": cfg.get("database", cfg.get("service", "")),
            "user": cfg.get("user", cfg.get("db_user", "")),
            "db_type": cfg.get("db_type", ""),
        },
    }

    engine = _infer_db_engine(cfg, "postgresql")
    conn = None
    try:
        conn = get_connection("repo")
        cursor = conn.cursor()

        partition_key_min = _resolve_partition_key_min(cursor, engine, partition_date, sql_id)
        result["partition_key_min"] = partition_key_min

        # 1) APM_TOP_OS_PROCESS_LIST 누락 여부
        t0 = time.time()
        try:
            where_sql = f" WHERE partition_key >= {partition_key_min}"
            params = []
            if db_id is not None:
                if engine == "oracle":
                    where_sql += " AND db_id = :1"
                else:
                    where_sql += " AND db_id = %s"
                params = [db_id]

            exists = _has_rows(cursor, engine, "APM_TOP_OS_PROCESS_LIST", where_sql, params)
            sample = _fetch_full_rows(cursor, engine, "APM_TOP_OS_PROCESS_LIST", where_sql, params, limit=10)
            status = "pass" if exists else "fail"
            evidence = f"rows exists={exists}\nrows:\n{_fmt_rows(sample)}"
            step = _step_result("repo_apm_top_os_process_list_no_missing", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_apm_top_os_process_list_no_missing", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 6, step["step"], step["status"])

        # 2) ORA_SQL_ELAPSE elapsed>=1 존재 여부
        t0 = time.time()
        try:
            cols = _table_columns(cursor, "ORA_SQL_ELAPSE")
            elapsed_col = _pick_col(cols, ["elapsed_time", "elapsed", "elapse_time", "elapse"])
            if not elapsed_col:
                step = _step_result(
                    "repo_ora_sql_elapse_over_1s_logged",
                    "fail",
                    "ORA_SQL_ELAPSE에서 elapsed 컬럼을 찾지 못했습니다.",
                    _elapsed_ms(t0),
                )
            else:
                elapsed_num = _numeric_expr(engine, elapsed_col)
                where_sql = f" WHERE partition_key >= {partition_key_min} AND {elapsed_num} >= 1"
                params = []
                if db_id is not None:
                    if engine == "oracle":
                        where_sql += " AND db_id = :1"
                    else:
                        where_sql += " AND db_id = %s"
                    params = [db_id]
                
                exists = _has_rows(cursor, engine, "ORA_SQL_ELAPSE", where_sql, params)
                sample = _fetch_full_rows(cursor, engine, "ORA_SQL_ELAPSE", where_sql, params, limit=10)
                status = "pass" if exists else "fail"
                evidence = f"elapsed>=1 exists={exists} (elapsed_col={elapsed_col})\nrows:\n{_fmt_rows(sample)}"
                step = _step_result("repo_ora_sql_elapse_over_1s_logged", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_ora_sql_elapse_over_1s_logged", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 6, step["step"], step["status"])

        # 3) ORA_SQL_STAT_10MIN elapsed>=50 존재 여부
        t0 = time.time()
        try:
            cols = _table_columns(cursor, "ORA_SQL_STAT_10MIN")
            elapsed_col = _pick_col(cols, ["elapsed_time", "elapsed", "elapse_time", "elapse"])
            if not elapsed_col:
                step = _step_result(
                    "repo_ora_sql_stat_10min_over_50s_only",
                    "fail",
                    "ORA_SQL_STAT_10MIN에서 elapsed 컬럼을 찾지 못했습니다.",
                    _elapsed_ms(t0),
                )
            else:
                elapsed_num = _numeric_expr(engine, elapsed_col)
                where_sql = f" WHERE partition_key >= {partition_key_min} AND {elapsed_num} >= 50"
                params = []
                if db_id is not None:
                    if engine == "oracle":
                        where_sql += " AND db_id = :1"
                    else:
                        where_sql += " AND db_id = %s"
                    params = [db_id]
                
                exists = _has_rows(cursor, engine, "ORA_SQL_STAT_10MIN", where_sql, params)
                sample = _fetch_full_rows(cursor, engine, "ORA_SQL_STAT_10MIN", where_sql, params, limit=10)
                status = "pass" if exists else "fail"
                evidence = f"elapsed>=50 exists={exists} (elapsed_col={elapsed_col})\nrows:\n{_fmt_rows(sample)}"
                step = _step_result("repo_ora_sql_stat_10min_over_50s_only", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_ora_sql_stat_10min_over_50s_only", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 6, step["step"], step["status"])

        # 4) ORA_SQL_ELAPSE elapsed>=1 + execution(행수)>=10
        t0 = time.time()
        try:
            cols = _table_columns(cursor, "ORA_SQL_ELAPSE")
            elapsed_col = _pick_col(cols, ["elapsed_time", "elapsed", "elapse_time", "elapse"])
            if not elapsed_col:
                step = _step_result(
                    "repo_ora_sql_elapse_over_1s_elapsed_execution_logged",
                    "fail",
                    "ORA_SQL_ELAPSE에서 elapsed 컬럼을 찾지 못했습니다.",
                    _elapsed_ms(t0),
                )
            else:
                elapsed_num = _numeric_expr(engine, elapsed_col)
                ge1_where = f" WHERE partition_key >= {partition_key_min} AND {elapsed_num} >= 1"
                base_where = f" WHERE partition_key >= {partition_key_min}"
                params = []
                if db_id is not None:
                    if engine == "oracle":
                        ge1_where += " AND db_id = :1"
                        base_where += " AND db_id = :1"
                    else:
                        ge1_where += " AND db_id = %s"
                        base_where += " AND db_id = %s"
                    params = [db_id]
                
                has_elapsed_ge1 = _has_rows(cursor, engine, "ORA_SQL_ELAPSE", ge1_where, params)
                sample = _fetch_full_rows(cursor, engine, "ORA_SQL_ELAPSE", base_where, params, limit=10)
                has_exec_10 = len(sample) >= 10
                status = "pass" if (has_elapsed_ge1 and has_exec_10) else "fail"
                evidence = (
                    f"elapsed>=1 exists={has_elapsed_ge1}, execution rows(<=10 sample)={len(sample)} "
                    f"(pass condition: >=10)\nrows:\n{_fmt_rows(sample)}"
                )
                step = _step_result("repo_ora_sql_elapse_over_1s_elapsed_execution_logged", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_ora_sql_elapse_over_1s_elapsed_execution_logged", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 6, step["step"], step["status"])

        # 5) ORA_SQL_ELAPSE 대상 SQL 조회
        t0 = time.time()
        try:
            if engine == "oracle":
                db_filter = " AND db_id = :3" if db_id is not None else ""
                sql = (
                    "SELECT * FROM ("
                    "  SELECT db_id, time, sql_id, sql_hash, sql_addr, sql_plan_hash, "
                    "         (ELAPSE/1000) AS elapse_ms_to_sec "
                    "  FROM ora_sql_elapse "
                    f"  WHERE partition_key > :1 AND sql_id = :2{db_filter} "
                    "  ORDER BY db_id, time DESC"
                    ") WHERE ROWNUM <= 200"
                )
                params = [partition_key_min, sql_id]
                if db_id is not None:
                    params.append(db_id)
            else:
                db_filter = " AND db_id = %s" if db_id is not None else ""
                sql = (
                    "SELECT db_id, time, sql_id, sql_hash, sql_addr, sql_plan_hash, "
                    "       (ELAPSE/1000.0) AS elapse_ms_to_sec "
                    "FROM ora_sql_elapse "
                    f"WHERE partition_key > %s AND sql_id = %s{db_filter} "
                    "ORDER BY db_id, time DESC LIMIT 200"
                )
                params = [partition_key_min, sql_id]
                if db_id is not None:
                    params.append(db_id)

            cursor.execute(sql, params)
            rows = _fetch_rows_with_columns(cursor, limit=50)
            status = "pass" if rows else "fail"
            evidence = (
                f"query=ORA_SQL_ELAPSE, partition_key>{partition_key_min}, sql_id='{sql_id}'\n"
                f"rows(sample up to 50)={len(rows)}\n{_fmt_rows(rows)}"
            )
            step = _step_result("repo_ora_sql_elapse_target_sql_collect_check", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_ora_sql_elapse_target_sql_collect_check", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 6, step["step"], step["status"])

        # 6) ORA_SQL_STAT_10MIN 대상 SQL 조회
        t0 = time.time()
        try:
            if engine == "oracle":
                db_filter = " AND db_id = :3" if db_id is not None else ""
                sql = (
                    "SELECT * FROM ("
                    "  SELECT db_id, time, sql_id, sql_hash, sql_addr, sql_plan_hash, "
                    "         execution_count, "
                    "         (elapsed_time/100) AS elapse_cs_to_sec, "
                    "         (CASE WHEN execution_count > 0 "
                    "               THEN (elapsed_time/execution_count)/100 "
                    "               ELSE NULL END) AS per_elapse_sec "
                    "  FROM ora_sql_stat_10min "
                    f"  WHERE partition_key > :1 AND sql_id = :2{db_filter} "
                    "  ORDER BY db_id, time DESC"
                    ") WHERE ROWNUM <= 200"
                )
                params = [partition_key_min, sql_id]
                if db_id is not None:
                    params.append(db_id)
            else:
                db_filter = " AND db_id = %s" if db_id is not None else ""
                sql = (
                    "SELECT db_id, time, sql_id, sql_hash, sql_addr, sql_plan_hash, "
                    "       execution_count, "
                    "       (elapsed_time/100.0) AS elapse_cs_to_sec, "
                    "       (CASE WHEN execution_count > 0 "
                    "             THEN (elapsed_time/execution_count)/100.0 "
                    "             ELSE NULL END) AS per_elapse_sec "
                    "FROM ora_sql_stat_10min "
                    f"WHERE partition_key > %s AND sql_id = %s{db_filter} "
                    "ORDER BY db_id, time DESC LIMIT 200"
                )
                params = [partition_key_min, sql_id]
                if db_id is not None:
                    params.append(db_id)

            cursor.execute(sql, params)
            rows = _fetch_rows_with_columns(cursor, limit=50)
            status = "pass" if rows else "fail"
            evidence = (
                f"query=ORA_SQL_STAT_10MIN, partition_key>{partition_key_min}, sql_id='{sql_id}'\n"
                f"rows(sample up to 50)={len(rows)}\n{_fmt_rows(rows)}"
            )
            step = _step_result("repo_ora_sql_stat_10min_target_sql_collect_check", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_ora_sql_stat_10min_target_sql_collect_check", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 6, step["step"], step["status"])

        if any(s["status"] == "fail" for s in result["steps"]):
            result["overall_status"] = "fail"

    except Exception as e:
        return {"error": str(e), "steps": [], "overall_status": "error"}
    finally:
        if conn:
            release_connection("repo", conn)

    result["total_duration_ms"] = _elapsed_ms(total_t0)
    return result


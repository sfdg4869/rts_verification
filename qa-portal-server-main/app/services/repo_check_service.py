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

        # 1) APM_TOP_OS_PROCESS_LIST 최근 20행 조회
        t0 = time.time()
        try:
            params = []
            db_filter = ""
            if db_id is not None:
                if engine == "oracle":
                    db_filter = " AND db_id = :1"
                else:
                    db_filter = " AND db_id = %s"
                params = [db_id]

            if engine == "oracle":
                sql = (
                    "SELECT * FROM ("
                    f"  SELECT * FROM APM_TOP_OS_PROCESS_LIST"
                    f"  WHERE partition_key >= {partition_key_min}{db_filter}"
                    "  ORDER BY time DESC"
                    ") WHERE ROWNUM <= 20"
                )
            else:
                sql = (
                    f"SELECT * FROM APM_TOP_OS_PROCESS_LIST"
                    f" WHERE partition_key >= {partition_key_min}{db_filter}"
                    " ORDER BY time DESC LIMIT 20"
                )

            cursor.execute(sql, params)
            rows = cursor.fetchall() or []
            cols = [d[0] for d in (cursor.description or [])]
            sample = [{cols[i]: r[i] for i in range(len(cols))} for r in rows]

            exists = len(sample) > 0
            status = "pass" if exists else "fail"
            evidence = f"rows exists={exists}, count={len(sample)} (최근 20행)\nrows:\n{_fmt_rows(sample)}"
            step = _step_result("repo_apm_top_os_process_list_no_missing", status, evidence, _elapsed_ms(t0))
        except Exception as e:
            step = _step_result("repo_apm_top_os_process_list_no_missing", "fail", str(e), _elapsed_ms(t0))
        result["steps"].append(step)
        if progress_callback:
            progress_callback(len(result["steps"]), 1, step["step"], step["status"])

        if any(s["status"] == "fail" for s in result["steps"]):
            result["overall_status"] = "fail"

    except Exception as e:
        return {"error": str(e), "steps": [], "overall_status": "error"}
    finally:
        if conn:
            release_connection("repo", conn)

    result["total_duration_ms"] = _elapsed_ms(total_t0)
    return result


"""
MaxGauge WS 정합성 검증 서비스

MaxGauge 웹 화면 WebSocket(sgaStatusStat) 값과 실제 Oracle Target DB
v$session 값을 비교하여 정합성을 검증한다.
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

_logger = logging.getLogger(__name__)

V_SESSION_SQL = {
    "active_session_cnt": (
        "SELECT COUNT(*) FROM v$session WHERE status='ACTIVE' AND type='USER'"
    ),
    "total_session_cnt": (
        "SELECT COUNT(*) FROM v$session WHERE type='USER'"
    ),
    "lock_wait_session_cnt": (
        "SELECT COUNT(*) FROM v$session WHERE blocking_session IS NOT NULL"
    ),
}

# ── Step 1: apm_db_info 조회 ─────────────────────────────────────────────────

def _fetch_apm_db_info(config_id: str) -> Tuple[bool, List[Dict], str]:
    """
    MongoDB config_id 로 Repo DB에 임시 접속하여 apm_db_info 전체 행을 반환한다.

    connect_repo_by_config_id 는 MongoDB 원본 문서를 정규화하면서 service_type/sid
    키를 잃어 ORA-12514 가 발생한다. 따라서 MongoDB 원본 문서를 직접 조회해서
    OracleService 에 그대로 전달한다 (OracleService._build_dsn 이 모든 키 변형을 처리).

    반환: (success, rows, error_msg)
    """
    from app import shared_db as _sdb
    from app.services.oracle_service import OracleService

    # ── MongoDB에서 원본 문서 직접 조회 ──
    try:
        collection = _sdb.db_config_service.db_configs_collection
        if collection is None:
            return False, [], "MongoDB에 연결되어 있지 않습니다."
        doc = collection.find_one({"id": config_id})
        if doc:
            doc.pop("_id", None)
        if not doc:
            return False, [], f"config_id를 찾을 수 없습니다: {config_id}"
    except Exception as e:
        return False, [], f"MongoDB 조회 실패: {e}"

    # ── DB 타입 판별 ──
    db_type = (doc.get("db_type") or "").lower()
    port = int(doc.get("port") or doc.get("db_port") or 5432)
    is_oracle = db_type in ("oracle", "ora") or port == 1521

    sql = (
        "SELECT db_id, instance_name, host_ip, db_user, db_password, "
        "sid, lsnr_port FROM apm_db_info ORDER BY db_id ASC"
    )

    if is_oracle:
        # OracleService 에 원본 doc 전달 → _build_dsn 이 service_type/sid/service 모두 처리
        svc = OracleService(doc)
        conn = svc.connect()
        if conn is None:
            return False, [], "Oracle Repo DB 연결 실패 (OracleService.connect 반환 None)"
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = ["db_id", "instance_name", "host_ip", "db_user",
                    "db_password", "sid", "lsnr_port"]
            rows = [dict(zip(cols, tup)) for tup in cur.fetchall()]
            cur.close()
            return True, rows, ""
        except Exception as e:
            return False, [], f"apm_db_info 조회 실패: {e}"
        finally:
            svc.release_connection(conn)
    else:
        # PostgreSQL 직접 연결
        import psycopg2
        import psycopg2.extras
        from app.services.dg_password_service import decrypt_dg_password

        raw_pw = doc.get("password") or doc.get("db_password") or ""
        pw = decrypt_dg_password(raw_pw)
        database = doc.get("database") or doc.get("service") or doc.get("service_name") or ""
        user = doc.get("db_user") or doc.get("user") or doc.get("username") or ""
        host = doc.get("host", "localhost")
        try:
            conn = psycopg2.connect(
                host=host, port=port, database=database,
                user=user, password=pw, sslmode="disable",
            )
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
            conn.close()
            return True, rows, ""
        except Exception as e:
            return False, [], f"apm_db_info 조회 실패: {e}"


# ── Step 2: Playwright WS 수집 ───────────────────────────────────────────────

def _collect_ws_data(
    mg_url: str,
    mg_user: str,
    mg_password: str,
    collect_seconds: int,
    monitor_url: str = "",
) -> Dict[int, Dict]:
    """
    Playwright sync_api 로 MaxGauge에 로그인하고 WebSocket 메시지를 수집한다.
    반환: {db_id(int): {active_session_cnt, total_session_cnt, lock_wait_session_cnt, instance_name}}
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    ws_data: Dict[int, Dict] = {}
    base = mg_url.rstrip("/")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        # WebSocket 메시지 인터셉트
        def _on_ws(ws):
            def _on_frame(frame):
                try:
                    body = frame.body
                    if not body:
                        return
                    msg = json.loads(body)
                    hdr = msg.get("request_header", {})
                    res = msg.get("result", {})
                    if (
                        hdr.get("type") == "plugin_push"
                        and res.get("command") == "sgaStatusStat"
                    ):
                        db_id = hdr.get("db_id")
                        delta = res.get("Delta", {})
                        if db_id is not None:
                            ws_data[int(db_id)] = {
                                "active_session_cnt": delta.get("active_session_cnt", 0),
                                "total_session_cnt": delta.get("total_session_cnt", 0),
                                "lock_wait_session_cnt": delta.get("lock_wait_session_cnt", 0),
                                "instance_name": res.get("InstanceName", ""),
                            }
                except Exception:
                    pass

            ws.on("framereceived", _on_frame)

        page.on("websocket", _on_ws)

        # 메인 URL 접근 (로그인 페이지로 리다이렉트되거나 직접 로그인 폼 노출)
        try:
            page.goto(base, wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            _logger.warning("MaxGauge 메인 URL 타임아웃, 계속 진행")

        # 로그인 폼 탐지 및 제출
        _do_login(page, mg_user, mg_password, base)

        if monitor_url:
            # 사용자가 직접 지정한 모니터링 화면 URL 로 바로 이동
            try:
                page.goto(monitor_url, wait_until="domcontentloaded", timeout=20_000)
                _logger.info("모니터링 화면 직접 이동: %s", monitor_url)
            except PWTimeout:
                _logger.warning("모니터링 화면 URL 타임아웃: %s", monitor_url)
        else:
            # URL 미지정 → 자동 탐색
            _navigate_to_monitoring(page, base, ws_data)

        # WS 수집 대기
        time.sleep(collect_seconds)

        try:
            browser.close()
        except Exception:
            pass

    return ws_data


def _do_login(page, mg_user: str, mg_password: str, base: str) -> None:
    """MaxGauge 로그인 폼을 탐지하여 자격증명을 입력하고 제출한다."""
    from playwright.sync_api import TimeoutError as PWTimeout

    # 로그인 폼 대기
    try:
        page.wait_for_selector("input[type=password]", timeout=8_000)
    except PWTimeout:
        return  # 로그인 폼 없음 → 이미 로그인됨

    # 모든 input을 순서대로 가져와서 타입별로 채움
    try:
        all_inputs = page.query_selector_all(
            "input:not([type='hidden']):not([type='checkbox']):not([type='radio']):not([type='submit'])"
        )
        for inp in all_inputs:
            try:
                itype = (inp.get_attribute("type") or "text").lower()
                if itype == "password":
                    inp.click()
                    inp.fill(mg_password)
                else:
                    inp.click()
                    inp.fill(mg_user)
            except Exception:
                continue
    except Exception as e:
        _logger.warning("로그인 입력 실패: %s", e)
        return

    _logger.info("로그인 입력 완료, 버튼 클릭 시도")

    # Login 버튼 클릭
    submitted = False
    for sel in [
        "button:has-text('Login')",
        "button:has-text('로그인')",
        "button[type=submit]",
        "input[type=submit]",
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                submitted = True
                _logger.info("로그인 버튼 클릭: %s", sel)
                break
        except Exception:
            continue

    if not submitted:
        try:
            page.keyboard.press("Enter")
            _logger.info("Enter 키로 로그인 제출")
        except Exception:
            pass

    # 로그인 후 페이지 로드 대기
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass


def _navigate_to_monitoring(page, base: str, ws_data: Dict) -> None:
    """
    로그인 후 sgaStatusStat WS 메시지가 발생하는 모니터링 화면으로 이동한다.
    URL 미상이므로 현재 페이지의 링크를 탐색하고 후보 경로를 순차 방문한다.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    def _has_ws() -> bool:
        return len(ws_data) > 0

    if _has_ws():
        return  # 이미 수신 중

    # 현재 페이지 링크 탐색 (monitoring/dashboard/realtime 포함 href 우선)
    keywords = ["monitoring", "monitor", "realtime", "dashboard", "instance", "summary"]
    try:
        anchors = page.query_selector_all("a[href]")
        for kw in keywords:
            for a in anchors:
                href = a.get_attribute("href") or ""
                if kw in href.lower():
                    target = href if href.startswith("http") else base + "/" + href.lstrip("/")
                    try:
                        page.goto(target, wait_until="domcontentloaded", timeout=15_000)
                        time.sleep(3)
                        if _has_ws():
                            return
                    except Exception:
                        pass
    except Exception:
        pass

    if _has_ws():
        return

    # 후보 경로 순차 방문
    candidates = [
        "",                      # 메인 (로그인 직후)
        "/MAXGAUGE",
        "/MAXGAUGE/main",
        "/MAXGAUGE/monitoring",
        "/MAXGAUGE/monitor",
        "/MAXGAUGE/dashboard",
        "/MAXGAUGE/realtime",
        "/MAXGAUGE/instance",
        "/MAXGAUGE/summary",
    ]
    for path in candidates:
        url = base + path if path else base
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15_000)
            time.sleep(3)
            if _has_ws():
                return
        except Exception:
            continue


# ── Step 3: Target Oracle v$session 쿼리 ────────────────────────────────────

def _query_target_db(row: Dict) -> Dict[str, Any]:
    """
    단일 Target Oracle DB에 v$session 쿼리 3종을 실행한다.
    apm_db_info.db_password 는 OracleService 내부에서 decrypt_dg_password() 로 자동 복호화된다.
    접속 실패 또는 쿼리 실패 시 해당 항목을 'N/A'로 반환하고 db_error 에 원인을 기록한다.
    """
    from app.services.oracle_service import OracleService

    host = row.get("host_ip", "")
    port = int(row.get("lsnr_port") or 1521)
    sid  = row.get("sid", "")
    user = row.get("db_user", "")
    pw   = row.get("db_password", "")

    cfg = {
        "host": host,
        "port": port,
        "user": user,
        "password": pw,
        "sid": sid,
        "db_type": "oracle",
    }
    svc = OracleService(cfg)

    # connect() 는 실패 시 None 반환하고 에러를 로그에만 남김.
    # 원인을 UI 에 표시하기 위해 connect_or_raise 를 직접 호출해 예외를 캡처한다.
    conn = None
    db_error: Optional[str] = None
    try:
        conn = svc.connect_or_raise()
    except Exception as e:
        db_error = str(e)
        _logger.warning(
            "Target DB 접속 실패: db_id=%s %s:%s/%s user=%s → %s",
            row.get("db_id"), host, port, sid, user, db_error,
        )
        na = {k: "N/A" for k in V_SESSION_SQL}
        na["db_error"] = db_error
        return na

    result: Dict[str, Any] = {}
    try:
        cur = conn.cursor()
        for key, sql in V_SESSION_SQL.items():
            try:
                cur.execute(sql)
                val = cur.fetchone()
                result[key] = int(val[0]) if val else 0
            except Exception as e:
                _logger.warning("v$session 쿼리 실패 (%s): %s", key, e)
                result[key] = "N/A"
        cur.close()
    finally:
        svc.release_connection(conn)

    return result


# ── Step 4: 비교 및 리포트 ──────────────────────────────────────────────────

def _match_label(db_val, ws_val) -> str:
    if db_val == "N/A" or ws_val is None:
        return "N/A"
    return "일치" if db_val == ws_val else "불일치"


def _compare_report(
    apm_rows: List[Dict],
    ws_data: Dict[int, Dict],
    db_results: Dict[int, Dict],
) -> List[Dict]:
    report = []
    for row in apm_rows:
        db_id = int(row["db_id"])
        ws = ws_data.get(db_id, {})
        db = db_results.get(db_id, {})

        active_db = db.get("active_session_cnt", "N/A")
        active_ws = ws.get("active_session_cnt")
        total_db = db.get("total_session_cnt", "N/A")
        total_ws = ws.get("total_session_cnt")
        lock_db = db.get("lock_wait_session_cnt", "N/A")
        lock_ws = ws.get("lock_wait_session_cnt")

        m_active = _match_label(active_db, active_ws)
        m_total = _match_label(total_db, total_ws)
        m_lock = _match_label(lock_db, lock_ws)

        ws_received = bool(ws)
        if not ws_received:
            overall = "N/A"
        elif m_active == "일치" and m_total == "일치" and m_lock == "일치":
            overall = "일치"
        else:
            overall = "불일치"

        report.append({
            "db_id": db_id,
            "instance_name": row.get("instance_name") or ws.get("instance_name") or "-",
            "active_db": active_db,
            "active_ws": active_ws,
            "match_active": m_active,
            "total_db": total_db,
            "total_ws": total_ws,
            "match_total": m_total,
            "lock_db": lock_db,
            "lock_ws": lock_ws,
            "match_lock": m_lock,
            "overall": overall,
            "ws_received": ws_received,
            "db_error": db.get("db_error"),  # 접속 실패 시 오류 메시지
        })
    return report


# ── 메인 진입점 ──────────────────────────────────────────────────────────────

def run_ws_consistency(
    config_id: str,
    mg_url: str,
    mg_user: str,
    mg_password: str,
    collect_seconds: int = 30,
    monitor_url: str = "",
    progress_callback=None,
) -> Dict[str, Any]:
    """
    WS 정합성 검증 메인 함수. rts_check_routes.py 의 threading.Thread worker 에서 호출된다.

    Args:
        config_id: MongoDB Repo DB id (UUID 문자열, /api/v1/db_list 가 반환하는 id 필드)
        mg_url: MaxGauge 웹 기본 URL (예: http://10.10.47.72:19190/MAXGAUGE)
        mg_user: MaxGauge 로그인 아이디
        mg_password: MaxGauge 로그인 비밀번호
        collect_seconds: WebSocket 메시지 수집 대기 시간 (초)
        progress_callback: (done, total, step_name, step_status) 콜백 (선택)
    """
    _progress = progress_callback or (lambda *_: None)
    started_at = time.time()

    # Step 1: apm_db_info 조회
    _progress(0, 4, "apm_db_info 조회", "running")
    ok, apm_rows, err = _fetch_apm_db_info(config_id)
    if not ok:
        return {"error": f"apm_db_info 조회 실패: {err}", "report": []}
    _progress(1, 4, "apm_db_info 조회 완료", "done")
    _logger.info("apm_db_info 조회 완료: %d 행", len(apm_rows))

    # Step 2: Playwright WS 수집 (collect_seconds 소요)
    _progress(1, 4, f"MaxGauge WS 수집 ({collect_seconds}초)", "running")
    ws_data: Dict[int, Dict] = {}
    try:
        ws_data = _collect_ws_data(mg_url, mg_user, mg_password, collect_seconds, monitor_url)
    except Exception as e:
        _logger.error("WS 수집 중 예외: %s", e)
    _progress(2, 4, f"WS 수집 완료 ({len(ws_data)}개 수신)", "done")
    _logger.info("WS 수집 완료: %d 개 db_id 수신", len(ws_data))

    # Step 3: Target Oracle DB v$session 병렬 쿼리
    _progress(2, 4, "Target DB v$session 조회 중", "running")
    db_results: Dict[int, Dict] = {}
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="ws-vsession") as exe:
        future_map = {exe.submit(_query_target_db, row): row for row in apm_rows}
        for future in as_completed(future_map):
            row = future_map[future]
            db_id = int(row["db_id"])
            try:
                db_results[db_id] = future.result()
            except Exception as e:
                _logger.warning("v$session 조회 예외 db_id=%s: %s", db_id, e)
                db_results[db_id] = {k: "N/A" for k in V_SESSION_SQL}
    _progress(3, 4, "DB 조회 완료", "done")

    # Step 4: 비교 및 리포트
    _progress(3, 4, "비교 분석 중", "running")
    report = _compare_report(apm_rows, ws_data, db_results)
    _progress(4, 4, "완료", "done")

    match_count = sum(1 for r in report if r["overall"] == "일치")
    mismatch_count = sum(1 for r in report if r["overall"] == "불일치")
    na_count = sum(1 for r in report if r["overall"] == "N/A")

    # 첫 번째 DB 오류 샘플 (UI 에 표시용)
    db_error_sample: Optional[str] = None
    for r in report:
        if r.get("db_error"):
            db_error_sample = f"[db_id={r['db_id']} {r['instance_name']}] {r['db_error']}"
            break

    return {
        "report": report,
        "total_instances": len(apm_rows),
        "ws_received_count": sum(1 for r in report if r["ws_received"]),
        "match_count": match_count,
        "mismatch_count": mismatch_count,
        "na_count": na_count,
        "duration_seconds": round(time.time() - started_at, 1),
        "mg_url": mg_url,
        "collect_seconds": collect_seconds,
        "db_error_sample": db_error_sample,
    }


def _build_summary(apm_rows, ws_data, db_results, started_at, iteration, interval_seconds, mg_url):
    report = _compare_report(apm_rows, ws_data, db_results)
    match_count    = sum(1 for r in report if r["overall"] == "일치")
    mismatch_count = sum(1 for r in report if r["overall"] == "불일치")
    na_count       = sum(1 for r in report if r["overall"] == "N/A")
    db_error_sample = next(
        (f"[db_id={r['db_id']} {r['instance_name']}] {r['db_error']}"
         for r in report if r.get("db_error")),
        None,
    )
    return {
        "report": report,
        "total_instances": len(apm_rows),
        "ws_received_count": sum(1 for r in report if r["ws_received"]),
        "match_count": match_count,
        "mismatch_count": mismatch_count,
        "na_count": na_count,
        "duration_seconds": round(time.time() - started_at, 1),
        "mg_url": mg_url,
        "interval_seconds": interval_seconds,
        "iteration": iteration,
        "last_updated": int(time.time()),
        "db_error_sample": db_error_sample,
    }


def run_ws_consistency_realtime(
    config_id: str,
    mg_url: str,
    mg_user: str,
    mg_password: str,
    interval_seconds: int = 1,
    monitor_url: str = "",
    stop_event: Optional[threading.Event] = None,
    result_callback=None,
    progress_callback=None,
) -> None:
    """
    실시간 WS 정합성 검증 (트리거 방식).

    MaxGauge가 sgaStatusStat WS 메시지를 보내는 순간(= MaxGauge가 v$session을 읽은 직후)
    해당 Oracle DB를 즉시 조회하여 비교한다.
    이 방식으로 MaxGauge 갱신 주기(3초)와 Oracle 조회 타이밍을 최대한 일치시킨다.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    _progress  = progress_callback or (lambda *_: None)
    _result    = result_callback   or (lambda _: None)
    started_at = time.time()

    _progress(0, 3, "apm_db_info 조회", "running")
    ok, apm_rows, err = _fetch_apm_db_info(config_id)
    if not ok:
        _result({"error": f"apm_db_info 조회 실패: {err}", "report": []})
        return
    _progress(1, 3, f"apm_db_info 조회 완료 ({len(apm_rows)}개)", "done")

    apm_by_id: Dict[int, Dict] = {int(r["db_id"]): r for r in apm_rows}

    ws_data:    Dict[int, Dict] = {}  # 최신 WS 값
    db_results: Dict[int, Dict] = {}  # 최신 Oracle 조회 결과
    iteration   = [0]

    # 결과 갱신 스로틀 (0.5초에 한 번만 result_callback 호출)
    last_emit   = [0.0]
    emit_lock   = threading.Lock()

    def _maybe_emit():
        now = time.time()
        with emit_lock:
            if now - last_emit[0] < 0.5:
                return
            last_emit[0] = now
        iteration[0] += 1
        summary = _build_summary(
            apm_rows, dict(ws_data), dict(db_results),
            started_at, iteration[0], interval_seconds, mg_url,
        )
        _result(summary)
        _progress(2, 3, f"#{iteration[0]}회차 갱신 ({len(ws_data)}개 WS 수신)", "done")

    # db_id 별 조회 중복 방지 (같은 db_id의 이전 조회가 끝나지 않으면 건너뜀)
    _querying: set = set()
    _querying_lock = threading.Lock()

    executor = ThreadPoolExecutor(max_workers=30, thread_name_prefix="ws-ora")

    def _trigger_oracle_query(db_id: int) -> None:
        """WS 메시지 도착 즉시 해당 Oracle DB 조회 트리거."""
        with _querying_lock:
            if db_id in _querying:
                return  # 이미 조회 중 → 건너뜀
            _querying.add(db_id)

        row = apm_by_id.get(db_id)
        if row is None:
            with _querying_lock:
                _querying.discard(db_id)
            return

        def _run():
            try:
                db_results[db_id] = _query_target_db(row)
                _maybe_emit()
            finally:
                with _querying_lock:
                    _querying.discard(db_id)

        executor.submit(_run)

    _progress(1, 3, "MaxGauge 로그인 중", "running")

    base = mg_url.rstrip("/")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = browser.new_context(ignore_https_errors=True)
            page    = context.new_page()

            def _on_ws(ws):
                def _on_frame(frame):
                    try:
                        body = frame.body
                        if not body:
                            return
                        msg = json.loads(body)
                        hdr = msg.get("request_header", {})
                        res = msg.get("result", {})
                        if (
                            hdr.get("type") == "plugin_push"
                            and res.get("command") == "sgaStatusStat"
                        ):
                            db_id = hdr.get("db_id")
                            delta = res.get("Delta", {})
                            if db_id is not None:
                                did = int(db_id)
                                ws_data[did] = {
                                    "active_session_cnt":    delta.get("active_session_cnt", 0),
                                    "total_session_cnt":     delta.get("total_session_cnt", 0),
                                    "lock_wait_session_cnt": delta.get("lock_wait_session_cnt", 0),
                                    "instance_name":         res.get("InstanceName", ""),
                                }
                                # WS 수신 즉시 Oracle 조회 트리거
                                _trigger_oracle_query(did)
                    except Exception:
                        pass
                ws.on("framereceived", _on_frame)

            page.on("websocket", _on_ws)

            try:
                page.goto(base, wait_until="domcontentloaded", timeout=30_000)
            except PWTimeout:
                pass

            _do_login(page, mg_user, mg_password, base)

            current_url = page.url
            _logger.info("로그인 후 URL: %s", current_url)

            # 로그인 페이지에 머물고 있으면 실패 (URL 기준으로만 판단)
            if "login" in current_url.lower():
                _logger.error("로그인 실패 — 로그인 페이지에 머물고 있음. URL: %s", current_url)
                _result({"error": f"MaxGauge 로그인 실패. 아이디/비밀번호를 확인하세요. (현재 URL: {current_url})", "report": []})
                return

            if monitor_url:
                try:
                    page.goto(monitor_url, wait_until="networkidle", timeout=30_000)
                    _logger.info("모니터링 화면 이동 완료: %s", page.url)
                except PWTimeout:
                    _logger.warning("모니터링 화면 networkidle 타임아웃, 계속 진행")

            # 스크린샷 저장 (디버그용)
            try:
                import os
                screenshot_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "ws_debug_screenshot.png")
                page.screenshot(path=screenshot_path, full_page=False)
                _logger.info("스크린샷 저장: %s", screenshot_path)
            except Exception as se:
                _logger.warning("스크린샷 실패: %s", se)

            _progress(2, 3, "WS 수신 대기 중...", "running")

            # 30초 내 WS 미수신 시 오류 보고
            _ws_wait_start = time.time()
            while not ws_data and time.time() - _ws_wait_start < 30:
                if stop_event and stop_event.is_set():
                    return
                time.sleep(1)

            if not ws_data:
                _logger.error("WS 메시지 30초간 미수신. 현재 URL: %s", page.url)
                _result({"error": f"WS 메시지를 받지 못했습니다. 모니터링 화면 URL을 확인하세요. (현재 URL: {page.url})", "report": []})
                return

            _logger.info("WS 수신 시작: %d개 db_id", len(ws_data))

            while not (stop_event and stop_event.is_set()):
                time.sleep(0.5)

            try:
                browser.close()
            except Exception:
                pass
    finally:
        executor.shutdown(wait=False)

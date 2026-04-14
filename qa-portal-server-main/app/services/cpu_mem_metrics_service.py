"""
CPU/MEM 전용 조회 서비스 (in-memory 저장)
"""

import json
import os
import time
import re
import threading
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.services.rts_check_service import (
    DAEMONS,
    _SSHSession,
    _resolve_base_dir,
    _step_rtsctl_stat,
    get_apm_db_row,
)

_STORE: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
_KEEP_SECONDS = 3 * 60 * 60  # 3h keep window
_WINDOW_START: Dict[str, int] = {}
_WINDOW_META: Dict[str, Dict[str, Any]] = {}
_COLLECTOR_THREADS: Dict[str, threading.Thread] = {}
_COLLECTOR_LOCK = threading.Lock()
_COLLECT_INTERVAL_SECONDS = 60
_APP_DIR = os.path.dirname(os.path.dirname(__file__))
_RESOURCE_DIR = os.path.join(_APP_DIR, "resource")


def _elapsed_ms(start: float) -> int:
    return int((time.time() - start) * 1000)


def _ensure_resource_dir() -> None:
    os.makedirs(_RESOURCE_DIR, exist_ok=True)


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", (s or "").strip()) or "instance"


def _session_key(host_ip: str, conf_name: str) -> str:
    return f"{host_ip}:{conf_name}"


def _build_log_file_path(conf_name: str, start_ts: int) -> str:
    stamp = datetime.fromtimestamp(start_ts).strftime("%Y%m%d_%H%M%S")
    fname = f"{_safe_name(conf_name)}_{stamp}.jsonl"
    return os.path.join(_RESOURCE_DIR, fname)


def _append_log_line(file_path: str, payload: Dict[str, Any]) -> None:
    _ensure_resource_dir()
    with open(file_path, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _kb_to_mb(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return round(float(v) / 1024.0, 3)
    except Exception:
        return None


def _fmt_ts(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _collector_worker(session_key: str, end_ts: int, kwargs: Dict[str, Any]) -> None:
    """
    2시간 집계 세션 동안 주기적으로 snapshot을 수집해서 저장한다.
    """
    while int(time.time()) < end_ts:
        # 세션이 중간에 갱신/종료되면 중단
        meta = _WINDOW_META.get(session_key) or {}
        if not meta:
            break
        if int(meta.get("end_ts", 0)) != int(end_ts):
            break

        time.sleep(_COLLECT_INTERVAL_SECONDS)
        if int(time.time()) >= end_ts:
            break
        try:
            _collect_snapshot(enable_store=True, **kwargs)
        except Exception:
            # 백그라운드 수집 실패는 메인 요청 흐름을 깨지 않도록 무시
            pass


def _ensure_collector_started(session_key: str, end_ts: int, kwargs: Dict[str, Any]) -> None:
    with _COLLECTOR_LOCK:
        th = _COLLECTOR_THREADS.get(session_key)
        if th and th.is_alive():
            return
        worker_kwargs = {
            "db_id": kwargs.get("db_id"),
            "ssh_user": kwargs.get("ssh_user"),
            "ssh_password": kwargs.get("ssh_password"),
            "ssh_port": kwargs.get("ssh_port", 22),
            "conf_name": kwargs.get("conf_name"),
            "base_dir": kwargs.get("base_dir"),
            "host_override": kwargs.get("host_override"),
        }
        th = threading.Thread(
            target=_collector_worker,
            args=(session_key, int(end_ts), worker_kwargs),
            daemon=True,
            name=f"cpu-mem-collector-{session_key}",
        )
        _COLLECTOR_THREADS[session_key] = th
        th.start()


def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    try:
        return int(float(v))
    except Exception:
        return None


def _parse_full_metric_line(line: str) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[int], str]:
    # expected: pid cpu mem rss comm
    cols = line.split()
    if len(cols) < 5:
        return None, None, None, None, line
    pid = _to_int(cols[0])
    if pid is None:
        return None, None, None, None, line
    return pid, _to_float(cols[1]), _to_float(cols[2]), _to_int(cols[3]), " ".join(cols[4:])


def _parse_no_mem_metric_line(line: str) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[int], str]:
    # expected: pid cpu rss comm
    cols = line.split()
    if len(cols) < 4:
        return None, None, None, None, line
    pid = _to_int(cols[0])
    if pid is None:
        return None, None, None, None, line
    return pid, _to_float(cols[1]), None, _to_int(cols[2]), " ".join(cols[3:])


def _first_number(text: str) -> Optional[float]:
    m = re.search(r"[-+]?\d+(?:\.\d+)?", text or "")
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _pid_fallback(ssh: _SSHSession, conf_name: str, daemon: str) -> Optional[str]:
    cmd = (
        f"ps -ef 2>/dev/null | grep -i '{conf_name}' | grep -i '{daemon}' | "
        "grep -v grep | awk 'NR==1{print $2}'"
    )
    out, _, _ = ssh.run(cmd)
    pid = out.strip().splitlines()[0] if out.strip() else ""
    return pid or None


def _read_pid_metrics(ssh: _SSHSession, pid: str) -> Tuple[Optional[float], Optional[float], Optional[int], Optional[int], str]:
    cmd_specs = [
        (f"ps -p {pid} -o pid=,%cpu=,%mem=,rss=,comm= 2>/dev/null", "full"),
        (f"ps -p {pid} -o pid=,pcpu=,pmem=,rss=,comm= 2>/dev/null", "full"),
        (f"UNIX95=1 ps -p {pid} -o pid=,pcpu=,pmem=,rss=,comm= 2>/dev/null", "full"),
        (f"ps -p {pid} -o pid=,pcpu=,rss=,comm= 2>/dev/null", "no_mem"),
        (f"UNIX95=1 ps -p {pid} -o pid=,pcpu=,rss=,comm= 2>/dev/null", "no_mem"),
    ]
    best_cpu: Optional[float] = None
    best_mem: Optional[float] = None
    best_rss: Optional[int] = None
    best_vsz: Optional[int] = None
    best_raw = ""

    for cmd, mode in cmd_specs:
        out, _, _ = ssh.run(cmd)
        text = out.strip()
        if not text:
            continue
        for line in text.splitlines():
            if mode == "full":
                _, cpu, mem, rss, raw = _parse_full_metric_line(line)
            else:
                _, cpu, mem, rss, raw = _parse_no_mem_metric_line(line)
            if cpu is not None and best_cpu is None:
                best_cpu = cpu
            if mem is not None and best_mem is None:
                best_mem = mem
            if rss is not None and best_rss is None:
                best_rss = rss
            if raw:
                best_raw = raw
        if best_cpu is not None and best_mem is not None and best_rss is not None:
            return best_cpu, best_mem, best_rss, best_vsz, best_raw

    # OS별 컬럼 지원 편차가 크므로 항목별 단건 조회로 보강
    if best_cpu is None:
        for cmd in [
            f"ps -p {pid} -o pcpu= 2>/dev/null",
            f"ps -p {pid} -o %cpu= 2>/dev/null",
            f"UNIX95=1 ps -p {pid} -o pcpu= 2>/dev/null",
            f"ps -p {pid} -o c= 2>/dev/null",
        ]:
            out, _, _ = ssh.run(cmd)
            v = _first_number(out.strip())
            if v is not None:
                best_cpu = v
                break

    if best_mem is None:
        for cmd in [
            f"ps -p {pid} -o pmem= 2>/dev/null",
            f"ps -p {pid} -o %mem= 2>/dev/null",
            f"UNIX95=1 ps -p {pid} -o pmem= 2>/dev/null",
        ]:
            out, _, _ = ssh.run(cmd)
            v = _first_number(out.strip())
            if v is not None:
                best_mem = v
                break

    if best_rss is None:
        for cmd in [
            f"ps -p {pid} -o rss= 2>/dev/null",
            f"UNIX95=1 ps -p {pid} -o rss= 2>/dev/null",
            f"ps -p {pid} -o sz= 2>/dev/null",
            f"UNIX95=1 ps -p {pid} -o sz= 2>/dev/null",
        ]:
            out, _, _ = ssh.run(cmd)
            v = _first_number(out.strip())
            if v is not None:
                best_rss = int(v)
                break

    # HP-UX/AIX 등에서 -p/-o가 제한될 때: ps -ef 라인 파싱
    # 일반 포맷: UID PID PPID C STIME TTY TIME CMD
    if best_cpu is None:
        out_ef, _, _ = ssh.run(
            f"ps -ef 2>/dev/null | awk '$2=={pid} {{print; exit}}'"
        )
        line_ef = out_ef.strip().splitlines()[0] if out_ef.strip() else ""
        if line_ef:
            cols = line_ef.split()
            if len(cols) >= 4:
                cpu_try = _to_float(cols[3])  # C 컬럼 (상대 CPU)
                if cpu_try is not None:
                    best_cpu = cpu_try
                if not best_raw:
                    best_raw = line_ef

    # VSZ(virtual mem) fallback
    if best_vsz is None:
        for cmd in [
            f"ps -p {pid} -o vsz= 2>/dev/null",
            f"UNIX95=1 ps -p {pid} -o vsz= 2>/dev/null",
        ]:
            out, _, _ = ssh.run(cmd)
            v = _first_number(out.strip())
            if v is not None:
                best_vsz = int(v)
                if not best_raw:
                    best_raw = "vsz fallback"
                break

    if best_cpu is not None or best_mem is not None or best_rss is not None or best_vsz is not None:
        return best_cpu, best_mem, best_rss, best_vsz, (best_raw or "partial cpu/mem data")
    return None, None, None, None, "no cpu/mem data (ps format unsupported)"


def _collect_snapshot(
    db_id: int,
    ssh_user: str,
    ssh_password: str,
    ssh_port: int = 22,
    conf_name: Optional[str] = None,
    base_dir: Optional[str] = None,
    host_override: Optional[str] = None,
    enable_store: bool = True,
) -> Dict[str, Any]:
    t0 = time.time()
    ok, db_row, err_msg = get_apm_db_row(db_id)
    if not ok:
        return {"error": err_msg, "overall_status": "error", "metrics": []}

    host_ip = host_override or db_row["host_ip"]
    resolved_conf = conf_name or db_row["instance_name"]

    try:
        ssh = _SSHSession(host_ip, ssh_port, ssh_user, ssh_password)
    except Exception as e:
        return {"error": f"SSH connection failed: {e}", "overall_status": "error", "metrics": []}

    try:
        resolved_base = _resolve_base_dir(ssh, resolved_conf, base_dir)
        _, pid_map = _step_rtsctl_stat(ssh, resolved_base, resolved_conf)

        metrics: List[Dict[str, Any]] = []
        now_ts = int(time.time())
        for daemon in DAEMONS:
            pid = pid_map.get(daemon)
            if not pid:
                pid = _pid_fallback(ssh, resolved_conf, daemon)

            if not pid:
                metrics.append({
                    "daemon": daemon,
                    "status": "fail",
                    "pid": None,
                    "cpu_pct": None,
                    "mem_pct": None,
                    "rss_kb": None,
                    "evidence": "pid not found",
                })
                continue

            cpu, mem, rss, vsz, raw = _read_pid_metrics(ssh, pid)
            status = "pass" if (cpu is not None or mem is not None or rss is not None or vsz is not None) else "skip"
            row = {
                "daemon": daemon,
                "status": status,
                "pid": str(pid),
                "cpu_pct": cpu,
                "mem_pct": mem,
                "rss_kb": rss,
                "vsz_kb": vsz,
                "evidence": raw,
                "collected_at": now_ts,
            }
            metrics.append(row)

            sess_key = _session_key(host_ip, resolved_conf)
            meta = _WINDOW_META.get(sess_key)
            # 요구사항: 2시간 집계 조회 버튼 시작 전에는 로깅/저장하지 않음
            if enable_store and meta:
                key = f"{host_ip}:{resolved_conf}:{daemon}"
                sample = {
                    "ts": now_ts,
                    "pid": str(pid),
                    "cpu_pct": cpu,
                    "mem_pct": mem,
                    "rss_kb": rss,
                    "vsz_kb": vsz,
                }
                _STORE[key].append(sample)
                try:
                    log_payload = {
                        "logged_at": _fmt_ts(now_ts),
                        "host_ip": host_ip,
                        "conf_name": resolved_conf,
                        "daemon": daemon,
                        "pid": str(pid),
                        "cpu_pct": cpu,
                        "mem_pct": mem,
                        "real_mem_mb": _kb_to_mb(rss),
                        "virtual_mem_mb": _kb_to_mb(vsz),
                    }
                    _append_log_line(
                        meta["file_path"],
                        log_payload,
                    )
                except Exception:
                    # 파일 쓰기 실패는 조회 흐름을 중단하지 않음
                    pass

        # prune old samples
        cutoff = int(time.time()) - _KEEP_SECONDS
        for k in list(_STORE.keys()):
            _STORE[k] = [x for x in _STORE[k] if int(x.get("ts", 0)) >= cutoff]
            if not _STORE[k]:
                _STORE.pop(k, None)

        overall = "pass" if any(m["status"] == "pass" for m in metrics) else "fail"
        return {
            "overall_status": overall,
            "mode": "snapshot",
            "db_id": db_id,
            "host_ip": host_ip,
            "conf_name": resolved_conf,
            "resolved_base_dir": resolved_base,
            "metrics": metrics,
            "collected_at": now_ts,
            "logging_enabled": _session_key(host_ip, resolved_conf) in _WINDOW_META,
            "log_file": (_WINDOW_META.get(_session_key(host_ip, resolved_conf)) or {}).get("file_path"),
            "total_duration_ms": _elapsed_ms(t0),
        }
    finally:
        ssh.close()


def _agg(values: List[Optional[float]]) -> Tuple[Optional[float], Optional[float]]:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return None, None
    return round(sum(nums) / len(nums), 3), round(max(nums), 3)


def collect_cpu_mem_snapshot(**kwargs) -> Dict[str, Any]:
    # window 버튼으로 세션이 시작된 이후에만 저장됨
    return _collect_snapshot(enable_store=True, **kwargs)


def collect_cpu_mem_window(window_minutes: int = 120, **kwargs) -> Dict[str, Any]:
    # 먼저 snapshot을 수집해 host/conf를 식별
    snap = _collect_snapshot(enable_store=False, **kwargs)
    if snap.get("overall_status") == "error":
        snap["mode"] = "window"
        return snap

    host_ip = snap.get("host_ip", "")
    conf_name = snap.get("conf_name", "")
    win_min = max(1, int(window_minutes))
    session_key = _session_key(host_ip, conf_name)
    now_ts = int(time.time())
    started_now = False

    # 요구사항: 2시간 집계는 버튼 클릭 시점부터 시작
    # 첫 window 호출 시 시작시각/파일명을 고정하고, 이후 해당 세션 구간만 집계
    start_ts = _WINDOW_START.get(session_key)
    if start_ts is None:
        start_ts = int(snap.get("collected_at") or now_ts)
        _WINDOW_START[session_key] = start_ts
        _WINDOW_META[session_key] = {
            "start_ts": start_ts,
            "file_path": _build_log_file_path(conf_name, start_ts),
        }
        started_now = True
    end_ts = start_ts + (win_min * 60)
    _WINDOW_META[session_key]["end_ts"] = end_ts

    # 2시간 경과 후 다음 호출은 새 세션으로 재시작
    if now_ts > end_ts:
        start_ts = int(snap.get("collected_at") or now_ts)
        _WINDOW_START[session_key] = start_ts
        _WINDOW_META[session_key] = {
            "start_ts": start_ts,
            "file_path": _build_log_file_path(conf_name, start_ts),
        }
        end_ts = start_ts + (win_min * 60)
        _WINDOW_META[session_key]["end_ts"] = end_ts
        started_now = True

    # 세션 시작 시점 포함: 현재 snapshot을 저장(메모리+파일)
    snap_store = _collect_snapshot(enable_store=True, **kwargs)
    if snap_store.get("overall_status") == "error":
        snap_store["mode"] = "window"
        return snap_store

    # 버튼 1회 클릭 후 백그라운드 자동 누적 시작
    _ensure_collector_started(session_key, end_ts, kwargs)

    out_rows: List[Dict[str, Any]] = []
    total_samples = 0
    pass_daemon_count = 0
    for daemon in DAEMONS:
        key = f"{host_ip}:{conf_name}:{daemon}"
        samples = [
            s for s in _STORE.get(key, [])
            if start_ts <= int(s.get("ts", 0)) <= end_ts
        ]
        total_samples += len(samples)
        cpu_avg, cpu_max = _agg([s.get("cpu_pct") for s in samples])
        mem_avg, mem_max = _agg([s.get("mem_pct") for s in samples])
        rss_avg, rss_max = _agg([s.get("rss_kb") for s in samples])
        vsz_avg, vsz_max = _agg([s.get("vsz_kb") for s in samples])
        has_numeric = any(
            (s.get("cpu_pct") is not None)
            or (s.get("mem_pct") is not None)
            or (s.get("rss_kb") is not None)
            or (s.get("vsz_kb") is not None)
            for s in samples
        )
        if has_numeric:
            row_status = "pass"
            pass_daemon_count += 1
        elif samples:
            row_status = "skip"
        else:
            row_status = "fail"
        out_rows.append({
            "daemon": daemon,
            "status": row_status,
            "sample_count": len(samples),
            "cpu_avg_pct": cpu_avg,
            "cpu_max_pct": cpu_max,
            "mem_avg_pct": mem_avg,
            "mem_max_pct": mem_max,
            "rss_avg_kb": rss_avg,
            "rss_max_kb": rss_max,
            "vsz_avg_kb": vsz_avg,
            "vsz_max_kb": vsz_max,
        })

    overall = "pass" if pass_daemon_count > 0 else ("fail" if total_samples == 0 else "skip")
    return {
        "overall_status": overall,
        "mode": "window",
        "window_minutes": win_min,
        "db_id": snap.get("db_id"),
        "host_ip": host_ip,
        "conf_name": conf_name,
        "resolved_base_dir": snap.get("resolved_base_dir"),
        "window_start_ts": start_ts,
        "window_end_ts": end_ts,
        "window_started_now": started_now,
        "log_file": (_WINDOW_META.get(session_key) or {}).get("file_path"),
        "collect_interval_sec": _COLLECT_INTERVAL_SECONDS,
        "collector_running": bool(_COLLECTOR_THREADS.get(session_key) and _COLLECTOR_THREADS[session_key].is_alive()),
        "sample_count_total": total_samples,
        "metrics": out_rows,
        "note": "window logging starts at first click and auto-collects every 60s (in-memory + jsonl)",
        "total_duration_ms": snap_store.get("total_duration_ms", 0),
    }


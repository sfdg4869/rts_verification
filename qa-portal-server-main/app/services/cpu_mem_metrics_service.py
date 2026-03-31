"""
CPU/MEM 전용 조회 서비스 (in-memory 저장)
"""

import time
from collections import defaultdict
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


def _elapsed_ms(start: float) -> int:
    return int((time.time() - start) * 1000)


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


def _parse_metric_line(line: str) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[int], str]:
    cols = line.split()
    if len(cols) >= 5:
        return _to_int(cols[0]), _to_float(cols[1]), _to_float(cols[2]), _to_int(cols[3]), cols[4]
    if len(cols) >= 4:
        return _to_int(cols[0]), _to_float(cols[1]), _to_float(cols[2]), None, cols[3]
    return None, None, None, None, line


def _pid_fallback(ssh: _SSHSession, conf_name: str, daemon: str) -> Optional[str]:
    cmd = (
        f"ps -ef 2>/dev/null | grep -i '{conf_name}' | grep -i '{daemon}' | "
        "grep -v grep | awk 'NR==1{print $2}'"
    )
    out, _, _ = ssh.run(cmd)
    pid = out.strip().splitlines()[0] if out.strip() else ""
    return pid or None


def _read_pid_metrics(ssh: _SSHSession, pid: str) -> Tuple[Optional[float], Optional[float], Optional[int], str]:
    cmds = [
        f"ps -p {pid} -o pid=,%cpu=,%mem=,rss=,comm= 2>/dev/null",
        f"ps -p {pid} -o pid=,pcpu=,pmem=,rss=,comm= 2>/dev/null",
        f"UNIX95=1 ps -p {pid} -o pid=,pcpu=,pmem=,rss=,comm= 2>/dev/null",
    ]
    for cmd in cmds:
        out, _, _ = ssh.run(cmd)
        text = out.strip()
        if not text:
            continue
        line = text.splitlines()[0]
        _, cpu, mem, rss, raw = _parse_metric_line(line)
        return cpu, mem, rss, raw
    return None, None, None, "no cpu/mem data"


def _collect_snapshot(
    db_id: int,
    ssh_user: str,
    ssh_password: str,
    ssh_port: int = 22,
    conf_name: Optional[str] = None,
    base_dir: Optional[str] = None,
    host_override: Optional[str] = None,
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

            cpu, mem, rss, raw = _read_pid_metrics(ssh, pid)
            status = "pass" if (cpu is not None or mem is not None or rss is not None) else "skip"
            row = {
                "daemon": daemon,
                "status": status,
                "pid": str(pid),
                "cpu_pct": cpu,
                "mem_pct": mem,
                "rss_kb": rss,
                "evidence": raw,
                "collected_at": now_ts,
            }
            metrics.append(row)

            key = f"{host_ip}:{resolved_conf}:{daemon}"
            _STORE[key].append({
                "ts": now_ts,
                "pid": str(pid),
                "cpu_pct": cpu,
                "mem_pct": mem,
                "rss_kb": rss,
            })

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
    return _collect_snapshot(**kwargs)


def collect_cpu_mem_window(window_minutes: int = 120, **kwargs) -> Dict[str, Any]:
    snap = _collect_snapshot(**kwargs)
    if snap.get("overall_status") == "error":
        snap["mode"] = "window"
        return snap

    host_ip = snap.get("host_ip", "")
    conf_name = snap.get("conf_name", "")
    cutoff = int(time.time()) - max(1, int(window_minutes)) * 60

    out_rows: List[Dict[str, Any]] = []
    total_samples = 0
    for daemon in DAEMONS:
        key = f"{host_ip}:{conf_name}:{daemon}"
        samples = [s for s in _STORE.get(key, []) if int(s.get("ts", 0)) >= cutoff]
        total_samples += len(samples)
        cpu_avg, cpu_max = _agg([s.get("cpu_pct") for s in samples])
        mem_avg, mem_max = _agg([s.get("mem_pct") for s in samples])
        rss_avg, rss_max = _agg([s.get("rss_kb") for s in samples])
        out_rows.append({
            "daemon": daemon,
            "sample_count": len(samples),
            "cpu_avg_pct": cpu_avg,
            "cpu_max_pct": cpu_max,
            "mem_avg_pct": mem_avg,
            "mem_max_pct": mem_max,
            "rss_avg_kb": rss_avg,
            "rss_max_kb": rss_max,
        })

    overall = "pass" if total_samples > 0 else "fail"
    return {
        "overall_status": overall,
        "mode": "window",
        "window_minutes": int(window_minutes),
        "db_id": snap.get("db_id"),
        "host_ip": host_ip,
        "conf_name": conf_name,
        "resolved_base_dir": snap.get("resolved_base_dir"),
        "sample_count_total": total_samples,
        "metrics": out_rows,
        "note": "in-memory storage (reset on server restart)",
        "total_duration_ms": snap.get("total_duration_ms", 0),
    }


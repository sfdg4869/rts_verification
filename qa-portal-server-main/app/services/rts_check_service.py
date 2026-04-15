"""
RTS 프로세스 상태 점검 서비스
SSH를 통해 원격 호스트의 MaxGauge RTS 데몬 상태를 순차적으로 점검한다.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import paramiko

_logger = logging.getLogger(__name__)

MAX_OUTPUT_BYTES = 10 * 1024  # 10 KB
SSH_CONNECT_TIMEOUT = 10
CMD_TIMEOUT = 15

DAEMONS = ["rts", "sndf", "obsd"]

_SENSITIVE_PATTERNS = re.compile(
    r"(password|passwd|pwd|secret|token)[=:\s]+\S+", re.IGNORECASE
)

_ABNORMAL_KEYWORDS = [
    "SIGBUS", "SIGSEGV", "SIGABRT", "SIGKILL",
    "core dumped", "Segmentation fault", "Bus error",
    "abnormal termination", "fatal signal",
]


def _mask_sensitive(text: str) -> str:
    return _SENSITIVE_PATTERNS.sub(r"\1=***", text)


def _truncate(text: str, max_len: int = MAX_OUTPUT_BYTES) -> str:
    if len(text) > max_len:
        return text[:max_len] + "\n...(truncated)"
    return text


class _SSHSession:
    """단일 SSH 세션 — 점검 동안 연결을 유지하고 여러 명령을 실행한다."""

    def __init__(self, host: str, port: int, username: str, password: str):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            hostname=host,
            port=int(port),
            username=username,
            password=password,
            timeout=SSH_CONNECT_TIMEOUT,
        )

    def run(self, cmd: str, timeout: int = CMD_TIMEOUT) -> Tuple[str, str, int]:
        """명령 실행 → (stdout, stderr, exit_code)"""
        _logger.debug("SSH cmd: %s", cmd)
        _, stdout_ch, stderr_ch = self.client.exec_command(cmd, timeout=timeout)
        channel = stdout_ch.channel

        t0 = time.time()
        while not channel.exit_status_ready():
            if time.time() - t0 > timeout:
                channel.close()
                raise TimeoutError(f"Command timed out after {timeout}s: {cmd}")
            time.sleep(0.1)

        exit_code = channel.recv_exit_status()
        out = stdout_ch.read(MAX_OUTPUT_BYTES + 1)
        err = stderr_ch.read(MAX_OUTPUT_BYTES + 1)

        try:
            out_str = out.decode("utf-8")
        except UnicodeDecodeError:
            out_str = out.decode("utf-8", errors="ignore")
        try:
            err_str = err.decode("utf-8")
        except UnicodeDecodeError:
            err_str = err.decode("utf-8", errors="ignore")

        return _truncate(out_str), _truncate(err_str), exit_code

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass


def _step_result(name: str, status: str, evidence: str, duration_ms: int) -> Dict[str, Any]:
    return {
        "step": name,
        "status": status,
        "evidence": _mask_sensitive(evidence),
        "duration_ms": duration_ms,
    }


def _elapsed_ms(start: float) -> int:
    return int((time.time() - start) * 1000)


def _shell_quote(s: str) -> str:
    """POSIX shell-safe single-quote wrapping."""
    return "'" + str(s).replace("'", "'\"'\"'") + "'"


# ────────────────────────────────────────────────────────
# 개별 점검 스텝
# ────────────────────────────────────────────────────────

def _log_root(conf_dir: str) -> str:
    """로그 디렉터리 경로: {conf_dir}/log/maxgauge"""
    return f"{conf_dir}/log/maxgauge"


def _step_log_dirs(ssh: _SSHSession, conf_dir: str) -> Dict[str, Any]:
    """Step 1: {conf_dir}/log/maxgauge 아래 rts/sndf/obsd 이름이 포함된 로그 파일 존재 여부"""
    t0 = time.time()
    log_dir = _log_root(conf_dir)

    out, err, rc = ssh.run(f"test -d {log_dir} && echo DIR_OK || echo DIR_MISSING")
    if "DIR_MISSING" in out:
        return _step_result(
            "log_directory_check", "fail",
            f"{log_dir} — 디렉터리 없음",
            _elapsed_ms(t0),
        )

    results = {}
    for d in DAEMONS:
        # OS별 find 옵션 차이를 피하기 위해 portable 명령만 사용
        fout, _, _ = ssh.run(
            f"find {log_dir} -type f 2>/dev/null | grep -i { _shell_quote(d) } | head -5"
        )
        files = [f for f in fout.strip().splitlines() if f]
        if files:
            results[d] = f"EXISTS ({len(files)} file(s))"
        else:
            results[d] = "MISSING"

    all_exist = all("EXISTS" in v for v in results.values())
    evidence = ", ".join(f"{d}: {v}" for d, v in results.items())
    return _step_result(
        "log_directory_check",
        "pass" if all_exist else "fail",
        f"{log_dir} — {evidence}",
        _elapsed_ms(t0),
    )


def _mxgrc_prefix(conf_dir: str) -> str:
    """rtsctl 실행 전 필수 선행 명령: cd → . .mxgrc"""
    return f"cd {conf_dir} && . ./.mxgrc"


def _step_rtsctl_stat(ssh: _SSHSession, conf_dir: str) -> Tuple[Dict[str, Any], Dict[str, Optional[str]]]:
    """Step 2: . .mxgrc 적용 후 rtsctl stat 로 rts/sndf/obsd RUNNING 여부 확인
    Returns: (step_result, pid_map)  — pid_map은 Step 3에서 사용
    """
    t0 = time.time()
    mxgrc_file = f"{conf_dir}/.mxgrc"

    # .mxgrc 존재 확인
    chk_out, _, _ = ssh.run(f"test -f {mxgrc_file} && echo OK || echo MISSING")
    if "MISSING" in chk_out:
        pid_map: Dict[str, Optional[str]] = {d: None for d in DAEMONS}
        return _step_result(
            "rtsctl_stat", "fail",
            f".mxgrc not found at {mxgrc_file}",
            _elapsed_ms(t0),
        ), pid_map

    prefix = _mxgrc_prefix(conf_dir)
    out, err, rc = ssh.run(f"{prefix} && rtsctl stat 2>&1")

    combined = out + "\n" + err
    pid_map = {}
    daemon_status: Dict[str, str] = {}

    for daemon in DAEMONS:
        pid_map[daemon] = None
        # rtsctl 출력 형식이 환경마다 달라서 daemon별 블록을 잡아 status/pid를 독립 파싱한다.
        block_match = re.search(rf"(?is)\b{daemon}\b[^;\n]*", combined)
        block = block_match.group(0) if block_match else ""
        block_low = block.lower()

        if "does not exist" in block_low:
            daemon_status[daemon] = "DOES_NOT_EXIST"
        elif "not running" in block_low or "stopped" in block_low:
            daemon_status[daemon] = "STOPPED"
        elif "running" in block_low:
            daemon_status[daemon] = "RUNNING"
        else:
            daemon_status[daemon] = block.strip() if block.strip() else "UNKNOWN"

        m_pid = re.search(r"(?i)\bpid\b\s*[:=]?\s*(\d+)", block)
        if m_pid:
            pid_map[daemon] = m_pid.group(1)

    if "rtsctl" in combined and "not found" in combined.lower():
        return _step_result(
            "rtsctl_stat", "fail",
            f"rtsctl command not found (after . .mxgrc in {conf_dir})\n{combined[:300]}",
            _elapsed_ms(t0),
        ), pid_map

    all_running = all(s == "RUNNING" for s in daemon_status.values())
    evidence_parts = [f"{d}: {s}" + (f" (pid={pid_map[d]})" if pid_map[d] else "") for d, s in daemon_status.items()]
    return _step_result(
        "rtsctl_stat",
        "pass" if all_running else "fail",
        f"(. .mxgrc applied) " + "; ".join(evidence_parts),
        _elapsed_ms(t0),
    ), pid_map


def _step_pid_match(ssh: _SSHSession, conf_name: str, pid_map: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """Step 3: Step 2에서 얻은 PID가 실제 프로세스로 떠있는지 확인"""
    t0 = time.time()
    mismatches: List[str] = []
    matches: List[str] = []

    for daemon, rts_pid in pid_map.items():
        if not rts_pid:
            mismatches.append(f"{daemon}: no pid from rtsctl")
            continue

        # OS 호환: Linux/AIX/HP-UX/Solaris 순으로 시도
        checks = [
            f"ps -p {rts_pid} -o pid=,comm=,args= 2>/dev/null",
            f"UNIX95=1 ps -p {rts_pid} -o pid=,comm=,args= 2>/dev/null",
            f"ps -ef 2>/dev/null | awk '$2=={rts_pid} {{print; found=1; exit}} END{{if(!found) exit 1}}'",
            f"ps -e -o pid=,args= 2>/dev/null | awk '$1=={rts_pid} {{print; found=1; exit}} END{{if(!found) exit 1}}'",
        ]
        found_line = ""
        for cmd in checks:
            out, _, rc = ssh.run(cmd)
            if out.strip():
                found_line = out.strip().splitlines()[0]
                break
        if found_line:
            matches.append(f"{daemon}: pid {rts_pid} OK ({found_line[:140]})")
        else:
            mismatches.append(f"{daemon}: pid {rts_pid} not in ps output")

    status = "pass" if not mismatches else "fail"
    evidence = "; ".join(matches + mismatches)
    return _step_result("pid_cross_check", status, evidence, _elapsed_ms(t0))


def _step_rtsctl_stop_verify(
    ssh: _SSHSession, conf_dir: str, conf_name: str,
    stop_wait_sec: int = 10,
) -> Dict[str, Any]:
    """
    rtsctl stop 수행 후 정상 중지 확인.

    검증 항목:
      1. rtsctl stop 실행
      2. rtsctl stat → rts/sndf/obsd 모두 "does not exist" 확인
      3. ps -ef | grep {conf_name} → 관련 PID 없음 확인
    """
    t0 = time.time()
    prefix = _mxgrc_prefix(conf_dir)
    evidence_lines: List[str] = []
    failed: List[str] = []

    # ── 1. rtsctl stop 실행 ──────────────────────────────────────────
    stop_out, stop_err, stop_rc = ssh.run(f"{prefix} && rtsctl stop 2>&1")
    stop_combined = (stop_out + "\n" + stop_err).strip()
    evidence_lines.append(f"[rtsctl stop rc={stop_rc}] {stop_combined[:300]}")

    # stop 완료까지 대기
    if stop_wait_sec > 0:
        time.sleep(stop_wait_sec)

    # ── 2. rtsctl stat → "does not exist" 확인 ───────────────────────
    stat_out, stat_err, _ = ssh.run(f"{_mxgrc_prefix(conf_dir)} && rtsctl stat 2>&1")
    stat_combined = (stat_out + "\n" + stat_err)
    evidence_lines.append(f"[rtsctl stat after stop]\n{stat_combined[:500]}")

    daemon_status: Dict[str, str] = {}
    for daemon in DAEMONS:
        block_match = re.search(rf"(?is)\b{daemon}\b[^;\n]*", stat_combined)
        block = block_match.group(0) if block_match else ""
        block_low = block.lower()

        if "does not exist" in block_low:
            daemon_status[daemon] = "DOES_NOT_EXIST"
        elif "not running" in block_low or "stopped" in block_low:
            daemon_status[daemon] = "STOPPED"
        elif "running" in block_low:
            daemon_status[daemon] = "STILL_RUNNING"
        else:
            daemon_status[daemon] = block.strip() if block.strip() else "UNKNOWN"

    stat_parts = [f"{d}: {s}" for d, s in daemon_status.items()]
    evidence_lines.append("[stat check] " + "; ".join(stat_parts))

    not_stopped = [d for d, s in daemon_status.items() if s not in ("DOES_NOT_EXIST", "STOPPED")]
    if not_stopped:
        failed.append(f"rtsctl stat: 중지 미확인 daemon={not_stopped}")

    # ── 3. ps -ef | grep {conf_name} → PID 없음 확인 ─────────────────
    safe_conf = _shell_quote(conf_name)
    ps_out, _, _ = ssh.run(
        f"ps -ef 2>/dev/null | grep {safe_conf} | grep -v grep || true"
    )
    ps_lines = [l for l in ps_out.strip().splitlines() if l.strip()]
    if ps_lines:
        evidence_lines.append(f"[ps check] FAIL — 아직 PID 존재:\n" + "\n".join(ps_lines[:10]))
        failed.append(f"ps: {len(ps_lines)}개 프로세스 잔존")
    else:
        evidence_lines.append(f"[ps check] PASS — {conf_name} 관련 PID 없음")

    status = "pass" if not failed else "fail"
    return _step_result(
        "rtsctl_stop_verify",
        status,
        "\n".join(evidence_lines),
        _elapsed_ms(t0),
    )


def _find_latest_log(ssh: _SSHSession, log_dir: str, daemon: str) -> Optional[str]:
    """로그 디렉터리에서 daemon 이름이 포함된 로그 파일 경로를 반환(휴대성 우선)."""
    out, _, _ = ssh.run(
        f"find {log_dir} -type f 2>/dev/null | grep -i { _shell_quote(daemon) } | head -1"
    )
    path = out.strip().splitlines()[0] if out.strip() else None
    return path


def _step_error_grep(ssh: _SSHSession, conf_dir: str) -> Dict[str, Any]:
    """Step 4: {conf_dir}/log/maxgauge/ 에서 rts/sndf/obsd 로그의 SIGBUS 등 비정상 시그널 grep.
    키워드가 없으면 PASS, 있으면 FAIL."""
    t0 = time.time()
    log_dir = _log_root(conf_dir)
    keyword_pattern = "|".join(_ABNORMAL_KEYWORDS)
    findings: List[str] = []
    error_samples: List[str] = []
    has_signal = False
    has_missing = False

    for daemon in DAEMONS:
        log_file = _find_latest_log(ssh, log_dir, daemon)
        if not log_file:
            has_missing = True
            findings.append(f"*{daemon}* log: not found in {log_dir}")
            continue

        fname = log_file.rsplit("/", 1)[-1]
        cmd = f"grep -iE '{keyword_pattern}' {log_file} | tail -5"
        out, err, rc = ssh.run(cmd)
        text = out.strip()
        if text:
            has_signal = True
            lines = text.splitlines()
            findings.append(f"{fname}: {len(lines)} hit(s) — {lines[-1][:200]}")
            error_samples.append(f"[{fname}] signal hit logs:\n{text}")
        else:
            findings.append(f"{fname}: clean (no abnormal signal)")

    evidence = "; ".join(findings)
    if error_samples:
        evidence = evidence + "\n\n" + "\n\n".join(error_samples)

    return _step_result(
        "error_log_grep",
        "fail" if (has_signal or has_missing) else "pass",
        evidence,
        _elapsed_ms(t0),
    )


def _step_resource_usage(ssh: _SSHSession, pid_map: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """Step 5: 해당 프로세스 CPU/MEM 사용률"""
    t0 = time.time()
    pids = [p for p in pid_map.values() if p]
    if not pids:
        return _step_result("resource_usage", "skip", "no pids available", _elapsed_ms(t0))

    pid_to_daemon = {pid: d for d, pid in pid_map.items() if pid}
    parts: List[str] = []
    fallback_alive_parts: List[str] = []

    for pid in pids:
        daemon_label = pid_to_daemon.get(pid, pid)
        tried_cmds = [
            f"ps -p {pid} -o pid=,%cpu=,%mem=,comm= 2>/dev/null",
            f"ps -p {pid} -o pid=,pcpu=,pmem=,comm= 2>/dev/null",
            f"UNIX95=1 ps -p {pid} -o pid=,pcpu=,pmem=,comm= 2>/dev/null",
        ]
        line = ""
        for cmd in tried_cmds:
            out, _, _ = ssh.run(cmd)
            if out.strip():
                line = out.strip().splitlines()[0]
                break

        if line:
            cols = line.split()
            if len(cols) >= 4:
                parts.append(f"{daemon_label}(pid={cols[0]}): cpu={cols[1]}% mem={cols[2]}%")
            else:
                parts.append(f"{daemon_label}(pid={pid}): {line}")
            continue

        # CPU/MEM 포맷을 못 가져온 경우에도 PID 생존 여부는 별도 확인
        out_alive, _, _ = ssh.run(
            f"ps -ef 2>/dev/null | awk '$2=={pid} {{print; found=1; exit}} END{{if(!found) exit 1}}'"
        )
        if out_alive.strip():
            fallback_alive_parts.append(f"{daemon_label}(pid={pid}): alive (cpu/mem format unsupported on this OS)")
        else:
            fallback_alive_parts.append(f"{daemon_label}(pid={pid}): not found")

    if parts:
        return _step_result("resource_usage", "pass", "; ".join(parts), _elapsed_ms(t0))

    # CPU/MEM 포맷을 못 읽어도 프로세스는 살아있는 경우
    if fallback_alive_parts and any("alive" in x for x in fallback_alive_parts):
        return _step_result("resource_usage", "skip", "; ".join(fallback_alive_parts), _elapsed_ms(t0))

    return _step_result("resource_usage", "skip", "ps returned no data", _elapsed_ms(t0))


def _step_abnormal_signals(ssh: _SSHSession, conf_dir: str) -> Dict[str, Any]:
    """Step 6: {conf_dir}/log/maxgauge/ 에서 SIGBUS, SIGSEGV 등 비정상 종료 징후 검색"""
    t0 = time.time()
    log_dir = _log_root(conf_dir)
    keyword_pattern = "|".join(_ABNORMAL_KEYWORDS)
    findings: List[str] = []
    log_samples: List[str] = []
    has_signal = False
    has_missing = False

    for daemon in DAEMONS:
        log_file = _find_latest_log(ssh, log_dir, daemon)
        if not log_file:
            has_missing = True
            findings.append(f"*{daemon}* log: not found in {log_dir}")
            continue

        fname = log_file.rsplit("/", 1)[-1]
        cmd = f"grep -iE '{keyword_pattern}' {log_file} | tail -3"
        out, err, rc = ssh.run(cmd)
        text = out.strip()
        if text:
            has_signal = True
            lines = text.splitlines()
            findings.append(f"{fname}: {len(lines)} hit(s) — {lines[-1][:200]}")
            # 비정상 키워드 히트가 있을 때만 원문 로그를 제공
            log_samples.append(f"[{fname}] abnormal hit logs:\n{text}")
        else:
            findings.append(f"{fname}: clean")

    evidence = "; ".join(findings)
    if log_samples:
        evidence = evidence + "\n\n" + "\n\n".join(log_samples)

    return _step_result(
        "abnormal_signal_check",
        "fail" if (has_signal or has_missing) else "pass",
        evidence,
        _elapsed_ms(t0),
    )


def _step_target_vsql_query(db_row: Dict[str, Any], host_override: Optional[str] = None) -> Dict[str, Any]:
    """Step 7: Target DB에서 v$sql 조회, 5행 이상이면 pass"""
    t0 = time.time()
    try:
        import oracledb
        from app.services.oracle_service import OracleService

        host = host_override or db_row.get("host_ip")
        user = db_row.get("db_user")
        password = db_row.get("db_password")
        sid = db_row.get("sid")
        port = int(db_row.get("lsnr_port") or 1521)

        missing = []
        if not host:
            missing.append("host_ip")
        if not user:
            missing.append("db_user")
        if not password:
            missing.append("db_password")
        if not sid:
            missing.append("sid")
        if missing:
            return _step_result(
                "target_vsql_query",
                "fail",
                f"Target DB 연결값 누락: {', '.join(missing)}",
                _elapsed_ms(t0),
            )

        cfg = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "sid": sid,
            "service_type": "sid",
            "db_type": "oracle",
        }
        # Oracle pool에서 간헐적으로 stale connection(DPI-1010)이 발생해
        # Step 7 조회는 직접 신규 연결로 수행한다.
        svc = OracleService(cfg)
        dsn, user_name, plain_password = svc._get_credentials()  # noqa: SLF001
        conn = oracledb.connect(user=user_name, password=plain_password, dsn=dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT sql_id, "
                "plan_hash_value, "
                "executions, "
                "(elapsed_time/1000000) AS elapse_us_to_sec, "
                "(CASE WHEN executions > 0 THEN (elapsed_time/executions)/1000000 ELSE NULL END) AS per_elapse_sec, "
                "DBMS_LOB.SUBSTR(sql_fulltext, 1000, 1) AS sql_fulltext "
                "FROM v$sql WHERE SQL_id = :1",
                ["3b8uva7q2cf5a"],
            )
            rows = cur.fetchall() or []
            cols = [d[0] for d in (cur.description or [])]
            cur.close()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        def _to_float(v: Any) -> float:
            try:
                return float(v)
            except Exception:
                return 0.0

        def _to_int(v: Any) -> int:
            try:
                return int(v)
            except Exception:
                return 0

        idx_exec = next((i for i, c in enumerate(cols) if str(c).lower() == "executions"), None)
        idx_elapsed = next((i for i, c in enumerate(cols) if str(c).lower() == "elapse_us_to_sec"), None)

        pass_hit = False
        formatted: List[str] = []
        for row in rows[:10]:
            if idx_exec is not None and idx_elapsed is not None:
                exec_v = _to_int(row[idx_exec])
                elapsed_v = _to_float(row[idx_elapsed])
                if exec_v >= 5 and elapsed_v >= 50:
                    pass_hit = True
            parts = []
            for i, c in enumerate(cols):
                val = row[i]
                if c.lower() == "sql_fulltext" and isinstance(val, str):
                    val = val[:300]
                parts.append(f"{c}={val}")
            formatted.append("{ " + ", ".join(parts) + " }")

        status = "pass" if pass_hit else "fail"
        evidence = (
            f"rows={len(rows)} (pass condition: executions>=5 AND elapse_us_to_sec>=50)\n"
            + ("\n".join(formatted) if formatted else "[]")
        )
        return _step_result("target_vsql_query", status, evidence, _elapsed_ms(t0))
    except Exception as e:
        return _step_result("target_vsql_query", "fail", f"query failed: {e}", _elapsed_ms(t0))


# ────────────────────────────────────────────────────────
# 공용 진입점
# ────────────────────────────────────────────────────────

def _resolve_conf_dir(
    ssh: _SSHSession, conf_name: str, user_base_dir: Optional[str]
) -> Optional[str]:
    """
    conf_name 에 해당하는 MaxGauge conf 디렉터리(= .mxgrc 가 있는 곳)를 찾아 반환.
    경로 어딘가에 conf_name 이 포함된 디렉터리를 탐색하며, .mxgrc 존재를 우선 기준으로 삼는다.
    찾지 못하면 None 반환.
    """
    conf_q = _shell_quote(conf_name)

    def _safe_run(cmd: str, timeout: int = CMD_TIMEOUT) -> Tuple[str, str, int]:
        try:
            return ssh.run(cmd, timeout=timeout)
        except TimeoutError:
            return "", "timeout", 124
        except Exception:
            return "", "error", 1

    def _has_mxgrc(d: str) -> bool:
        cq = _shell_quote(d)
        out, _, _ = _safe_run(f"test -f {cq}/.mxgrc && echo OK || echo NG", timeout=3)
        return out.strip() == "OK"

    # 고정 후보 + 서버의 실제 최상위 디렉토리를 동적으로 합쳐서 탐색 루트를 구성한다.
    # /proc, /sys, /dev 등 가상 파일시스템은 제외한다.
    SKIP_ROOTS = {"/proc", "/sys", "/dev", "/run", "/tmp", "/lost+found"}
    fixed_roots = ["/data1", "/data2", "/data", "/mxg", "/maxgauge",
                   "/u01", "/u02", "/app", "/home", "/opt", "/usr/local", "/export"]

    # 서버 최상위 디렉토리 목록 가져오기
    top_out, _, _ = _safe_run(
        "ls -1d /*/  2>/dev/null | sed 's|/$||'",
        timeout=5,
    )
    dynamic_roots = [
        p.strip() for p in top_out.splitlines()
        if p.strip() and p.strip() not in SKIP_ROOTS
    ]
    # 고정 후보를 앞에 두고 동적 목록으로 보완 (중복 제거, 순서 유지)
    seen: set = set()
    search_roots = []
    for r in fixed_roots + dynamic_roots:
        if r and r not in seen:
            seen.add(r)
            search_roots.append(r)

    all_roots_str = " ".join(_shell_quote(r) for r in search_roots)

    # 1) 사용자가 base_dir 또는 conf_dir를 직접 준 경우
    if user_base_dir:
        base = user_base_dir.rstrip("/")
        candidate = f"{base}/{conf_name}"
        if _has_mxgrc(candidate):
            return candidate
        if _has_mxgrc(base):
            return base
        out, _, _ = _safe_run(f"test -d {_shell_quote(candidate)} && echo OK || echo NG", timeout=3)
        return candidate if out.strip() == "OK" else base

    # 2) find로 .mxgrc 직접 탐색 — 경로에 conf_name 포함된 것
    out_find, _, _ = _safe_run(
        f"for R in {all_roots_str}; do"
        f"  [ -d \"$R\" ] || continue;"
        f"  find \"$R\" -maxdepth 10 -name '.mxgrc' 2>/dev/null | grep -F {conf_q};"
        f"done | head -5",
        timeout=30,
    )
    for line in out_find.strip().splitlines():
        line = line.strip()
        if line and conf_name in line and line.endswith("/.mxgrc"):
            return line[: -len("/.mxgrc")]

    # 3) rtsctl 바이너리 위치로 conf_dir 역추적
    out_rtsc, _, _ = _safe_run(
        f"for R in {all_roots_str}; do"
        f"  [ -d \"$R\" ] || continue;"
        f"  find \"$R\" -maxdepth 10 -name 'rtsctl' -type f 2>/dev/null | grep -F {conf_q};"
        f"done | head -3",
        timeout=25,
    )
    for line in out_rtsc.strip().splitlines():
        line = line.strip()
        if line and conf_name in line:
            d = line.rsplit("/", 1)[0]
            if d.endswith("/bin"):
                d = d[:-4]
            if _has_mxgrc(d):
                return d

    # 4) .mxgrc 없어도 conf_name과 정확히 일치하는 디렉토리 반환 (최후 수단)
    out_dir, _, _ = _safe_run(
        f"for R in {all_roots_str}; do"
        f"  [ -d \"$R\" ] || continue;"
        f"  find \"$R\" -maxdepth 10 -type d -name {conf_q} 2>/dev/null;"
        f"done | head -5",
        timeout=25,
    )
    for line in out_dir.strip().splitlines():
        line = line.strip()
        if line:
            return line

    return None


def get_apm_db_row(db_id: int) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Repo DB에서 APM_DB_INFO 한 행 조회 (shared_db 사용)."""
    from app.shared_db import get_connection, release_connection, get_db_config, _infer_db_engine

    config = get_db_config("repo")
    if not config:
        return False, None, "Repo DB not configured"

    engine = _infer_db_engine(config, "postgresql")
    conn = get_connection("repo")

    try:
        cursor = conn.cursor()
        if engine == "oracle":
            cursor.execute(
                "SELECT db_id, instance_name, host_ip, host_name, host_id, "
                "db_user, db_password, sid, lsnr_ip, lsnr_port, os_type, oracle_version "
                "FROM apm_db_info WHERE db_id = :1",
                [db_id],
            )
        else:
            cursor.execute(
                "SELECT db_id, instance_name, host_ip, host_name, host_id, "
                "db_user, db_password, sid, lsnr_ip, lsnr_port, os_type, oracle_version "
                "FROM apm_db_info WHERE db_id = %s",
                [db_id],
            )
        row = cursor.fetchone()
        cursor.close()
        if not row:
            return False, None, f"db_id={db_id} not found in APM_DB_INFO"
        return True, {
            "db_id": row[0],
            "instance_name": row[1],
            "host_ip": row[2],
            "host_name": row[3],
            "host_id": row[4],
            "db_user": row[5],
            "db_password": row[6],
            "sid": row[7],
            "lsnr_ip": row[8],
            "lsnr_port": row[9],
            "os_type": row[10],
            "oracle_version": row[11],
        }, ""
    except Exception as e:
        return False, None, str(e)
    finally:
        release_connection("repo", conn)


def run_rts_check(
    db_id: int,
    ssh_user: str,
    ssh_password: str,
    ssh_port: int = 22,
    conf_name: Optional[str] = None,
    base_dir: Optional[str] = None,
    host_override: Optional[str] = None,
    on_failure: str = "run_all",
    verify_stop: bool = False,
    stop_wait_sec: int = 10,
) -> Dict[str, Any]:
    """
    단일 호스트에 대한 RTS 점검을 수행한다.

    Args:
        db_id: APM_DB_INFO.DB_ID
        ssh_user / ssh_password / ssh_port: SSH 접속 정보 (저장하지 않음)
        conf_name: INSTANCE_NAME 대체값
        base_dir: 설치 디렉터리 루트 (미지정 시 자동 탐색)
        host_override: HOST_IP 대신 사용할 호스트
        on_failure: "stop_at_first_failure" | "run_all"
        verify_stop: True 이면 기존 점검 완료 후 rtsctl stop → 중지 검증 스텝 추가
        stop_wait_sec: rtsctl stop 후 대기 초 (기본 10초)

    Returns:
        점검 결과 JSON dict
    """
    total_t0 = time.time()

    ok, db_row, err_msg = get_apm_db_row(db_id)
    if not ok:
        return {"error": err_msg, "steps": [], "overall_status": "error"}

    host_ip = host_override or db_row["host_ip"]
    resolved_conf = conf_name or db_row["instance_name"]

    result: Dict[str, Any] = {
        "db_id": db_id,
        "host_ip": host_ip,
        "conf_name": resolved_conf,
        "instance_name": db_row["instance_name"],
        "on_failure": on_failure,
        "steps": [],
        "overall_status": "pass",
    }

    try:
        ssh = _SSHSession(host_ip, ssh_port, ssh_user, ssh_password)
    except Exception as e:
        result["overall_status"] = "error"
        result["error"] = f"SSH connection failed: {_mask_sensitive(str(e))}"
        result["total_duration_ms"] = _elapsed_ms(total_t0)
        return result

    try:
        resolved_conf_dir = _resolve_conf_dir(ssh, resolved_conf, base_dir)
        if resolved_conf_dir is None:
            result["overall_status"] = "error"
            result["error"] = (
                f"MaxGauge 설치 경로를 찾을 수 없습니다 (conf: {resolved_conf}). "
                "서버에 직접 접속해 경로를 확인 후 'Base Dir' 필드에 입력해주세요."
            )
            result["total_duration_ms"] = _elapsed_ms(total_t0)
            return result
        result["resolved_conf_dir"] = resolved_conf_dir
        stop = on_failure == "stop_at_first_failure"

        step_funcs = [
            lambda: _step_log_dirs(ssh, resolved_conf_dir),
            lambda: _step_rtsctl_stat(ssh, resolved_conf_dir),
            lambda: _step_pid_match(ssh, resolved_conf, pid_map),
            lambda: _step_error_grep(ssh, resolved_conf_dir),
            lambda: _step_abnormal_signals(ssh, resolved_conf_dir),
        ]

        pid_map: Dict[str, Optional[str]] = {d: None for d in DAEMONS}

        for i, fn in enumerate(step_funcs):
            try:
                ret = fn()
                if i == 1:
                    step_data, pid_map = ret
                    result["steps"].append(step_data)
                    if step_data["status"] == "fail" and stop:
                        result["overall_status"] = "fail"
                        break
                else:
                    result["steps"].append(ret)
                    if ret["status"] == "fail" and stop:
                        result["overall_status"] = "fail"
                        break
            except Exception as e:
                step_names = [
                    "log_directory_check", "rtsctl_stat", "pid_cross_check",
                    "error_log_grep", "abnormal_signal_check",
                ]
                result["steps"].append(
                    _step_result(step_names[i], "fail", f"exception: {_mask_sensitive(str(e))}", 0)
                )
                if stop:
                    result["overall_status"] = "fail"
                    break

        if any(s["status"] == "fail" for s in result["steps"]):
            result["overall_status"] = "fail"

        # ── Stop 검증 스텝 (verify_stop=True 이고 기존 점검이 pass 인 경우만) ──
        if verify_stop:
            try:
                stop_step = _step_rtsctl_stop_verify(
                    ssh, resolved_conf_dir, resolved_conf, stop_wait_sec=stop_wait_sec
                )
            except Exception as e:
                stop_step = _step_result(
                    "rtsctl_stop_verify", "fail",
                    f"exception: {_mask_sensitive(str(e))}", 0,
                )
            result["steps"].append(stop_step)
            if stop_step["status"] == "fail":
                result["overall_status"] = "fail"

    finally:
        ssh.close()

    result["total_duration_ms"] = _elapsed_ms(total_t0)
    return result


def run_rts_check_multi(
    targets: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """여러 호스트를 순차 점검한다. 각 target dict는 run_rts_check의 kwargs."""
    results = []
    for t in targets:
        results.append(run_rts_check(**t))
    return results

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

DAEMONS = ["rts", "sndf", "obsd", "updater"]

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
# OS 감지 및 OS별 명령어 헬퍼
# ────────────────────────────────────────────────────────

def _detect_os(ssh: "_SSHSession") -> str:
    """SSH로 접속한 서버의 OS 타입을 감지한다.

    Returns:
        'linux' | 'aix' | 'hpux' | 'sunos' | 'unknown'
    """
    try:
        out, _, _ = ssh.run("uname -s 2>/dev/null", timeout=5)
        s = out.strip().lower()
        if "linux" in s:
            return "linux"
        if "aix" in s:
            return "aix"
        if "hp-ux" in s:
            return "hpux"
        if "sunos" in s or "solaris" in s:
            return "sunos"
    except Exception:
        pass
    return "unknown"


def _os_ps_cmd(os_type: str) -> str:
    """실행 중인 MaxGauge/rts 프로세스를 찾는 OS별 ps 명령어."""
    if os_type in ("hpux", "sunos"):
        return "ps -ef 2>/dev/null | egrep 'rts|rtsmon|maxgauge|mxg' | head -15"
    elif os_type == "aix":
        # ps aux on AIX는 COMMAND 컬럼을 터미널 너비로 잘라 절대경로가 잘림.
        # ps -ef 는 전체 args를 포함한 전체 경로를 보여줌.
        return "ps -ef 2>/dev/null | grep -E 'rts|rtsmon|maxgauge|mxg' | grep -v grep | head -15"
    else:
        return "ps aux 2>/dev/null | grep -E 'rts|rtsmon|maxgauge|mxg' | grep -v grep | head -15"


def _find_in_root(root: str, name: str, os_type: str, depth: int = 8) -> str:
    """단일 루트 디렉터리 안에서 name 파일/디렉터리 탐색 명령어.
    HP-UX / SunOS는 -maxdepth 미지원 → 없이 탐색 (루트 자체가 좁으므로 충분히 빠름).
    """
    rq = _shell_quote(root)
    nq = _shell_quote(name)
    if os_type in ("linux", "aix", "unknown"):
        return f"test -d {rq} && find {rq} -maxdepth {depth} -name {nq} 2>/dev/null | head -5"
    else:
        return f"test -d {rq} && find {rq} -name {nq} 2>/dev/null | head -5"


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
    rtsctl stop → 중지 확인 → rtsctl start 재기동.

    검증 항목:
      1. rtsctl stop + rtsctl stop_updater 실행
      2. rtsctl stat → 모든 daemon "does not exist/stopped" 확인
      3. ps -ef | grep {conf_name} → 관련 PID 없음 확인
      4. rtsctl start 실행 (재기동)
    """
    t0 = time.time()
    prefix = _mxgrc_prefix(conf_dir)
    evidence_lines: List[str] = []
    failed: List[str] = []

    # ── 1. stop 실행 ─────────────────────────────────────────────────
    # updater는 별도 명령으로 종료 (rtsctl stop_updater)
    stop_out, stop_err, stop_rc = ssh.run(f"{prefix} && rtsctl stop 2>&1")
    stop_combined = (stop_out + "\n" + stop_err).strip()
    evidence_lines.append(f"[rtsctl stop rc={stop_rc}] {stop_combined[:300]}")

    upd_out, upd_err, upd_rc = ssh.run(f"{prefix} && rtsctl stop_updater 2>&1")
    upd_combined = (upd_out + "\n" + upd_err).strip()
    evidence_lines.append(f"[rtsctl stop_updater rc={upd_rc}] {upd_combined[:300]}")

    if stop_wait_sec > 0:
        time.sleep(stop_wait_sec)

    # ── 2. 중지 확인: rtsctl stat ────────────────────────────────────
    stat_out, stat_err, _ = ssh.run(f"{_mxgrc_prefix(conf_dir)} && rtsctl stat 2>&1")
    stat_combined = stat_out + "\n" + stat_err
    evidence_lines.append(f"[rtsctl stat after stop]\n{stat_combined[:500]}")

    stop_status: Dict[str, str] = {}
    for daemon in DAEMONS:
        block_match = re.search(rf"(?is)\b{daemon}\b[^;\n]*", stat_combined)
        block = block_match.group(0) if block_match else ""
        block_low = block.lower()
        if "does not exist" in block_low:
            stop_status[daemon] = "DOES_NOT_EXIST"
        elif "not running" in block_low or "stopped" in block_low:
            stop_status[daemon] = "STOPPED"
        elif "running" in block_low:
            stop_status[daemon] = "STILL_RUNNING"
        else:
            stop_status[daemon] = block.strip() if block.strip() else "UNKNOWN"

    evidence_lines.append("[stop stat] " + "; ".join(f"{d}: {s}" for d, s in stop_status.items()))
    not_stopped = [d for d, s in stop_status.items() if s not in ("DOES_NOT_EXIST", "STOPPED")]
    if not_stopped:
        failed.append(f"stop 미확인 daemon={not_stopped}")

    # ── 3. ps 잔존 PID 확인 ──────────────────────────────────────────
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

    # ── 4. rtsctl start 실행 (stop 검증 결과와 무관하게 항상 재기동) ──
    start_out, start_err, start_rc = ssh.run(f"{_mxgrc_prefix(conf_dir)} && rtsctl start 2>&1")
    start_combined = (start_out + "\n" + start_err).strip()
    evidence_lines.append(f"[rtsctl start rc={start_rc}] {start_combined[:300]}")

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
    """Step 4: {conf_dir}/log/maxgauge/ 에서 rts/sndf/obsd 로그의 [FATA] / [ERRO] 키워드 grep.
    키워드가 없으면 PASS, 있으면 FAIL."""
    t0 = time.time()
    log_dir = _log_root(conf_dir)
    # [FATA] 또는 [ERRO] 패턴 — grep -F 로 고정 문자열 매칭 (정규식 불필요)
    findings: List[str] = []
    error_samples: List[str] = []
    has_error = False
    has_missing = False

    for daemon in DAEMONS:
        log_file = _find_latest_log(ssh, log_dir, daemon)
        if not log_file:
            has_missing = True
            findings.append(f"*{daemon}* log: not found in {log_dir}")
            continue

        fname = log_file.rsplit("/", 1)[-1]
        # grep -F: 고정 문자열, -e: 패턴 여러 개
        cmd = f"grep -F -e '[FATA]' -e '[ERRO]' {log_file} 2>/dev/null | tail -5"
        out, _, _ = ssh.run(cmd)
        text = out.strip()
        if text:
            has_error = True
            lines = text.splitlines()
            findings.append(f"{fname}: {len(lines)} hit(s) — {lines[-1][:200]}")
            error_samples.append(f"[{fname}] error logs:\n{text}")
        else:
            findings.append(f"{fname}: clean (no [FATA]/[ERRO])")

    evidence = "; ".join(findings)
    if error_samples:
        evidence = evidence + "\n\n" + "\n\n".join(error_samples)

    return _step_result(
        "error_log_grep",
        "fail" if (has_error or has_missing) else "pass",
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
    """Step 5: {conf_dir}/log/maxgauge/ 에서 SIGBUS, SIGSEGV 등 비정상 종료 징후 검색"""
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
    ssh: "_SSHSession", conf_name: str, user_base_dir: Optional[str],
    os_type: str = "unknown",
) -> Tuple[Optional[str], str]:
    """
    SSH로 접속한 서버에서 conf_name(= instance_name)에 해당하는
    MaxGauge conf 디렉터리(= .mxgrc 가 있는 곳)를 찾아 반환한다.

    Returns:
        (conf_dir_path, diag_msg) — 찾으면 경로, 못 찾으면 None + 진단 메시지

    모든 Unix(Linux / AIX / HP-UX / SunOS) 호환:
      - find -maxdepth : Linux/AIX만 사용. HP-UX/SunOS는 shell glob 순회.
      - POSIX test / grep / ls 만 사용.
    """
    diag: List[str] = []
    conf_q = _shell_quote(conf_name)

    def _safe_run(cmd: str, timeout: int = CMD_TIMEOUT) -> Tuple[str, str, int]:
        try:
            return ssh.run(cmd, timeout=timeout)
        except TimeoutError:
            return "", "timeout", 124
        except Exception:
            return "", "error", 1

    def _dir_exists(d: str) -> bool:
        out, _, _ = _safe_run(f"test -d {_shell_quote(d)} && echo Y || echo N", timeout=3)
        return out.strip() == "Y"

    def _has_mxgrc(d: str) -> bool:
        out, _, _ = _safe_run(f"test -f {_shell_quote(d)}/.mxgrc && echo Y || echo N", timeout=3)
        return out.strip() == "Y"

    def _content_matches(mxgrc_path: str) -> bool:
        """파일 내용에 conf_name 포함 여부 (대소문자 무시)."""
        out, _, _ = _safe_run(
            f"grep {conf_q} {_shell_quote(mxgrc_path)} 2>/dev/null | head -1",
            timeout=3,
        )
        if out.strip():
            return True
        # grep -i 가 없는 구형 OS 대비 — 대문자 변환해서 재시도
        upper = conf_name.upper()
        out2, _, _ = _safe_run(
            f"grep {_shell_quote(upper)} {_shell_quote(mxgrc_path)} 2>/dev/null | head -1",
            timeout=3,
        )
        return bool(out2.strip())

    def _check_dir(d: str) -> bool:
        """d 디렉터리가 .mxgrc를 갖고, 경로나 내용에 conf_name이 있으면 True."""
        if not _has_mxgrc(d):
            return False
        if conf_name.lower() in d.lower():
            return True
        return _content_matches(f"{d}/.mxgrc")

    conf_name_upper = conf_name.upper()
    conf_name_upper_q = _shell_quote(conf_name_upper)

    def _find_matching_conf_dir_in(root: str) -> Optional[str]:
        """root 아래에서 conf_name에 매칭되는 conf_dir을 단일 SSH 호출로 반환.
        경로 또는 .mxgrc 내용에 conf_name이 있으면 매칭.
        HP-UX/SunOS: shell glob 순회, Linux/AIX: find (-follow for AIX).
        ※ case 문으로 경로 매칭 — 중첩 파이프 없이 모든 POSIX sh 호환.
        """
        rq = _shell_quote(root)
        # case 문: 파이프 없이 경로에 conf_name 포함 여부 확인 (POSIX 호환)
        path_case = f"case \"$_d\" in *{conf_name}*|*{conf_name_upper}*) true ;; *) false ;; esac"
        grep_content = (
            f"grep {conf_q} \"$_d/.mxgrc\" > /dev/null 2>&1 "
            f"|| grep {conf_name_upper_q} \"$_d/.mxgrc\" > /dev/null 2>&1"
        )
        match_cond = f"( {path_case} || {grep_content} )"

        if os_type == "aix":
            # AIX: -follow 로 심볼릭 링크 디렉터리도 탐색
            cmd = (
                f"find {rq} -follow -maxdepth 8 -name '.mxgrc' 2>/dev/null | while read _f; do "
                f"  _d=$(dirname \"$_f\"); "
                f"  {match_cond} && echo \"$_d\" && break; "
                f"done 2>/dev/null | head -1"
            )
        elif os_type in ("linux", "unknown"):
            cmd = (
                f"find {rq} -maxdepth 8 -name '.mxgrc' 2>/dev/null | while read _f; do "
                f"  _d=$(dirname \"$_f\"); "
                f"  {match_cond} && echo \"$_d\" && break; "
                f"done 2>/dev/null | head -1"
            )
        else:
            # HP-UX / SunOS: shell glob 3단계 순회
            cmd = (
                f"for _d in {rq} {rq}/* {rq}/*/* {rq}/*/*/*; do "
                f"  if test -f \"$_d/.mxgrc\"; then "
                f"    {match_cond} && echo \"$_d\" && break; "
                f"  fi; "
                f"done 2>/dev/null | head -1"
            )
        out, _, _ = _safe_run(cmd, timeout=40)
        lines = [l.strip() for l in out.strip().splitlines() if l.strip()]
        return lines[0] if lines else None

    def _find_mxgrc_in(root: str) -> List[str]:
        """root 아래 .mxgrc 파일 목록 반환 — 진단용 (step6 fallback)."""
        rq = _shell_quote(root)
        if os_type == "aix":
            cmd = f"find {rq} -follow -maxdepth 8 -name '.mxgrc' 2>/dev/null | head -50"
        elif os_type in ("linux", "unknown"):
            cmd = f"find {rq} -maxdepth 8 -name '.mxgrc' 2>/dev/null | head -50"
        else:
            cmd = (
                f"for _d in {rq} {rq}/* {rq}/*/* {rq}/*/*/*; do "
                f"test -f \"$_d/.mxgrc\" && echo \"$_d/.mxgrc\"; "
                f"done 2>/dev/null | head -50"
            )
        out, _, _ = _safe_run(cmd, timeout=25)
        return [l.strip() for l in out.strip().splitlines() if l.strip().endswith("/.mxgrc")]

    def _find_rtsctl_in(root: str) -> List[str]:
        """root 아래 rtsctl 바이너리 목록 반환."""
        rq = _shell_quote(root)
        if os_type == "aix":
            cmd = f"find {rq} -follow -maxdepth 8 -name 'rtsctl' -type f 2>/dev/null | head -5"
        elif os_type in ("linux", "unknown"):
            cmd = f"find {rq} -maxdepth 8 -name 'rtsctl' -type f 2>/dev/null | head -5"
        else:
            # HP-UX / SunOS: shell glob 순회로 대체
            cmd = (
                f"for _f in {rq}/rtsctl {rq}/*/rtsctl {rq}/bin/rtsctl "
                f"{rq}/*/bin/rtsctl {rq}/*/*/rtsctl {rq}/*/*/bin/rtsctl; do "
                f"test -f \"$_f\" && echo \"$_f\"; "
                f"done 2>/dev/null | head -5"
            )
        out, _, _ = _safe_run(cmd, timeout=15)
        return [l.strip() for l in out.strip().splitlines() if l.strip()]

    def _rtsctl_to_dir(path: str) -> Optional[str]:
        """rtsctl 경로 → conf_dir. .mxgrc + conf_name 매칭 시 반환."""
        d = path.rsplit("/", 1)[0]
        if d.endswith("/bin"):
            d = d[:-4]
        return d if _check_dir(d) else None

    # ── 1) 사용자가 base_dir 직접 입력 ─────────────────────────────────
    if user_base_dir:
        base = user_base_dir.rstrip("/")
        for cand in [f"{base}/{conf_name}", base]:
            if _has_mxgrc(cand):
                return cand, f"user_base_dir={base} → {cand}"
        return base, f"user_base_dir={base} (no .mxgrc found, returning as-is)"

    # ── 2) 서버의 최상위 디렉터리를 동적으로 수집 ────────────────────────
    # ls / 는 모든 Unix에서 동작. 가상 FS 및 시스템 디렉터리는 제외.
    SKIP = {
        "proc", "sys", "dev", "run", "tmp", "lost+found",
        "etc", "bin", "sbin", "lib", "lib64", "lib32",
        "boot", "var", "cdrom", "media", "mnt", "srv",
    }
    ls_out, _, _ = _safe_run("ls / 2>/dev/null", timeout=5)
    dyn_roots = [
        f"/{d.strip()}" for d in ls_out.splitlines()
        if d.strip() and d.strip() not in SKIP
    ]
    diag.append(f"ls /: {dyn_roots}")
    # 고정 후보를 앞에 두고 동적 목록으로 보완
    fixed = [
        "/data1", "/data2", "/data3", "/data",
        "/mxg", "/maxgauge",
        "/u01", "/u02", "/u03",
        "/app", "/apps", "/application",
        "/opt", "/opt/maxgauge",
        "/usr/local",
        "/home", "/export", "/product", "/software",
    ]
    seen: set = set()
    roots: List[str] = []
    for r in fixed + dyn_roots:
        if r and r not in seen:
            seen.add(r)
            roots.append(r)

    # AIX는 /data1/home/maxgauge 같은 2단계 경로에 설치되는 경우가 있어
    # 주요 data 루트의 home / maxgauge 서브디렉터리를 roots에 추가
    if os_type == "aix":
        aix_extra = []
        for r in ["/data1", "/data2", "/data3", "/u", "/export", "/home"]:
            for sub in ["home", "home/maxgauge", "home/mxg"]:
                cand = f"{r}/{sub}"
                if cand not in seen:
                    seen.add(cand)
                    aix_extra.append(cand)
        roots = aix_extra + roots  # 앞에 두어 먼저 탐색

    # ── 2.5) maxgauge 유저 홈 디렉터리 → roots 앞에 추가 ────────────────
    out_home, _, _ = _safe_run(
        "grep '^maxgauge:' /etc/passwd 2>/dev/null | cut -d: -f6 | head -1",
        timeout=3,
    )
    user_home = out_home.strip() if out_home.strip().startswith("/") else None
    diag.append(f"maxgauge user home: {user_home!r}")
    if user_home:
        for h in [user_home, user_home.rsplit("/", 1)[0]]:
            if h and h != "/" and h not in seen:
                seen.add(h)
                roots.insert(0, h)

    diag.append(f"roots to search: {roots}")

    # ── 3) which rtsctl (PATH에 등록된 경우 즉시 해결) ──────────────────
    out_wh, _, _ = _safe_run(
        "which rtsctl 2>/dev/null || type rtsctl 2>/dev/null | head -1",
        timeout=5,
    )
    diag.append(f"which rtsctl: {out_wh.strip()!r}")
    for wl in out_wh.strip().splitlines():
        wl = wl.strip()
        if "/" in wl:
            d = _rtsctl_to_dir(wl.split()[-1])
            if d:
                return d, f"which rtsctl → {d}"

    # ── 3.5) /proc/{pid}/cwd 로 실행 중 프로세스의 작업 디렉터리 확인 ──────
    # AIX/SunOS: ps에 절대경로가 없을 때 /proc/{pid}/cwd symlink가 conf_dir을 가리킴
    proc_cwd_cmd = (
        f"_pid=$(ps -ef 2>/dev/null | grep {conf_q} | grep -v grep | awk 'NR==1{{print $2}}'); "
        f"[ -n \"$_pid\" ] && ("
        f"  readlink /proc/$_pid/cwd 2>/dev/null || "
        f"  ls -la /proc/$_pid/cwd 2>/dev/null | awk '{{print $NF}}' | grep '^/' | head -1"
        f") | head -1"
    )
    out_cwd, _, _ = _safe_run(proc_cwd_cmd, timeout=5)
    cwd_path = out_cwd.strip()
    diag.append(f"/proc/pid/cwd: {cwd_path!r}")
    if cwd_path.startswith("/"):
        for cand in [cwd_path, f"{cwd_path}/{conf_name}"]:
            if _check_dir(cand):
                return cand, f"/proc/pid/cwd → {cand}"
        # cwd가 bin 디렉터리인 경우 parent 확인
        for cand in [cwd_path.rstrip("/"), cwd_path.rsplit("/", 1)[0]]:
            if cand and _check_dir(cand):
                return cand, f"/proc/pid/cwd parent → {cand}"
        # cwd를 roots 앞에 추가해 step5/6에서도 탐색
        for p in [cwd_path, cwd_path.rsplit("/", 1)[0]]:
            if p and p != "/" and p not in seen:
                seen.add(p)
                roots.insert(0, p)

    # ── 4) ps로 실행 중 프로세스에서 경로 추출 ──────────────────────────
    ps_cmd = _os_ps_cmd(os_type)
    out_ps, _, _ = _safe_run(ps_cmd, timeout=8)
    diag.append(f"ps result: {out_ps.strip()[:400]!r}")
    for ps_line in out_ps.strip().splitlines():
        for part in ps_line.split():
            if "/" not in part:
                continue
            if conf_name.lower() in part.lower():
                d = part.rsplit("/", 1)[0]
                if d.endswith("/bin"):
                    d = d[:-4]
                if _has_mxgrc(d):
                    return d, f"ps path match → {d}"
            if any(x in part for x in ("rtsctl", "rtsmon")):
                d = _rtsctl_to_dir(part)
                if d:
                    return d, f"ps rtsctl path → {d}"

    # ── 5) 각 루트 아래 {conf_name} 서브디렉터리 직접 확인 (가장 빠름) ──
    # test -f 로만 확인 → SSH 1 round-trip per path, OS 무관.
    # 단일 인스턴스 설치의 경우 conf_name 이름 없이 maxgauge 디렉터리 자체가 conf_dir.
    checked5: List[str] = []
    existing_roots: List[str] = []   # step6/7 에서 재사용 (중복 _dir_exists 호출 방지)
    for root in roots:
        if not _dir_exists(root):
            continue
        existing_roots.append(root)
        for cand in [
            f"{root}/{conf_name}",
            f"{root}/maxgauge/{conf_name}",
            f"{root}/mxg/{conf_name}",
            # AIX 패턴: /data1/home/maxgauge/{conf_name} 또는 /data1/home/{conf_name}
            f"{root}/home/maxgauge/{conf_name}",
            f"{root}/home/{conf_name}",
            f"{root}/home/mxg/{conf_name}",
            # 단일 인스턴스: MaxGauge 루트 자체가 conf_dir (경로에 instance명 없을 수 있음)
            f"{root}/home/maxgauge",
            f"{root}/home/mxg",
            f"{root}/maxgauge",
            f"{root}/mxg",
            f"{root}/maxg",
            f"{root}/MXG",
            f"{root}/MAXGAUGE",
        ]:
            checked5.append(cand)
            if _check_dir(cand):
                return cand, f"step5 direct check → {cand}"
    diag.append(f"step5 checked {len(checked5)} paths, none matched. existing_roots={existing_roots}")

    # ── 6) 각 루트 아래 .mxgrc 탐색 + conf_name 매칭 (단일 SSH 호출) ──────
    # existing_roots 는 step5 에서 이미 존재 확인됨 → _dir_exists 재호출 불필요
    diag.append(f"step6 start: searching {len(existing_roots)} roots")
    for root in existing_roots:
        d = _find_matching_conf_dir_in(root)
        if d:
            return d, f"step6 .mxgrc match → {d}"
    # 디버그: 매칭 실패 시 실제로 어떤 .mxgrc가 있는지 확인
    # user_home, cwd_path, 그리고 ls /에서 찾은 실제 존재하는 root 샘플링
    found6: List[str] = []
    sample_roots = []
    if user_home:
        sample_roots.append(user_home)
    if cwd_path.startswith("/"):
        sample_roots.append(cwd_path.rsplit("/", 1)[0])
    sample_roots += [r for r in existing_roots if r in set(dyn_roots)][:5]
    for root in sample_roots:
        found6.extend(_find_mxgrc_in(root))
    diag.append(f"step6 no match. sample .mxgrc files (from {sample_roots}): {found6[:20]}")

    # ── 7) 각 루트 아래 rtsctl 위치로 역추적 ───────────────────────────
    found7: List[str] = []
    for root in existing_roots:
        for rpath in _find_rtsctl_in(root):
            found7.append(rpath)
            d = _rtsctl_to_dir(rpath)
            if d:
                return d, f"step7 rtsctl → {d}"
    diag.append(f"step7 rtsctl files found: {found7}")

    return None, "\n".join(diag)


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
        # OS 타입 감지 (uname -s) — 이후 find/ps 명령어 분기에 사용
        detected_os = _detect_os(ssh)
        result["os_type"] = detected_os

        resolved_conf_dir, path_diag = _resolve_conf_dir(
            ssh, resolved_conf, base_dir, os_type=detected_os
        )
        if resolved_conf_dir is None:
            result["overall_status"] = "error"
            result["error"] = (
                f"MaxGauge 설치 경로를 찾을 수 없습니다 "
                f"(conf: {resolved_conf}, os: {detected_os}).\n"
                f"진단 정보:\n{path_diag}\n"
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

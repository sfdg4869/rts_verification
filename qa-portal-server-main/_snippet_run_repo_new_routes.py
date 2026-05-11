# Snippet from agent transcript 1639766a-2bdd-400a-9ad4-c152a2f349a0.jsonl (merged patches).
# Assumes `bp = Blueprint("rts_check", __name__, url_prefix="/api/v2/rts/check")` exists in the target module.
# Add these imports next to the module's existing ones if missing:
import json
import os
import threading
import time
import uuid

from flask import jsonify, request

_REPO_JOB_LOCK = threading.Lock()
_REPO_JOBS = {}
_REPO_JOB_STORE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "resource",
    "repo_jobs_state.json",
)


def _persist_repo_jobs() -> None:
    os.makedirs(os.path.dirname(_REPO_JOB_STORE_PATH), exist_ok=True)
    tmp = _REPO_JOB_STORE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_REPO_JOBS, f, ensure_ascii=False)
    os.replace(tmp, _REPO_JOB_STORE_PATH)


def _load_repo_jobs_from_disk() -> None:
    if not os.path.exists(_REPO_JOB_STORE_PATH):
        return
    try:
        with open(_REPO_JOB_STORE_PATH, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            _REPO_JOBS.update(loaded)
    except Exception:
        return


@bp.route("/run-repo-new", methods=["POST"])
def run_repo_new_api():
    from app.services.new_repo_check_service import run_new_repo_check
    from app.services.target_sql_test_service import _get_apm_db_row_with_secret

    data = request.get_json(silent=True) or {}
    db_id = data.get("db_id")
    target_cfg = data.get("target_config")
    sys_password = data.get("sys_password")
    case4_loops = int(data.get("case4_loops", 200))
    case5_loops = int(data.get("case5_loops", 30))
    case5_rows = int(data.get("case5_rows", 50000))
    repo_db_id_list = data.get("repo_db_id_list")
    repo_partition_date = data.get("repo_partition_date")
    repo_logging_time = data.get("repo_logging_time")
    repo_schema_name = (data.get("schema_name") or data.get("repo_pg_schema") or "").strip() or None

    if not db_id and not target_cfg:
        return jsonify({"error": "db_id or target_config is required"}), 400

    try:
        if not target_cfg or not target_cfg.get("password"):
            ok, row, err = _get_apm_db_row_with_secret(int(db_id))
            if not ok or not row:
                return jsonify({"error": err}), 400

            if not target_cfg:
                target_cfg = {
                    "host": row["host_ip"],
                    "port": row["lsnr_port"],
                    "user": row["db_user"],
                    "password": row["db_password"],
                    "sid": row["sid"],
                    "instance_name": row["instance_name"]
                }
            else:
                target_cfg["password"] = row["db_password"]

        result = run_new_repo_check(
            int(db_id),
            target_cfg,
            sys_password,
            case4_loops=case4_loops,
            case5_loops=case5_loops,
            case5_rows=case5_rows,
            repo_db_id_list=repo_db_id_list,
            repo_partition_date=repo_partition_date,
            repo_logging_time=repo_logging_time,
            repo_schema_name=repo_schema_name,
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/run-repo-new-job", methods=["POST"])
def run_repo_new_job_api():
    """Repo 신규 점검 비동기 job 시작"""
    from app.services.new_repo_check_service import run_new_repo_check
    from app.services.target_sql_test_service import _get_apm_db_row_with_secret

    data = request.get_json(silent=True) or {}
    db_id = data.get("db_id")
    target_cfg = data.get("target_config")
    sys_password = data.get("sys_password")
    case4_loops = int(data.get("case4_loops", 200))
    case5_loops = int(data.get("case5_loops", 30))
    case5_rows = int(data.get("case5_rows", 50000))
    repo_db_id_list = data.get("repo_db_id_list")
    repo_partition_date = data.get("repo_partition_date")
    repo_logging_time = data.get("repo_logging_time")
    repo_schema_name = (data.get("schema_name") or data.get("repo_pg_schema") or "").strip() or None

    if not db_id and not target_cfg:
        return jsonify({"error": "db_id or target_config is required"}), 400

    job_id = str(uuid.uuid4())
    with _REPO_JOB_LOCK:
        _REPO_JOBS[job_id] = {
            "job_id": job_id,
            "type": "repo_new",
            "status": "running",
            "started_at": int(time.time()),
            "completed_steps": 0,
            "total_steps": 6,
            "current_step": "",
            "current_step_status": "",
            "progress_pct": 0,
            "result": None,
            "error": None,
        }
        _persist_repo_jobs()

    def _worker():
        try:
            resolved_target_cfg = target_cfg
            resolved_db_id = int(db_id) if db_id else None
            if not resolved_target_cfg or not resolved_target_cfg.get("password"):
                ok, row, err = _get_apm_db_row_with_secret(int(resolved_db_id))
                if not ok or not row:
                    raise RuntimeError(err)
                if not resolved_target_cfg:
                    resolved_target_cfg = {
                        "host": row["host_ip"],
                        "port": row["lsnr_port"],
                        "user": row["db_user"],
                        "password": row["db_password"],
                        "sid": row["sid"],
                        "instance_name": row["instance_name"],
                    }
                else:
                    resolved_target_cfg["password"] = row["db_password"]

            def _progress(done: int, total: int, step_name: str, step_status: str):
                with _REPO_JOB_LOCK:
                    job = _REPO_JOBS.get(job_id)
                    if not job:
                        return
                    job["completed_steps"] = int(done)
                    job["total_steps"] = int(total)
                    job["current_step"] = step_name
                    job["current_step_status"] = step_status
                    job["progress_pct"] = int((done / total) * 100) if total else 0
                    _persist_repo_jobs()

            result = run_new_repo_check(
                int(resolved_db_id),
                resolved_target_cfg,
                sys_password,
                progress_callback=_progress,
                case4_loops=case4_loops,
                case5_loops=case5_loops,
                case5_rows=case5_rows,
                repo_db_id_list=repo_db_id_list,
                repo_partition_date=repo_partition_date,
                repo_logging_time=repo_logging_time,
                repo_schema_name=repo_schema_name,
            )
            with _REPO_JOB_LOCK:
                job = _REPO_JOBS.get(job_id)
                if job:
                    job["status"] = "done"
                    job["completed_steps"] = 6
                    job["progress_pct"] = 100
                    job["result"] = result
                    _persist_repo_jobs()
        except Exception as e:
            with _REPO_JOB_LOCK:
                job = _REPO_JOBS.get(job_id)
                if job:
                    job["status"] = "error"
                    job["error"] = str(e)
                    _persist_repo_jobs()

    threading.Thread(target=_worker, daemon=True, name=f"repo-new-job-{job_id[:8]}").start()
    return jsonify({"job_id": job_id, "status": "running"}), 202


@bp.route("/run-repo-new-job/<job_id>", methods=["GET"])
def run_repo_new_job_status_api(job_id: str):
    """Repo 신규 점검 비동기 job 상태/결과 조회"""
    with _REPO_JOB_LOCK:
        job = _REPO_JOBS.get(job_id)
        if not job:
            _load_repo_jobs_from_disk()
            job = _REPO_JOBS.get(job_id)
            if not job:
                return jsonify({"error": "job not found"}), 404
        return jsonify(job), 200

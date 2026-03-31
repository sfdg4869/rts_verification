"""
RTS 프로세스 상태 점검 라우트
MaxGauge RTS 데몬(rts/sndf/obsd)의 상태를 SSH로 점검하는 API.
"""

from flask import Blueprint, jsonify, request
from flasgger import swag_from

bp = Blueprint("rts_check", __name__, url_prefix="/api/v2/rts/check")


_RUN_SPEC = {
    "tags": ["RTS Check"],
    "summary": "단일 호스트 RTS 상태 점검",
    "description": (
        "APM_DB_INFO에서 db_id로 대상 호스트를 조회한 뒤, "
        "SSH로 접속하여 MaxGauge RTS 데몬(rts/sndf/obsd)의 상태를 순차 점검한다.\n\n"
        "**6-step 점검:**\n"
        "1. 로그 디렉터리 존재 여부\n"
        "2. rtsctl stat (RUNNING 여부)\n"
        "3. PID 교차 검증 (ps vs rtsctl)\n"
        "4. ERROR/ERRO grep\n"
        "5. CPU/MEM 사용률\n"
        "6. SIGBUS/SIGSEGV 등 비정상 종료 키워드\n\n"
        "비밀번호는 서버에 저장되지 않으며, evidence에서 민감정보는 마스킹된다."
    ),
    "consumes": ["application/json"],
    "parameters": [
        {
            "in": "body",
            "name": "body",
            "required": True,
            "schema": {
                "type": "object",
                "required": ["db_id", "ssh_user", "ssh_password"],
                "properties": {
                    "db_id": {
                        "type": "integer",
                        "description": "APM_DB_INFO.DB_ID",
                        "example": 1,
                    },
                    "ssh_user": {
                        "type": "string",
                        "description": "SSH 접속 사용자명",
                        "example": "maxgauge",
                    },
                    "ssh_password": {
                        "type": "string",
                        "description": "SSH 비밀번호 (저장 안 함)",
                        "example": "password123",
                    },
                    "ssh_port": {
                        "type": "integer",
                        "description": "SSH 포트 (기본 22)",
                        "default": 22,
                    },
                    "conf_name": {
                        "type": "string",
                        "description": "설정 이름 (미지정 시 INSTANCE_NAME 사용)",
                    },
                    "base_dir": {
                        "type": "string",
                        "description": "설치 디렉터리 루트 (미지정 시 자동 탐색)",
                        "example": "/home/maxgauge",
                    },
                    "host_override": {
                        "type": "string",
                        "description": "HOST_IP 대신 사용할 호스트",
                    },
                    "on_failure": {
                        "type": "string",
                        "enum": ["run_all", "stop_at_first_failure"],
                        "default": "run_all",
                        "description": "실패 시 정책",
                    },
                },
            },
        }
    ],
    "responses": {
        200: {
            "description": "점검 결과",
            "schema": {
                "type": "object",
                "properties": {
                    "db_id": {"type": "integer"},
                    "host_ip": {"type": "string"},
                    "conf_name": {"type": "string"},
                    "on_failure": {"type": "string"},
                    "overall_status": {
                        "type": "string",
                        "enum": ["pass", "fail", "error"],
                    },
                    "total_duration_ms": {"type": "integer"},
                    "steps": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "step": {"type": "string"},
                                "status": {
                                    "type": "string",
                                    "enum": ["pass", "fail", "skip"],
                                },
                                "evidence": {"type": "string"},
                                "duration_ms": {"type": "integer"},
                            },
                        },
                    },
                },
            },
        },
        400: {"description": "필수 파라미터 누락"},
        500: {"description": "서버 오류"},
    },
}

_RUN_MULTI_SPEC = {
    "tags": ["RTS Check"],
    "summary": "다중 호스트 RTS 상태 점검",
    "description": (
        "targets 배열에 포함된 여러 호스트에 대해 순차적으로 RTS 점검을 수행한다.\n"
        "각 target 항목의 스키마는 POST /run과 동일하다."
    ),
    "consumes": ["application/json"],
    "parameters": [
        {
            "in": "body",
            "name": "body",
            "required": True,
            "schema": {
                "type": "object",
                "required": ["targets"],
                "properties": {
                    "targets": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["db_id", "ssh_user", "ssh_password"],
                            "properties": {
                                "db_id": {"type": "integer"},
                                "ssh_user": {"type": "string"},
                                "ssh_password": {"type": "string"},
                                "ssh_port": {"type": "integer", "default": 22},
                                "conf_name": {"type": "string"},
                                "base_dir": {"type": "string"},
                                "host_override": {"type": "string"},
                                "on_failure": {
                                    "type": "string",
                                    "enum": ["run_all", "stop_at_first_failure"],
                                    "default": "run_all",
                                },
                            },
                        },
                        "description": "점검 대상 목록",
                    }
                },
            },
        }
    ],
    "responses": {
        200: {
            "description": "각 호스트별 점검 결과 배열",
            "schema": {
                "type": "object",
                "properties": {
                    "results": {"type": "array", "items": {"type": "object"}},
                    "total_count": {"type": "integer"},
                    "fail_count": {"type": "integer"},
                },
            },
        },
        400: {"description": "필수 파라미터 누락"},
        500: {"description": "서버 오류"},
    },
}

_RUN_REPO_SPEC = {
    "tags": ["RTS Check"],
    "summary": "Repo DB 로깅 점검 실행",
    "description": (
        "Repo DB 테이블 기반으로 다음 6개 항목을 점검한다.\n"
        "1) APM_TOP_OS_PROCESS_LIST 누락 재현 여부\n"
        "2) ORA_SQL_ELAPSE 1초 이상 SQL Elapsed 로깅\n"
        "3) ORA_SQL_STAT_10MIN 50초 이상만 로깅 여부\n"
        "4) ORA_SQL_ELAPSE Elapsed/Execution 로깅 여부\n"
        "5) ORA_SQL_ELAPSE 대상 SQL(3b8uva7q2cf5a) ELAPSE 수집 확인\n"
        "6) ORA_SQL_STAT_10MIN 대상 SQL(3b8uva7q2cf5a) ELAPSED_TIME/EXECUTION_COUNT 수집 확인"
    ),
    "consumes": ["application/json"],
    "parameters": [
        {
            "in": "body",
            "name": "body",
            "required": False,
            "schema": {
                "type": "object",
                "properties": {
                    "db_id": {"type": "integer", "description": "선택된 Target DB ID (옵션)"},
                    "recent_minutes": {"type": "integer", "default": 30, "description": "최근 조회 구간(분)"},
                    "partition_date": {
                        "type": "string",
                        "description": "파티션 기준일(YYMMDD 또는 YYYYMMDD). 미입력 시 오늘.",
                        "example": "260330",
                    },
                    "sql_id": {
                        "type": "string",
                        "description": "검증 대상 SQL_ID",
                        "default": "3b8uva7q2cf5a",
                    },
                },
            },
        }
    ],
    "responses": {
        200: {"description": "Repo 점검 결과"},
        500: {"description": "서버 오류"},
    },
}

_RUN_TARGET_SQL_SPEC = {
    "tags": ["RTS Check"],
    "summary": "Target DB SQL 미니테스트 실행",
    "description": (
        "Target DB(db_id 또는 직접 입력 접속정보)로 다음을 실행한다.\n"
        "1) ALTER SYSTEM 권한 확인\n"
        "2) DBMS_UTILITY 권한 확인\n"
        "3) QS_SQL_MINITEST_PROC 생성 및 반복 실행\n"
        "4) 7번: v$sql 결과 조회(SQL_ID=3b8uva7q2cf5a)"
    ),
    "consumes": ["application/json"],
    "parameters": [
        {
            "in": "body",
            "name": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "db_id": {"type": "integer", "description": "Target DB ID(APM_DB_INFO.DB_ID)"},
                    "target_config": {
                        "type": "object",
                        "description": "직접 입력 Target DB 접속정보",
                        "properties": {
                            "host": {"type": "string"},
                            "port": {"type": "integer", "default": 1521},
                            "user": {"type": "string"},
                            "password": {"type": "string"},
                            "sid": {"type": "string"},
                            "instance_name": {"type": "string"},
                        },
                    },
                },
            },
        }
    ],
    "responses": {
        200: {"description": "Target SQL 테스트 결과"},
        400: {"description": "파라미터 누락"},
        500: {"description": "서버 오류"},
    },
}


@bp.route("/set-repo", methods=["POST"])
@swag_from({
    "tags": ["RTS Check"],
    "summary": "MongoDB 설정을 Repo DB로 적용",
    "description": (
        "MongoDB에 저장된 DB 접속 정보(config_id)를 선택하여 Repo DB로 설정한다.\n"
        "설정 완료 후 /db-list로 APM_DB_INFO를 조회할 수 있다."
    ),
    "consumes": ["application/json"],
    "parameters": [{
        "in": "body", "name": "body", "required": True,
        "schema": {
            "type": "object",
            "required": ["config_id"],
            "properties": {
                "config_id": {
                    "type": "string",
                    "description": "MongoDB에 저장된 DB 설정 ID (UUID 또는 entry ID)",
                    "example": "a1b2c3d4-...",
                },
            },
        },
    }],
    "responses": {
        200: {"description": "Repo DB 설정 성공"},
        400: {"description": "config_id 누락 또는 설정을 찾을 수 없음"},
        500: {"description": "연결 테스트 실패"},
    },
})
def set_repo_from_mongodb():
    """MongoDB config → Repo DB 설정"""
    from app.services.db_config_service import DBConfigService
    from app.shared_db import set_db_config, get_connection, release_connection

    data = request.get_json(silent=True) or {}
    config_id = data.get("config_id")
    if not config_id:
        return jsonify({"error": "config_id is required"}), 400

    service = DBConfigService()
    if not service.is_connected():
        return jsonify({"error": "MongoDB 연결 실패"}), 500

    collection = service.db_configs_collection
    doc = collection.find_one({"id": config_id})
    if not doc:
        return jsonify({"error": f"config_id '{config_id}' not found"}), 400

    repo_cfg = {
        "host": doc.get("host", ""),
        "port": int(doc.get("db_port", doc.get("port", 1521))),
        "user": doc.get("db_user", doc.get("user", "")),
        "password": doc.get("db_password", doc.get("password", "")),
        "database": doc.get("database", doc.get("service", "")),
        "service": doc.get("service", doc.get("database", "")),
        "service_type": doc.get("service_type", ""),
        "db_type": doc.get("db_type", "oracle"),
    }

    set_db_config("repo", repo_cfg)

    try:
        conn = get_connection("repo")
        if conn is None:
            raise ValueError("연결 객체가 None입니다")
        release_connection("repo", conn)
    except Exception as e:
        set_db_config("repo", None)
        return jsonify({"success": False, "error": f"Repo DB 연결 실패: {e}"}), 500

    return jsonify({
        "success": True,
        "message": f"Repo DB 설정 완료 ({doc.get('name', '')} / {repo_cfg['host']}:{repo_cfg['port']})",
        "repo": {
            "host": repo_cfg["host"],
            "port": repo_cfg["port"],
            "database": repo_cfg["database"],
            "user": repo_cfg["user"],
            "db_type": repo_cfg["db_type"],
        },
    }), 200


@bp.route("/set-repo-direct", methods=["POST"])
@swag_from({
    "tags": ["RTS Check"],
    "summary": "Repo DB 직접 입력 설정",
    "description": "host/port/user/password/database를 직접 입력하여 Repo DB를 설정한다.",
    "consumes": ["application/json"],
    "parameters": [{
        "in": "body", "name": "body", "required": True,
        "schema": {
            "type": "object",
            "required": ["host", "port", "user", "password", "database"],
            "properties": {
                "host": {"type": "string", "example": "192.168.0.100"},
                "port": {"type": "integer", "example": 5432},
                "user": {"type": "string", "example": "maxgauge"},
                "password": {"type": "string"},
                "database": {"type": "string", "example": "maxgauge"},
                "db_type": {"type": "string", "enum": ["postgresql", "oracle"], "default": "postgresql"},
            },
        },
    }],
    "responses": {
        200: {"description": "설정 완료"},
        400: {"description": "필수값 누락 또는 연결 실패"},
    },
})
def set_repo_direct():
    """Repo DB 직접 설정"""
    from app.shared_db import set_db_config, get_connection, release_connection

    data = request.get_json(silent=True) or {}
    required = ["host", "port", "user", "password", "database"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    set_db_config("repo", data)

    try:
        conn = get_connection("repo")
        release_connection("repo", conn)
        return jsonify({"success": True, "message": "Repo DB 연결 성공"}), 200
    except Exception as e:
        set_db_config("repo", None)
        return jsonify({"success": False, "error": f"연결 테스트 실패: {e}"}), 400


@bp.route("/repo-status", methods=["GET"])
@swag_from({
    "tags": ["RTS Check"],
    "summary": "현재 Repo DB 연결 상태",
    "responses": {200: {"description": "Repo DB 상태"}},
})
def get_repo_status():
    """현재 Repo DB 설정 상태 반환"""
    from app.shared_db import get_db_config

    config = get_db_config("repo")
    if config:
        return jsonify({
            "connected": True,
            "host": config.get("host", ""),
            "port": config.get("port", ""),
            "database": config.get("database", config.get("service", "")),
            "user": config.get("user", config.get("db_user", "")),
            "db_type": config.get("db_type", ""),
        }), 200
    return jsonify({"connected": False}), 200


@bp.route("/db-list", methods=["GET"])
@swag_from({
    "tags": ["RTS Check"],
    "summary": "APM_DB_INFO 목록 조회",
    "description": "Repo DB에서 APM_DB_INFO 테이블 전체를 조회한다. db_password는 마스킹.",
    "responses": {
        200: {"description": "DB 목록"},
        500: {"description": "조회 실패"},
    },
})
def get_db_list():
    """APM_DB_INFO 전체 목록"""
    from app.shared_db import get_connection, release_connection, get_db_config, _infer_db_engine

    config = get_db_config("repo")
    if not config:
        return jsonify({"error": "Repo DB not configured"}), 500

    engine = _infer_db_engine(config, "postgresql")
    conn = None
    try:
        conn = get_connection("repo")
        cursor = conn.cursor()
        cursor.execute(
            "SELECT db_id, instance_name, host_ip, host_name, host_id, "
            "db_user, sid, lsnr_ip, lsnr_port, os_type, oracle_version "
            "FROM apm_db_info ORDER BY db_id ASC"
        )
        columns = ["db_id", "instance_name", "host_ip", "host_name", "host_id",
                    "db_user", "sid", "lsnr_ip", "lsnr_port", "os_type", "oracle_version"]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return jsonify({"data": rows, "count": len(rows)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            release_connection("repo", conn)


@bp.route("/run", methods=["POST"])
@swag_from(_RUN_SPEC)
def run_check():
    """단일 호스트 RTS 점검"""
    from app.services.rts_check_service import run_rts_check

    data = request.get_json(silent=True) or {}

    db_id = data.get("db_id")
    ssh_user = data.get("ssh_user")
    ssh_password = data.get("ssh_password")

    if not all([db_id, ssh_user, ssh_password]):
        return jsonify({"error": "db_id, ssh_user, ssh_password are required"}), 400

    try:
        result = run_rts_check(
            db_id=int(db_id),
            ssh_user=ssh_user,
            ssh_password=ssh_password,
            ssh_port=int(data.get("ssh_port", 22)),
            conf_name=data.get("conf_name"),
            base_dir=data.get("base_dir"),
            host_override=data.get("host_override"),
            on_failure=data.get("on_failure", "run_all"),
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/run-multi", methods=["POST"])
@swag_from(_RUN_MULTI_SPEC)
def run_check_multi():
    """다중 호스트 RTS 점검"""
    from app.services.rts_check_service import run_rts_check_multi

    data = request.get_json(silent=True) or {}
    targets = data.get("targets")

    if not targets or not isinstance(targets, list):
        return jsonify({"error": "targets array is required"}), 400

    for i, t in enumerate(targets):
        if not all(t.get(k) for k in ("db_id", "ssh_user", "ssh_password")):
            return jsonify({"error": f"targets[{i}]: db_id, ssh_user, ssh_password are required"}), 400

    try:
        results = run_rts_check_multi(targets)
        fail_count = sum(1 for r in results if r.get("overall_status") != "pass")
        return jsonify({
            "results": results,
            "total_count": len(results),
            "fail_count": fail_count,
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/run-repo", methods=["POST"])
@swag_from(_RUN_REPO_SPEC)
def run_repo_check_api():
    """Repo DB 로깅 점검"""
    from app.services.repo_check_service import run_repo_check

    data = request.get_json(silent=True) or {}
    db_id = data.get("db_id")
    recent_minutes = int(data.get("recent_minutes", 30))
    partition_date = data.get("partition_date")
    sql_id = str(data.get("sql_id", "3b8uva7q2cf5a") or "3b8uva7q2cf5a").strip()

    try:
        result = run_repo_check(
            db_id=db_id,
            recent_minutes=recent_minutes,
            partition_date=partition_date,
            sql_id=sql_id,
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/run-target-sql", methods=["POST"])
@swag_from(_RUN_TARGET_SQL_SPEC)
def run_target_sql_api():
    """Target DB SQL 미니테스트 실행"""
    from app.services.target_sql_test_service import run_target_sql_test

    data = request.get_json(silent=True) or {}
    db_id = data.get("db_id")
    target_cfg = data.get("target_config")
    if not db_id and not target_cfg:
        return jsonify({"error": "db_id or target_config is required"}), 400

    try:
        result = run_target_sql_test(int(db_id) if db_id else None, target_config=target_cfg)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/cpu-mem/snapshot", methods=["POST"])
@swag_from({
    "tags": ["RTS Check"],
    "summary": "CPU/MEM 현재값 조회",
    "description": "선택한 Target DB의 호스트에 SSH 접속하여 rts/sndf/obsd CPU/MEM 현재값을 조회한다.",
})
def cpu_mem_snapshot_api():
    from app.services.cpu_mem_metrics_service import collect_cpu_mem_snapshot

    data = request.get_json(silent=True) or {}
    required = ["db_id", "ssh_user", "ssh_password"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    try:
        result = collect_cpu_mem_snapshot(
            db_id=int(data.get("db_id")),
            ssh_user=data.get("ssh_user"),
            ssh_password=data.get("ssh_password"),
            ssh_port=int(data.get("ssh_port", 22)),
            conf_name=data.get("conf_name"),
            base_dir=data.get("base_dir"),
            host_override=data.get("host_override"),
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/cpu-mem/window", methods=["POST"])
@swag_from({
    "tags": ["RTS Check"],
    "summary": "CPU/MEM 2시간 집계 조회",
    "description": "in-memory 샘플 기준 CPU/MEM 2시간(기본) 집계를 조회한다.",
})
def cpu_mem_window_api():
    from app.services.cpu_mem_metrics_service import collect_cpu_mem_window

    data = request.get_json(silent=True) or {}
    required = ["db_id", "ssh_user", "ssh_password"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    try:
        result = collect_cpu_mem_window(
            window_minutes=int(data.get("window_minutes", 120)),
            db_id=int(data.get("db_id")),
            ssh_user=data.get("ssh_user"),
            ssh_password=data.get("ssh_password"),
            ssh_port=int(data.get("ssh_port", 22)),
            conf_name=data.get("conf_name"),
            base_dir=data.get("base_dir"),
            host_override=data.get("host_override"),
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

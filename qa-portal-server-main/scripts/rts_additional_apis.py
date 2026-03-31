@bp.route("/db/setup", methods=["GET"])
@swag_from({
    'tags': ['DB연결 설정'],
    'summary': '🔧 Target & Repo DB 연결 정보 설정',
    'description': '두 DB의 연결 정보를 한번에 설정하여 다른 API에서 재사용할 수 있도록 합니다.',
    'parameters': [
        {"name": "target_host", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🎯 Target DB Host"},
        {"name": "target_port", "in": "query", "required": True, "schema": {"type": "integer"}, "description": "🎯 Target DB Port"},
        {"name": "target_service", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🎯 Target DB Service Name"},
        {"name": "target_user", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🎯 Target DB User"},
        {"name": "target_password", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🎯 Target DB Password"},
        {"name": "repo_host", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🗄️ Repo DB Host"},
        {"name": "repo_port", "in": "query", "required": True, "schema": {"type": "integer"}, "description": "🗄️ Repo DB Port"},
        {"name": "repo_service", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🗄️ Repo DB Service Name"},
        {"name": "repo_user", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🗄️ Repo DB User"},
        {"name": "repo_password", "in": "query", "required": True, "schema": {"type": "string"}, "description": "🗄️ Repo DB Password"}
    ],
    'responses': {
        200: {'description': '✅ DB 연결 정보 설정 완료'},
        400: {'description': '❌ 필수 파라미터 누락'}
    }
})
def setup_db_connections():
    """Target & Repo DB 연결 정보를 설정하고 연결 테스트"""
    from app.shared_db import set_target_db_config, set_repo_db_config
    
    # Target DB 설정
    target_config = {
        'host': request.args.get("target_host"),
        'port': request.args.get("target_port", type=int),
        'service': request.args.get("target_service"),
        'user': request.args.get("target_user"),
        'password': request.args.get("target_password")
    }
    
    # Repo DB 설정
    repo_config = {
        'host': request.args.get("repo_host"),
        'port': request.args.get("repo_port", type=int),
        'service': request.args.get("repo_service"),
        'user': request.args.get("repo_user"),
        'password': request.args.get("repo_password")
    }
    
    # 필수 파라미터 검증
    for key, value in target_config.items():
        if value is None:
            return jsonify({"error": f"target_{key} 파라미터가 필요합니다"}), 400
    
    for key, value in repo_config.items():
        if value is None:
            return jsonify({"error": f"repo_{key} 파라미터가 필요합니다"}), 400
    
    result = {"status": "success", "target_test": {}, "repo_test": {}}
    
    # Target DB 연결 테스트
    try:
        with _connect_target(target_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SYSDATE FROM DUAL")
                current_time = cur.fetchone()[0]
                result["target_test"]["status"] = "success"
                result["target_test"]["current_time"] = str(current_time)
                result["target_test"]["connection_info"] = f"{target_config['host']}:{target_config['port']}/{target_config['service']}"
        
        # 연결 성공 시 설정 저장
        set_target_db_config(target_config)
        
    except Exception as e:
        result["target_test"]["status"] = "error"
        result["target_test"]["message"] = str(e)
    
    # Repo DB 연결 테스트  
    try:
        with _connect_repo(repo_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SYSDATE FROM DUAL")
                current_time = cur.fetchone()[0]
                result["repo_test"]["status"] = "success"
                result["repo_test"]["current_time"] = str(current_time)
                result["repo_test"]["connection_info"] = f"{repo_config['host']}:{repo_config['port']}/{repo_config['service']}"
        
        # 연결 성공 시 설정 저장
        set_repo_db_config(repo_config)
        
    except Exception as e:
        result["repo_test"]["status"] = "error"
        result["repo_test"]["message"] = str(e)
    
    return jsonify(result)


@bp.route("/db/status", methods=["GET"])
@swag_from({
    'tags': ['DB연결 설정'],
    'summary': '📊 DB 연결 상태 확인',
    'description': '현재 설정된 DB 연결 정보 상태를 확인합니다.',
    'responses': {
        200: {'description': '✅ 상태 조회 완료'}
    }
})
def get_db_status():
    """현재 설정된 DB 연결 상태 확인"""
    from app.shared_db import get_all_db_status
    return jsonify(get_all_db_status())

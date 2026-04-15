# app/shared_db.py
import oracledb
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from app.services.db_config_service import db_config_service

_connection_pools = {"target": None, "repo": None} 
_db_connections = {"target": None, "repo": None}
_config_file = "db_config.json"  # 백업용으로 유지
_legacy_setup_file = "db_setup.json"
_default_profile_id = None  # 현재 사용 중인 프로필 ID
_logger = logging.getLogger(__name__)

def _load_config_from_mongodb():
    """MongoDB에서 기본 연결 프로필 로드"""
    global _db_connections, _default_profile_id
    
    try:
        if not db_config_service.is_connected():
            _logger.warning("MongoDB 연결이 없어 기본 JSON 파일을 사용합니다.")
            return _load_config_from_json()
        
        # 기본 연결 프로필 조회
        profile = db_config_service.get_default_connection_profile()
        
        if profile and profile.get('repo_config'):
            repo_config = profile['repo_config']
            
            # Repo DB 설정만 로드 (Target DB는 APM_DB_INFO에서 선택)
            _db_connections['repo'] = {
                'host': repo_config.get('host', ''),
                'port': repo_config.get('port', 5432),
                'user': repo_config.get('user', ''),
                'password': repo_config.get('password', ''),
                'service': repo_config.get('service', repo_config.get('database', '')),
                'database': repo_config.get('database', repo_config.get('service', '')),
                'db_id': repo_config.get('db_id', 1),
                'db_type': repo_config.get('db_type', repo_config.get('type', 'postgresql')).lower()
            }
            
            # Target DB는 설정하지 않음 (APM_DB_INFO에서 선택해야 함)
            _db_connections['target'] = None
            
            _default_profile_id = profile['profile_id']
            _logger.info(f"MongoDB에서 DB 설정 로드 완료: {_default_profile_id}")
            
        else:
            _logger.warning("MongoDB에서 기본 연결 프로필을 찾을 수 없습니다. 로컬 설정 파일로 fallback 합니다.")
            _load_config_from_json()
            
    except Exception as e:
        _logger.error(f"MongoDB DB 설정 로드 실패: {e}")
        # MongoDB 실패시 JSON 파일로 fallback
        _load_config_from_json()

def _load_config_from_json():
    """JSON 파일에서 DB 연결 정보 로드 (백업용)"""
    global _db_connections
    try:
        # 1) 표준 파일 우선 로드: {"target": {...}, "repo": {...}}
        if os.path.exists(_config_file):
            with open(_config_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                target_cfg = loaded.get('target')
                repo_cfg = loaded.get('repo')
                _db_connections = {"target": target_cfg, "repo": repo_cfg}
                _logger.info("db_config.json에서 DB 설정 로드 완료")
                return

        # 2) 레거시 파일 fallback: {"target_db": {...}, "repo_db": {...}}
        if os.path.exists(_legacy_setup_file):
            with open(_legacy_setup_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                _db_connections = {
                    "target": loaded.get('target_db'),
                    "repo": loaded.get('repo_db')
                }
                _logger.info("db_setup.json에서 DB 설정 로드 완료")
                return

        _logger.warning("로컬 DB 설정 파일(db_config.json / db_setup.json)을 찾지 못했습니다.")
        _db_connections = {"target": None, "repo": None}
    except Exception as e:
        _logger.error(f"JSON DB 설정 로드 실패: {e}")
        _db_connections = {"target": None, "repo": None}

def _save_config():
    """설정을 JSON 파일에 저장 (백업용)"""
    try:
        with open(_config_file, 'w', encoding='utf-8') as f:
            json.dump(_db_connections, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _logger.error(f"DB 설정 저장 실패: {e}")

# 모듈 로드 시 MongoDB에서 기본 설정 불러오기
_load_config_from_mongodb()

def _infer_db_engine(cfg: Dict[str, Any], default: str) -> str:
    explicit = cfg.get('db_type') or cfg.get('type')
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()

    port = cfg.get('db_port', cfg.get('port'))
    try:
        port = int(port) if port is not None else None
    except (TypeError, ValueError):
        port = None

    oracle_hint = any(
        bool((cfg.get(key) or '').strip())
        for key in ('service', 'service_name', 'serviceName', 'sid', 'schema_name')
    )
    if oracle_hint or (port == 1521):
        return 'oracle'
    return default


def _normalize_service_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized == 'sid':
        return 'sid'
    if normalized in ('service', 'service_name'):
        return 'service_name'
    return None


def _connect_postgres_db(config: Dict[str, Any]):
    """
    connect_repo_by_config_id 전용: 풀이 아닌 직접(direct) 연결을 반환한다.
    호출자가 conn.close()로 반납하면 되므로 풀 고갈 문제가 없다.
    """
    import psycopg2
    from app.services.dg_password_service import decrypt_dg_password
    host     = config.get('host', 'localhost')
    port     = int(config.get('port', config.get('db_port', 5432)) or 5432)
    database = config.get('database') or config.get('service') or ''
    user     = config.get('user') or config.get('db_user') or ''
    raw_pw   = config.get('password') or config.get('db_password') or ''
    password = decrypt_dg_password(raw_pw)
    try:
        return psycopg2.connect(
            host=host, port=port, database=database,
            user=user, password=password, sslmode='disable',
        )
    except Exception as e:
        raise ValueError(
            f"PostgreSQL 연결 실패 ({user}@{host}:{port}/{database or '(database 미지정)'}): {e}"
        ) from e


def _build_oracle_dsn(config: Dict[str, Any]) -> str:
    host = config.get('host', 'localhost')
    port = int(config.get('port', config.get('db_port', 1521)) or 1521)
    service_type = _normalize_service_type(config.get('service_type') or config.get('serviceType'))
    service_value = (
        config.get('service')
        or config.get('service_name')
        or config.get('serviceName')
        or config.get('database')
        or config.get('sid')
        or 'orcl'
    )

    if service_type == 'sid':
        return oracledb.makedsn(host, port, sid=service_value)

    # 기본은 service_name 사용, 실패 시 sid로 재시도
    try:
        return oracledb.makedsn(host, port, service_name=service_value)
    except Exception:
        return oracledb.makedsn(host, port, sid=service_value)


def _connect_oracle_db(config: Dict[str, Any]):
    from app.services.oracle_service import OracleService

    try:
        return OracleService(config).connect_or_raise()
    except Exception as e:
        raise ValueError(f"Oracle 연결 실패: {e}") from e


def set_db_config(db_type, config):
    """DB 설정 (레거시 호환용, JSON 파일에만 저장)"""
    global _db_connections
    
    if config is None:
        _db_connections[db_type] = None
        _save_config()
        return True
    
    normalized = dict(config)
    default_engine = 'postgresql' if db_type == 'repo' else 'oracle'
    engine = _infer_db_engine(normalized, default_engine)
    normalized['db_type'] = engine
    
    if db_type == 'repo':
        normalized['host'] = str(normalized.get('host', '')).strip()
        normalized['port'] = int(normalized.get('db_port', normalized.get('port', 5432)) or 5432)
        normalized['user'] = (normalized.get('db_user') or normalized.get('user') or '').strip()
        normalized['password'] = normalized.get('db_password', normalized.get('password', ''))
        normalized['database'] = normalized.get('database', normalized.get('service', '')).strip()
    else:
        normalized['host'] = str(normalized.get('host', '')).strip()
        normalized['port'] = int(normalized.get('port', normalized.get('db_port', 1521)) or 1521)
        normalized['user'] = (normalized.get('user') or '').strip()
        normalized['password'] = normalized.get('password', '')
        normalized['database'] = normalized.get('database', normalized.get('service', normalized.get('sid', ''))).strip()
        normalized['db_type'] = 'oracle'
    
    service_value = (
        normalized.get('service')
        or normalized.get('service_name')
        or normalized.get('serviceName')
        or normalized.get('database')
        or normalized.get('sid')
    )
    if service_value:
        normalized['service'] = service_value
    else:
        normalized.pop('service', None)
    
    service_type = _normalize_service_type(
        normalized.get('service_type') or normalized.get('serviceType')
    )
    if service_type:
        normalized['service_type'] = service_type
    else:
        normalized.pop('service_type', None)
    
    schema_val = normalized.get('schema_name')
    if schema_val is not None and not str(schema_val).strip():
        normalized.pop('schema_name', None)
    
    _db_connections[db_type] = normalized
    _save_config()
    return True
def set_connection_profile(profile_id: str) -> bool:
    """연결 프로필 설정"""
    global _db_connections, _default_profile_id
    
    try:
        if not db_config_service.is_connected():
            _logger.error("MongoDB 연결이 필요합니다.")
            return False
        
        profile = db_config_service.get_connection_profile(profile_id)
        
        if not profile or not profile.get('repo_config'):
            _logger.error(f"연결 프로필을 찾을 수 없습니다: {profile_id}")
            return False
        
        repo_config = profile['repo_config']

        set_db_config('repo', repo_config)
        _default_profile_id = profile_id
        
        # JSON 파일에도 백업 저장
        _save_config()
        
        _logger.info(f"연결 프로필 설정 완료: {profile_id}")
        return True
        
    except Exception as e:
        _logger.error(f"연결 프로필 설정 실패: {e}")
        return False

def get_current_profile_id() -> Optional[str]:
    """현재 사용 중인 프로필 ID 반환"""
    return _default_profile_id

def get_db_config(db_type):
    return _db_connections.get(db_type)

def record_connection_success(db_type):
    return True

def record_connection_error(db_type, error):
    return True

def get_db_status(db_type):
    return {"status": "success"}

def create_connection_pool(db_type, min_size=1, max_size=5):
    return True

def get_connection(db_type): # get_db_confing -> 내부적으로 _db_connections[]로 반환 / 메모리 안에 있는 _db_connections dict 설정을 읽음
    config = get_db_config(db_type) 
    if not config:
        raise ValueError(f"DB configuration not found for {db_type}")
    
    if db_type == 'repo':
        engine = _infer_db_engine(config, 'postgresql')
        if engine == 'oracle':
            return _connect_oracle_db(config)
        return _connect_postgres_db(config)
    
    # Target DB는 항상 Oracle
    return _connect_oracle_db(config)

def release_connection(db_type, connection):
    if not connection:
        return
    config = _db_connections.get(db_type)
    if config:
        engine = _infer_db_engine(config, 'postgresql' if db_type == 'repo' else 'oracle')
        if engine == 'postgresql':
            from app.services.postgresql_service import PostgreSQLService
            normalized = {
                'host': config.get('host', 'localhost'),
                'port': int(config.get('port', config.get('db_port', 5432)) or 5432),
                'database': config.get('database') or config.get('service') or '',
                'user': config.get('user') or config.get('db_user') or '',
                'password': config.get('password') or config.get('db_password') or '',
            }
            PostgreSQLService(normalized).release_connection(connection)
            return
        if engine == 'oracle':
            from app.services.oracle_service import OracleService
            OracleService(config).release_connection(connection)
            return
    try:
        connection.close()
    except Exception:
        pass

def close_all_pools():
    pass

def get_target_db_config():
    """Target DB 연결 정보 반환"""
    return _db_connections.get('target')

def get_repo_db_config():
    """Repo DB 연결 정보 반환"""
    return _db_connections.get('repo')

def is_target_db_configured():
    """Target DB가 설정되어 있는지 확인"""
    return _db_connections.get('target') is not None

def is_repo_db_configured():
    """Repo DB가 설정되어 있는지 확인"""
    return _db_connections.get('repo') is not None

# MongoDB 관련 함수들
def get_all_db_profiles():
    """MongoDB에서 모든 DB 프로파일 목록 조회"""
    try:
        return db_config_service.get_all_profiles()
    except Exception as e:
        _logger.error(f"MongoDB에서 프로파일 목록 조회 실패: {e}")
        return []

def create_db_profile(profile_name, target_config, repo_config):
    """새로운 DB 프로파일 생성 (Repo DB만, target_config는 무시)"""
    try:
        # MongoDB entries에 Repo DB 저장 (모든 필드 포함)
        # 이미 저장되어 있다면 config_id를 받아옴
        config_id = db_config_service.create_db_entry(repo_config)

        if not config_id:
            _logger.error("Repo DB 접속 정보 저장 실패")
            return False

        # 프로필 생성 (config_id를 repo_config_id로 사용)
        # MongoDB entries 구조에서는 config_id가 "entry_X" 형식이므로
        # 이를 repo_config_id로 사용
        profile_data = {
            'profile_name': profile_name,
            'repo_config_id': config_id,  # MongoDB entries의 config_id 사용
            'description': repo_config.get('description', '')
        }

        return db_config_service.save_connection_profile(profile_data)

    except Exception as e:
        _logger.error(f"DB 프로파일 생성 실패: {e}")
        return False

def delete_db_profile(profile_name):
    """DB 프로파일 삭제"""
    try:
        return db_config_service.delete_profile(profile_name)
    except Exception as e:
        _logger.error(f"DB 프로파일 삭제 실패: {e}")
        return False

def connect_repo_by_config_id(config_id: str):
    """
    MongoDB id(UUID)로 특정 Repo DB에 직접 연결을 반환한다.
    /api/v1/db_list가 반환하는 id 필드(UUID)를 사용해 db_configs_collection에서 직접 조회한다.
    active 프로필을 변경하지 않는다. (conn, normalized_cfg) 튜플 반환.
    """
    try:
        # /api/v1/db_list 와 동일하게 db_configs_collection에서 id(UUID)로 직접 조회
        collection = db_config_service.db_configs_collection
        if collection is None:
            raise ValueError("MongoDB에 연결되어 있지 않습니다.")
        target = collection.find_one({"id": config_id})
        if target:
            target.pop('_id', None)  # ObjectId 제거
        if not target:
            raise ValueError(f"config_id not found: {config_id}")
        normalized = {
            'host': target.get('host', ''),
            'port': int(target.get('port', target.get('db_port', 5432)) or 5432),
            'user': (
                target.get('username') or target.get('db_user') or target.get('user') or ''
            ),
            'password': target.get('password') or target.get('db_password') or '',
            'database': (
                target.get('database') or target.get('service_name') or target.get('service') or ''
            ),
            'schema_name': target.get('schema_name', ''),
            'db_type': target.get('db_type', 'postgresql').lower(),
        }
        engine = _infer_db_engine(normalized, 'postgresql')
        if engine == 'oracle':
            return _connect_oracle_db(normalized), normalized
        return _connect_postgres_db(normalized), normalized
    except Exception as e:
        raise ValueError(f"POL Repo 연결 실패 ({config_id}): {e}") from e


def get_mongodb_status():
    """MongoDB 연결 상태 확인"""
    try:
        return db_config_service.test_connection()
    except Exception as e:
        _logger.error(f"MongoDB 상태 확인 실패: {e}")
        return False

def save_target_config_to_mongodb(config):
    """Target DB 설정을 MongoDB에 저장"""
    try:
        return db_config_service.save_target_config(config)
    except Exception as e:
        _logger.error(f"Target DB 설정 MongoDB 저장 실패: {e}")
        return False

def save_repo_config_to_mongodb(config):
    """Repo DB 설정을 MongoDB에 저장"""
    try:
        return db_config_service.save_repo_config(config)
    except Exception as e:
        _logger.error(f"Repo DB 설정 MongoDB 저장 실패: {e}")
        return False

def get_target_config_from_mongodb():
    """MongoDB에서 Target DB 설정 조회"""
    try:
        return db_config_service.get_target_config()
    except Exception as e:
        _logger.error(f"MongoDB에서 Target DB 설정 조회 실패: {e}")
        return None

def get_repo_config_from_mongodb():
    """MongoDB에서 Repo DB 설정 조회"""
    try:
        return db_config_service.get_repo_config()
    except Exception as e:
        _logger.error(f"MongoDB에서 Repo DB 설정 조회 실패: {e}")
        return None

def reload_config_from_file():
    """JSON 파일에서 DB 설정 다시 로드 (기존 호환성용)"""
    global _db_connections
    try:
        import json
        import os
        
        # db_config.json 파일 로드 시도
        config_file = 'db_config.json'
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            _db_connections = config
            _logger.info(f"JSON 파일에서 DB 설정 다시 로드됨: {config_file}")
            return True
        else:
            _logger.warning(f"설정 파일을 찾을 수 없습니다: {config_file}")
            return False
            
    except Exception as e:
        _logger.error(f"JSON 파일 설정 다시 로드 실패: {e}")
        return False

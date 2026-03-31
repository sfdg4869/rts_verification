"""
MongoDB 기반 DB 설정 모델
Target DB와 Repo DB 연결 정보를 MongoDB에 저장하고 관리
"""

from datetime import datetime
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# .env 파일 로드
load_dotenv()

class DBConfigModel:
    """DB 설정 데이터 모델"""
    
    def __init__(self):
        self.collection_name = "db_configs"
    
    @staticmethod
    def create_target_config(
        name: str,
        host: str,
        port: int,
        user: str,
        password: str,
        service: str,
        description: str = ""
    ) -> Dict[str, Any]:
        """Target DB 설정 생성"""
        return {
            "config_id": f"target_{name}",
            "config_type": "target",
            "name": name,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "service": service,
            "description": description,
            "is_active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    @staticmethod
    def create_repo_config(
        name: str,
        host: str,
        port: int,
        user: str,
        password: str,
        service: str,
        db_id: int,
        description: str = ""
    ) -> Dict[str, Any]:
        """Repo DB 설정 생성"""
        return {
            "config_id": f"repo_{name}",
            "config_type": "repo", 
            "name": name,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "service": service,
            "db_id": db_id,
            "description": description,
            "is_active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    @staticmethod
    def create_connection_profile(
        profile_name: str,
        repo_config_id: str,
        description: str = ""
    ) -> Dict[str, Any]:
        """Repo DB 연결 프로필 생성 (Target DB는 APM_DB_INFO에서 선택)"""
        return {
            "profile_id": profile_name,
            "profile_name": profile_name,
            "repo_config_id": repo_config_id,
            "description": description,
            "is_default": False,
            "is_active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    @staticmethod
    def validate_target_config(config: Dict[str, Any]) -> bool:
        """Target DB 설정 유효성 검증"""
        required_fields = ['name', 'host', 'port', 'user', 'password', 'service']
        return all(field in config and config[field] for field in required_fields)
    
    @staticmethod
    def validate_repo_config(config: Dict[str, Any]) -> bool:
        """Repo DB 설정 유효성 검증"""
        required_fields = ['name', 'host', 'port', 'user', 'password', 'service', 'db_id']
        return all(field in config and config[field] is not None for field in required_fields)

def _build_mongodb_uri():
    """환경변수에서 MongoDB URI 구성"""
    host = os.getenv('MONGODB_HOST', 'localhost')
    port = os.getenv('MONGODB_PORT', '27017')
    username = os.getenv('MONGODB_USERNAME')
    password = os.getenv('MONGODB_PASSWORD')
    auth_source = os.getenv('MONGODB_AUTH_SOURCE', 'admin')
    
    # URI 직접 제공 시 우선 사용
    uri = os.getenv('MONGODB_URI')

    if uri:
        return uri
    
    # 개별 필드로 URI 구성
    if username and password:
        # RFC 3986에 따라 username과 password를 URL 인코딩
        encoded_username = quote_plus(username)
        encoded_password = quote_plus(password)
        return f'mongodb://{encoded_username}:{encoded_password}@{host}:{port}/{auth_source}?authSource={auth_source}'
    else:
        return f'mongodb://{host}:{port}/'

# MongoDB 연결 설정
MONGODB_CONFIG = {
    'uri': _build_mongodb_uri(),
    'database': os.getenv('MONGODB_DATABASE', 'repo_test'),
    'collections': {
        'db_configs': os.getenv('MONGODB_DB_CONFIGS_COLLECTION', 'test'),
        'connection_profiles': os.getenv('MONGODB_CONNECTION_PROFILES_COLLECTION', 'connection_profiles')
    }
}
"""
MongoDB 설정 라우트 모듈

Repo DB 접속 정보를 MongoDB에 저장/조회/수정/삭제하는 API 엔드포인트를 제공합니다.
"""
import logging
from typing import Any, Dict, List, Tuple

from flask import Blueprint, jsonify, request
from flasgger import swag_from

from app.services.db_config_service import DBConfigService

logger = logging.getLogger(__name__)

# MongoDB 설정 관련 라우트를 그룹화하는 Blueprint 생성
mongodb_config_bp = Blueprint('mongodb_config', __name__, url_prefix='/api/v1')

# 필수 필드 목록
REQUIRED_FIELDS = [
    'name', 'host', 'database', 'db_user', 'db_password', 
    'db_port', 'db_type', 'ssh_user', 'ssh_password', 
    'ssh_port', 'os_type', 'dg_home'
]


def _validate_required_fields(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    필수 필드 검증 함수
    
    Args:
        data: 검증할 데이터 딕셔너리
    
    Returns:
        Tuple[bool, str]: (검증 성공 여부, 에러 메시지)
    """
    missing_fields = [field for field in REQUIRED_FIELDS if not data.get(field)]
    
    if missing_fields:
        error_msg = f'필수 필드가 누락되었습니다: {", ".join(missing_fields)}'
        return False, error_msg
    
    return True, ''

def _sanitize_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    DB 설정 정보에서 민감한 정보(비밀번호 등)를 제거하는 함수
    
    Args:
        entry: 원본 DB 설정 정보 딕셔너리
    
    Returns:
        Dict[str, Any]: 비밀번호 필드가 제거된 안전한 설정 정보 딕셔너리
    """
    sanitized = dict(entry)
    
    # 민감한 정보 제거
    sensitive_keys = ('password', 'db_password', 'ssh_password')
    for key in sensitive_keys:
        sanitized.pop(key, None)
    
    return sanitized


def _convert_entry_to_response_format(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    MongoDB entries 배열의 항목을 원본 POST 요청 형식으로 변환하는 함수
    
    Args:
        entry: MongoDB에 저장된 entry 딕셔너리
    
    Returns:
        Dict[str, Any]: 원본 POST 요청 형식의 딕셔너리 (id 필드 포함)
    """
    return {
        'id': entry.get('id', ''),  # ID 필드 추가
        'name': entry.get('name', ''),
        'host': entry.get('host', ''),
        'database': entry.get('database', ''),
        'db_user': entry.get('user', ''),  # user -> db_user
        'db_password': entry.get('password', ''),  # password -> db_password
        'db_port': entry.get('port', 0),  # port -> db_port
        'db_type': entry.get('db_type', ''),
        'ssh_user': entry.get('ssh_user', ''),
        'ssh_password': entry.get('ssh_password', ''),
        'ssh_port': entry.get('ssh_port', 0),
        'os_type': entry.get('os', entry.get('os_type', '')),  # os -> os_type
        'schema_name': entry.get('schema_name', ''),
        'service': entry.get('service', ''),
        'service_type': entry.get('service_type', ''),
        'dg_home': entry.get('dg_home', ''),
        'description': entry.get('description', '')
    }


# ============================================================================
# GET 엔드포인트: 저장된 모든 Repo DB 접속 정보 목록 조회
# ============================================================================

@mongodb_config_bp.route('/db_list', methods=['GET'])
@swag_from({
    'tags': ['MongoDB Configuration'],
    'summary': 'Repo DB 접속 정보 목록 조회',
    'description': 'MongoDB에 저장된 모든 Repo DB 접속 정보를 조회합니다.',
    'responses': {
        200: {
            'description': '목록 조회 성공',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'id': {'type': 'string', 'description': '고유 ID (UUID 형식)'},
                        'name': {'type': 'string', 'description': '설정 이름'},
                        'host': {'type': 'string', 'description': 'DB 호스트 주소'},
                        'database': {'type': 'string', 'description': '데이터베이스 이름'},
                        'db_user': {'type': 'string', 'description': 'DB 사용자 이름'},
                        'db_password': {'type': 'string', 'description': 'DB 비밀번호'},
                        'db_port': {'type': 'integer', 'description': 'DB 포트 번호'},
                        'db_type': {'type': 'string', 'description': 'DB 타입'},
                        'ssh_user': {'type': 'string', 'description': 'SSH 사용자 이름'},
                        'ssh_password': {'type': 'string', 'description': 'SSH 비밀번호'},
                        'ssh_port': {'type': 'integer', 'description': 'SSH 포트 번호'},
                        'os_type': {'type': 'string', 'description': 'OS 타입'},
                        'schema_name': {'type': 'string', 'description': '스키마 이름'},
                        'service': {'type': 'string', 'description': '서비스 이름'},
                        'service_type': {'type': 'string', 'description': '서비스 타입'},
                        'dg_home': {'type': 'string', 'description': 'DG 홈 디렉토리'},
                        'description': {'type': 'string', 'description': '설명'}
                    }
                }
            }
        },
        500: {
            'description': '서버 오류',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {
                        'type': 'string',
                        'example': 'error',
                        'description': '응답 상태 (항상 "error")'
                    },
                    'message': {
                        'type': 'string',
                        'example': 'MongoDB 연결 실패',
                        'description': '에러 메시지 (MongoDB 연결 실패 또는 서버 오류)'
                    }
                },
                'required': ['status', 'message'],
                'examples': {
                    'mongodb_connection_failed': {
                        'summary': 'MongoDB 연결 실패',
                        'value': {
                            'status': 'error',
                            'message': 'MongoDB 연결 실패'
                        }
                    },
                    'server_error': {
                        'summary': '서버 내부 오류',
                        'value': {
                            'status': 'error',
                            'message': '서버 오류가 발생했습니다.'
                        }
                    }
                }
            }
        }
    }
})
def get_rts_mongodb_configs_all():
    """
    저장된 모든 Repo DB 접속 정보 목록을 조회하는 엔드포인트
    
    MongoDB의 entries 배열에서 직접 데이터를 읽어서 리스트로 반환합니다.
    
    Returns:
        JSON 응답:
            - 성공 시 (200): DB 설정 정보 리스트
            - 실패 시 (500): 서버 오류
    """
    try:
        # DB 설정 서비스 인스턴스 생성
        service = DBConfigService()
        
        if not service.is_connected():
            return jsonify({
                'status': 'error',
                'message': 'MongoDB 연결 실패'
            }), 500
        
        # 컬렉션에서 id 필드가 있는 모든 문서 조회
        collection = service.db_configs_collection
        docs = collection.find({"id": {"$exists": True}})
        
        configs = []
        
        for doc in docs:
            # MongoDB의 _id 필드 제거 (ObjectId는 JSON 직렬화 불가)
            if '_id' in doc:
                del doc['_id']
            # MongoDB에 저장된 데이터 그대로 반환 (변환 불필요)
            configs.append(doc)
        
        # 리스트로 직접 반환
        return jsonify(configs)
        
    except Exception as exc:
        logger.error('Failed to get DB configs: %s', str(exc))
        return jsonify({
            'status': 'error',
            'message': '서버 오류가 발생했습니다.'
        }), 500


# ============================================================================
# POST 엔드포인트: Repo DB 접속 정보 생성
# ============================================================================

@mongodb_config_bp.route('/db_list', methods=['POST'])
@swag_from({
    'tags': ['MongoDB Configuration'],
    'summary': 'Repo DB 접속 정보 생성',
    'description': 'Repo DB 접속 정보를 MongoDB에 저장합니다.',
    'parameters': [
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'required': REQUIRED_FIELDS,
                'properties': {
                    'name': {'type': 'string', 'description': '설정 이름'},
                    'host': {'type': 'string', 'description': 'DB 호스트 주소'},
                    'database': {'type': 'string', 'description': '데이터베이스 이름'},
                    'db_user': {'type': 'string', 'description': 'DB 사용자 이름'},
                    'db_password': {'type': 'string', 'description': 'DB 비밀번호'},
                    'db_port': {'type': 'integer', 'description': 'DB 포트 번호'},
                    'db_type': {'type': 'string', 'description': 'DB 타입 (postgresql, oracle 등)'},
                    'ssh_user': {'type': 'string', 'description': 'SSH 사용자 이름'},
                    'ssh_password': {'type': 'string', 'description': 'SSH 비밀번호'},
                    'ssh_port': {'type': 'integer', 'description': 'SSH 포트 번호'},
                    'os_type': {'type': 'string', 'description': 'OS 타입 (Linux, Windows 등)'},
                    'dg_home': {'type': 'string', 'description': 'DG 홈 디렉토리 경로'},
                    'schema_name': {'type': 'string', 'description': '스키마 이름 (선택)'},
                    'service': {'type': 'string', 'description': '서비스 이름 (선택, Oracle의 경우)'},
                    'service_type': {'type': 'string', 'description': '서비스 타입 (선택, sid, service_name)'},
                    'description': {'type': 'string', 'description': '설명 (선택)'}
                }
            }
        }
    ],
    'responses': {
        200: {
            'description': '생성 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'message': {'type': 'string'},
                    'id': {'type': 'string', 'description': '생성된 고유 ID (UUID 형식)'}
                }
            }
        },
        400: {'description': '필수 필드 누락'},
        500: {'description': '서버 오류'}
    }
})
def post_rts_mongodb_configs():
    """
    Repo DB 접속 정보를 생성하는 엔드포인트
    
    요청 본문에서 받은 DB 접속 정보를 MongoDB에 저장합니다.
    
    Returns:
        JSON 응답:
            - 성공 시 (200): 생성된 설정 ID
            - 실패 시 (400): 필수 필드 누락 에러
            - 실패 시 (500): 서버 오류
    """
    try:
        # 요청 데이터 추출
        data = request.get_json() or {}
        
        # 필수 필드 검증
        is_valid, error_msg = _validate_required_fields(data)
        if not is_valid:
            return jsonify({
                'status': 'error',
                'message': error_msg
            }), 400
        
        # DB 설정 서비스 인스턴스 생성
        service = DBConfigService()
        
        # MongoDB에 DB 설정 정보 저장
        entry_id = service.create_db_entry(data)
        if not entry_id:
            return jsonify({
                'status': 'error',
                'message': 'DB 설정 정보 저장에 실패했습니다.'
            }), 500
        
        # 성공 응답 반환
        return jsonify({
            'status': 'success',
            'message': 'Repo DB config has been saved.',
            'id': entry_id  # 생성된 ID 반환
        })
        
    except Exception as exc:
        logger.error('Failed to create DB config: %s', str(exc))
        return jsonify({
            'status': 'error',
            'message': '서버 오류가 발생했습니다.'
        }), 500


# ============================================================================
# PUT 엔드포인트: 특정 Repo DB 접속 정보 수정
# ============================================================================

@mongodb_config_bp.route('/db_list/<string:entry_id>', methods=['PUT'])
@swag_from({
    'tags': ['MongoDB Configuration'],
    'summary': 'Repo DB 접속 정보 수정',
    'description': '특정 Repo DB 접속 정보를 수정합니다. (ID로 식별)',
    'parameters': [
        {
            'name': 'entry_id',
            'in': 'path',
            'required': True,
            'type': 'string',
            'description': '수정할 설정의 고유 ID (UUID 형식)'
        },
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'host': {'type': 'string'},
                    'database': {'type': 'string'},
                    'db_user': {'type': 'string'},
                    'db_password': {'type': 'string'},
                    'db_port': {'type': 'integer'},
                    'db_type': {'type': 'string'},
                    'ssh_user': {'type': 'string'},
                    'ssh_password': {'type': 'string'},
                    'ssh_port': {'type': 'integer'},
                    'os_type': {'type': 'string'},
                    'dg_home': {'type': 'string'},
                    'schema_name': {'type': 'string'},
                    'service': {'type': 'string'},
                    'service_type': {'type': 'string'},
                    'description': {'type': 'string'}
                }
            }
        }
    ],
    'responses': {
        200: {'description': '수정 성공'},
        400: {'description': '잘못된 요청'},
        404: {'description': '대상 없음'},
        500: {'description': '서버 오류'}
    }
})
def put_rts_mongodb_configs_config_id(entry_id: str):
    """
    특정 Repo DB 접속 정보를 수정하는 엔드포인트
    
    Args:
        entry_id: 수정할 DB 설정의 고유 ID (UUID 형식)
    
    Returns:
        JSON 응답:
            - 성공 시 (200): 수정 성공 메시지
            - 실패 시 (400): 요청 본문이 비어있는 경우
            - 실패 시 (404): 해당 entry_id가 존재하지 않는 경우
            - 실패 시 (500): 서버 오류
    """
    try:
        # 요청 데이터 추출
        data = request.get_json() or {}
        
        # 요청 본문 검증
        if not data:
            return jsonify({
                'status': 'error',
                'message': '요청 본문이 비어있습니다.'
            }), 400
        
        # DB 설정 서비스 인스턴스 생성
        service = DBConfigService()
        
        # DB 설정 정보 업데이트
        success = service.update_db_entry(entry_id, data)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': f'{entry_id} updated.'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': f'{entry_id} not found.'
            }), 404
            
    except Exception as exc:
        logger.error('Failed to update DB List %s: %s', entry_id, str(exc))
        return jsonify({
            'status': 'error',
            'message': '서버 오류가 발생했습니다.'
        }), 500


# ============================================================================
# DELETE 엔드포인트: 특정 Repo DB 접속 정보 삭제
# ============================================================================

@mongodb_config_bp.route('/db_list/<string:entry_id>', methods=['DELETE'])
@swag_from({
    'tags': ['MongoDB Configuration'],
    'summary': 'Repo DB 접속 정보 삭제',
    'description': '특정 Repo DB 접속 정보를 삭제합니다. (ID로 식별)',
    'parameters': [
        {
            'name': 'entry_id',
            'in': 'path',
            'required': True,
            'type': 'string',
            'description': '삭제할 설정의 고유 ID (UUID 형식)'
        }
    ],
    'responses': {
        200: {'description': '삭제 성공'},
        404: {'description': '대상 없음'},
        500: {'description': '서버 오류'}
    }
})
def delete_rts_mongodb_configs_config_id(entry_id: str):
    """
    특정 Repo DB 접속 정보를 삭제하는 엔드포인트
    
    Args:
        entry_id: 삭제할 DB 설정의 고유 ID (UUID 형식)
    
    Returns:
        JSON 응답:
            - 성공 시 (200): 삭제 성공 메시지
            - 실패 시 (404): 해당 entry_id가 존재하지 않는 경우
            - 실패 시 (500): 서버 오류
    """
    try:
        # DB 설정 서비스 인스턴스 생성
        service = DBConfigService()
        
        # DB 설정 정보 삭제
        success = service.delete_db_entry(entry_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': f'{entry_id} deleted.'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': f'{entry_id} not found.'
            }), 404
            
    except Exception as exc:
        logger.error('Failed to delete DB List %s: %s', entry_id, str(exc))
        return jsonify({
            'status': 'error',
            'message': '서버 오류가 발생했습니다.'
        }), 500

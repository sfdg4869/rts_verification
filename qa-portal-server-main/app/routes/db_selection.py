"""
DB 선택 API 라우트
MongoDB에서 가져온 DB 설정 목록에서 Target DB와 Repo DB를 선택하는 API
"""

from flask import Blueprint, request, jsonify
from flasgger import swag_from
import logging

from app.services.db_config_service import db_config_service

# Blueprint 생성
db_selection_bp = Blueprint('db_selection', __name__, url_prefix='/rts/db-selection')
logger = logging.getLogger(__name__)

@db_selection_bp.route('/available-dbs', methods=['GET'])
@swag_from({
    'tags': ['DB Selection'],
    'summary': 'MongoDB에서 사용 가능한 모든 DB 설정 목록 조회',
    'description': 'MongoDB에 저장된 모든 DB 설정을 조회하여 Target/Repo DB 선택을 위한 목록을 제공합니다.',
    'responses': {
        200: {
            'description': '사용 가능한 DB 설정 목록',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'success'},
                    'message': {'type': 'string', 'example': '97개의 DB 설정을 찾았습니다.'},
                    'total_count': {'type': 'integer', 'example': 97},
                    'available_dbs': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'config_id': {'type': 'string', 'example': 'qs1_db_0_1'},
                                'display_name': {'type': 'string', 'example': '10.20.132.101:1521 (oracle19)'},
                                'host': {'type': 'string', 'example': '10.20.132.101'},
                                'port': {'type': 'integer', 'example': 1521},
                                'service_name': {'type': 'string', 'example': 'oracle19'},
                                'username': {'type': 'string', 'example': 'maxgauge'},
                                'description': {'type': 'string', 'example': 'OS: Linux, Collection: qs1_db'}
                            }
                        }
                    },
                    'debug_info': {
                        'type': 'object',
                        'properties': {
                            'service_connected': {'type': 'boolean'},
                            'database_name': {'type': 'string'},
                            'collections_found': {'type': 'array', 'items': {'type': 'string'}},
                            'configs_found': {'type': 'integer'}
                        }
                    }
                }
            }
        },
        500: {
            'description': '서버 오류',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'error'},
                    'message': {'type': 'string', 'example': '오류 메시지'}
                }
            }
        }
    }
})
def get_available_dbs():
    """MongoDB에서 사용 가능한 모든 DB 설정 목록 조회"""
    try:
        logger.info("사용 가능한 DB 설정 목록 조회 요청")
        
        # MongoDB 연결 상태 확인
        is_connected = db_config_service.is_connected()
        collections = []
        
        if is_connected:
            collections = db_config_service.db.list_collection_names()
        
        # 모든 DB 설정 조회
        available_dbs = db_config_service.get_all_db_configs()
        
        logger.info(f"총 {len(available_dbs)}개의 DB 설정 조회 완료")
        
        return jsonify({
            'status': 'success',
            'message': f'{len(available_dbs)}개의 DB 설정을 찾았습니다.',
            'total_count': len(available_dbs),
            'available_dbs': available_dbs,
            'debug_info': {
                'service_connected': is_connected,
                'database_name': db_config_service.db.name if is_connected else None,
                'collections_found': collections,
                'configs_found': len(available_dbs)
            }
        }), 200
        
    except Exception as e:
        logger.error(f"DB 설정 목록 조회 실패: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'DB 설정 목록 조회 중 오류가 발생했습니다: {str(e)}'
        }), 500

@db_selection_bp.route('/select-target', methods=['POST'])
@swag_from({
    'tags': ['DB Selection'],
    'summary': 'Target DB 선택',
    'description': 'MongoDB에서 조회한 DB 설정 중 하나를 Target DB로 선택합니다.',
    'parameters': [
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'required': ['config_id'],
                'properties': {
                    'config_id': {
                        'type': 'string',
                        'description': '선택할 DB 설정의 ID',
                        'example': 'qs1_db_0_1'
                    }
                }
            }
        }
    ],
    'responses': {
        200: {
            'description': 'Target DB 선택 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'success'},
                    'message': {'type': 'string', 'example': 'Target DB가 성공적으로 선택되었습니다.'},
                    'selected_config': {
                        'type': 'object',
                        'properties': {
                            'config_id': {'type': 'string'},
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        },
        400: {
            'description': '잘못된 요청',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'error'},
                    'message': {'type': 'string', 'example': 'config_id는 필수입니다.'}
                }
            }
        },
        500: {
            'description': '서버 오류'
        }
    }
})
def select_target_db():
    """Target DB 선택"""
    try:
        data = request.get_json()
        
        if not data or 'config_id' not in data:
            return jsonify({
                'status': 'error',
                'message': 'config_id는 필수입니다.'
            }), 400
        
        config_id = data['config_id']
        logger.info(f"Target DB 선택 요청: {config_id}")
        
        # Target DB 선택 수행
        success = db_config_service.select_target_db(config_id)
        
        if success:
            # 선택된 설정 조회
            current_target = db_config_service.get_current_target_db()
            
            return jsonify({
                'status': 'success',
                'message': 'Target DB가 성공적으로 선택되었습니다.',
                'selected_config': current_target
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Target DB 선택에 실패했습니다.'
            }), 500
            
    except Exception as e:
        logger.error(f"Target DB 선택 실패: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Target DB 선택 중 오류가 발생했습니다: {str(e)}'
        }), 500

@db_selection_bp.route('/select-repo', methods=['POST'])
@swag_from({
    'tags': ['DB Selection'],
    'summary': 'Repo DB 선택',
    'description': 'MongoDB에서 조회한 DB 설정 중 하나를 Repo DB로 선택합니다.',
    'parameters': [
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'required': ['config_id'],
                'properties': {
                    'config_id': {
                        'type': 'string',
                        'description': '선택할 DB 설정의 ID',
                        'example': 'qs1_db_0_2'
                    }
                }
            }
        }
    ],
    'responses': {
        200: {
            'description': 'Repo DB 선택 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'success'},
                    'message': {'type': 'string', 'example': 'Repo DB가 성공적으로 선택되었습니다.'},
                    'selected_config': {
                        'type': 'object',
                        'properties': {
                            'config_id': {'type': 'string'},
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        },
        400: {
            'description': '잘못된 요청'
        },
        500: {
            'description': '서버 오류'
        }
    }
})
def select_repo_db():
    """Repo DB 선택"""
    try:
        data = request.get_json()
        
        if not data or 'config_id' not in data:
            return jsonify({
                'status': 'error',
                'message': 'config_id는 필수입니다.'
            }), 400
        
        config_id = data['config_id']
        logger.info(f"Repo DB 선택 요청: {config_id}")
        
        # Repo DB 선택 수행
        success = db_config_service.select_repo_db(config_id)
        
        if success:
            # 선택된 설정 조회
            current_repo = db_config_service.get_current_repo_db()
            
            return jsonify({
                'status': 'success',
                'message': 'Repo DB가 성공적으로 선택되었습니다.',
                'selected_config': current_repo
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Repo DB 선택에 실패했습니다.'
            }), 500
            
    except Exception as e:
        logger.error(f"Repo DB 선택 실패: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Repo DB 선택 중 오류가 발생했습니다: {str(e)}'
        }), 500

@db_selection_bp.route('/current-selection', methods=['GET'])
@swag_from({
    'tags': ['DB Selection'],
    'summary': '현재 선택된 Target/Repo DB 조회',
    'description': '현재 선택된 Target DB와 Repo DB 설정을 조회합니다.',
    'responses': {
        200: {
            'description': '현재 선택된 DB 설정',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'success'},
                    'current_selection': {
                        'type': 'object',
                        'properties': {
                            'target_db': {
                                'type': 'object',
                                'properties': {
                                    'config_id': {'type': 'string'},
                                    'host': {'type': 'string'},
                                    'port': {'type': 'integer'},
                                    'service_name': {'type': 'string'},
                                    'username': {'type': 'string'},
                                    'description': {'type': 'string'}
                                }
                            },
                            'repo_db': {
                                'type': 'object',
                                'properties': {
                                    'config_id': {'type': 'string'},
                                    'host': {'type': 'string'},
                                    'port': {'type': 'integer'},
                                    'service_name': {'type': 'string'},
                                    'username': {'type': 'string'},
                                    'description': {'type': 'string'}
                                }
                            }
                        }
                    }
                }
            }
        },
        500: {
            'description': '서버 오류'
        }
    }
})
def get_current_selection():
    """현재 선택된 Target/Repo DB 조회"""
    try:
        logger.info("현재 선택된 DB 설정 조회 요청")
        
        # 현재 선택된 Target/Repo DB 조회
        current_target = db_config_service.get_current_target_db()
        current_repo = db_config_service.get_current_repo_db()
        
        return jsonify({
            'status': 'success',
            'current_selection': {
                'target_db': current_target,
                'repo_db': current_repo
            }
        }), 200
        
    except Exception as e:
        logger.error(f"현재 선택된 DB 설정 조회 실패: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'현재 선택된 DB 설정 조회 중 오류가 발생했습니다: {str(e)}'
        }), 500

@db_selection_bp.route('/search', methods=['GET'])
@swag_from({
    'tags': ['DB Selection'],
    'summary': 'DB 설정 검색',
    'description': 'host, service, username 등으로 DB 설정을 검색합니다.',
    'parameters': [
        {
            'name': 'q',
            'in': 'query',
            'type': 'string',
            'required': True,
            'description': '검색어 (host, service, username 등)',
            'example': '132.101'
        }
    ],
    'responses': {
        200: {
            'description': '검색된 DB 설정 목록',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'success'},
                    'message': {'type': 'string', 'example': '5개의 DB 설정을 찾았습니다.'},
                    'search_term': {'type': 'string', 'example': '132.101'},
                    'total_count': {'type': 'integer', 'example': 5},
                    'search_results': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'config_id': {'type': 'string'},
                                'display_name': {'type': 'string'},
                                'host': {'type': 'string'},
                                'port': {'type': 'integer'},
                                'service_name': {'type': 'string'},
                                'username': {'type': 'string'}
                            }
                        }
                    }
                }
            }
        },
        400: {
            'description': '잘못된 요청'
        },
        500: {
            'description': '서버 오류'
        }
    }
})
def search_db_configs():
    """DB 설정 검색"""
    try:
        search_term = request.args.get('q', '').strip()
        
        if not search_term:
            return jsonify({
                'status': 'error',
                'message': '검색어를 입력해주세요.'
            }), 400
        
        logger.info(f"DB 설정 검색 요청: {search_term}")
        
        # 모든 설정에서 검색
        all_configs = db_config_service.get_all_db_configs()
        search_results = []
        
        search_term_lower = search_term.lower()
        
        for config in all_configs:
            # 여러 필드에서 검색
            searchable_text = f"{config.get('host', '')} {config.get('service_name', '')} {config.get('username', '')} {config.get('description', '')}".lower()
            
            if search_term_lower in searchable_text:
                search_results.append(config)
        
        logger.info(f"검색 완료: {len(search_results)}개 결과")
        
        return jsonify({
            'status': 'success',
            'message': f'{len(search_results)}개의 DB 설정을 찾았습니다.',
            'search_term': search_term,
            'total_count': len(search_results),
            'search_results': search_results
        }), 200
        
    except Exception as e:
        logger.error(f"DB 설정 검색 실패: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'DB 설정 검색 중 오류가 발생했습니다: {str(e)}'
        }), 500
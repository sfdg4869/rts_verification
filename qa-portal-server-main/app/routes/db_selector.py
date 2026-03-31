from flask import Blueprint, request, jsonify, render_template
from flasgger import swag_from
import logging

logger = logging.getLogger(__name__)

db_selector_bp = Blueprint('db_selector', __name__, url_prefix='/rts/db-selector')

@db_selector_bp.route('/', methods=['GET'])
def db_selector_page():
    """DB 선택 웹 인터페이스"""
    return render_template('db_selector.html')

@db_selector_bp.route('/available-dbs', methods=['GET'])
@swag_from({
    'tags': ['DB Selector'],
    'summary': '📋 사용 가능한 모든 DB 목록 조회',
    'description': 'MongoDB에서 사용 가능한 모든 DB 설정을 조회하여 Target/Repo 선택에 사용',
    'responses': {
        200: {
            'description': '사용 가능한 DB 목록 조회 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'message': {'type': 'string'},
                    'available_dbs': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'id': {'type': 'string', 'description': 'DB 설정 고유 ID'},
                                'display_name': {'type': 'string', 'description': '화면 표시용 이름'},
                                'host': {'type': 'string'},
                                'port': {'type': 'integer'},
                                'service_name': {'type': 'string'},
                                'username': {'type': 'string'},
                                'description': {'type': 'string'},
                                'config_type': {'type': 'string', 'enum': ['target', 'repo', 'general']}
                            }
                        }
                    },
                    'total_count': {'type': 'integer'}
                }
            }
        }
    }
})
def get_available_dbs():
    """MongoDB에서 사용 가능한 모든 DB 목록 조회"""
    try:
        from app.services.db_config_service import DBConfigService
        service = DBConfigService()
        
        # 디버깅: MongoDB 연결 상태 확인
        if not service.is_connected():
            logger.error("MongoDB가 연결되지 않았습니다")
            return jsonify({
                'status': 'error', 
                'message': 'MongoDB 연결이 필요합니다.',
                'debug_info': {
                    'client_exists': service.client is not None,
                    'db_exists': service.db is not None
                },
                'available_dbs': [],
                'total_count': 0
            }), 500
        
        # 디버깅: 컬렉션 목록 확인
        try:
            collections = service.db.list_collection_names()
            logger.info(f"사용 가능한 컬렉션: {collections}")
        except Exception as e:
            logger.error(f"컬렉션 목록 조회 실패: {e}")
            return jsonify({
                'status': 'error',
                'message': f'컬렉션 목록 조회 실패: {str(e)}',
                'available_dbs': [],
                'total_count': 0
            }), 500
        
        # MongoDB에서 모든 DB 설정 조회
        configs = service.get_all_db_configs()
        logger.info(f"조회된 DB 설정 개수: {len(configs)}")
        
        # 디버깅 정보 추가
        debug_info = {
            'collections_found': collections,
            'configs_found': len(configs),
            'service_connected': service.is_connected(),
            'database_name': getattr(service.db, 'name', 'None') if hasattr(service, 'db') and service.db is not None else 'None'
        }
        
        # 사용자 선택을 위한 형태로 변환
        available_dbs = []
        for i, config in enumerate(configs):
            db_info = {
                'id': config.get('config_id', f'db_{i}'),
                'display_name': config.get('display_name', f"{config.get('host', 'Unknown')}:{config.get('port', '1521')}"),
                'host': config.get('host', ''),
                'port': config.get('port', 1521),
                'service_name': config.get('service_name', ''),
                'username': config.get('username', ''),
                'description': config.get('description', ''),
                'config_type': config.get('config_type', 'general')
            }
            available_dbs.append(db_info)
        
        return jsonify({
            'status': 'success',
            'message': f'{len(available_dbs)}개의 DB 설정을 찾았습니다.',
            'available_dbs': available_dbs,
            'total_count': len(available_dbs),
            'debug_info': debug_info
        })
        
    except Exception as e:
        logger.error(f"사용 가능한 DB 목록 조회 실패: {e}")
        import traceback
        logger.error(f"전체 에러 스택: {traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': f'DB 목록 조회 중 오류: {str(e)}',
            'available_dbs': [],
            'total_count': 0
        }), 500

@db_selector_bp.route('/select-target', methods=['POST'])
@swag_from({
    'tags': ['DB Selector'],
    'summary': '🎯 Target DB 선택 및 설정',
    'description': '사용 가능한 DB 목록에서 선택하여 Target DB로 설정',
    'parameters': [
        {
            'name': 'selection_data',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'required': ['selected_db_id', 'password'],
                'properties': {
                    'selected_db_id': {'type': 'string', 'description': '선택한 DB의 ID'},
                    'password': {'type': 'string', 'description': '선택한 DB의 비밀번호'},
                    'custom_settings': {
                        'type': 'object',
                        'description': '사용자 정의 설정 (선택사항)',
                        'properties': {
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        }
    ],
    'responses': {
        200: {
            'description': 'Target DB 설정 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'message': {'type': 'string'},
                    'selected_db': {
                        'type': 'object',
                        'properties': {
                            'id': {'type': 'string'},
                            'display_name': {'type': 'string'},
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        },
        400: {'description': '잘못된 요청 또는 선택한 DB를 찾을 수 없음'},
        500: {'description': '서버 오류'}
    }
})
def select_target_db():
    """Target DB 선택 및 설정"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': '요청 데이터가 없습니다.'
            }), 400
        
        selected_db_id = data.get('selected_db_id')
        password = data.get('password')
        custom_settings = data.get('custom_settings', {})
        
        if not selected_db_id or not password:
            return jsonify({
                'status': 'error',
                'message': '선택한 DB ID와 비밀번호가 필요합니다.'
            }), 400
        
        # 선택 가능한 DB 목록에서 선택한 DB 찾기
        from app.services.db_config_service import DBConfigService
        service = DBConfigService()
        configs = service.get_all_db_configs()
        
        selected_config = None
        for config in configs:
            if config.get('config_id') == selected_db_id:
                selected_config = config
                break
        
        if not selected_config:
            return jsonify({
                'status': 'error',
                'message': f'선택한 DB (ID: {selected_db_id})를 찾을 수 없습니다.'
            }), 400
        
        # 사용자 정의 설정이 있으면 적용
        final_config = {
            'host': custom_settings.get('host', selected_config.get('host')),
            'port': custom_settings.get('port', selected_config.get('port')),
            'service_name': custom_settings.get('service_name', selected_config.get('service_name')),
            'username': custom_settings.get('username', selected_config.get('username')),
            'password': password
        }
        
        # Target DB로 설정
        from app.shared_db import save_target_config_to_mongodb
        success = save_target_config_to_mongodb(final_config)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': f'Target DB가 성공적으로 설정되었습니다.',
                'selected_db': {
                    'id': selected_db_id,
                    'display_name': selected_config.get('display_name', ''),
                    'host': final_config['host'],
                    'port': final_config['port'],
                    'service_name': final_config['service_name'],
                    'username': final_config['username']
                }
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Target DB 설정 저장에 실패했습니다.'
            }), 500
            
    except Exception as e:
        logger.error(f"Target DB 선택 실패: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Target DB 선택 중 오류: {str(e)}'
        }), 500

@db_selector_bp.route('/select-repo', methods=['POST'])
@swag_from({
    'tags': ['DB Selector'],
    'summary': '📊 Repo DB 선택 및 설정',
    'description': '사용 가능한 DB 목록에서 선택하여 Repo DB로 설정',
    'parameters': [
        {
            'name': 'selection_data',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'required': ['selected_db_id', 'password'],
                'properties': {
                    'selected_db_id': {'type': 'string', 'description': '선택한 DB의 ID'},
                    'password': {'type': 'string', 'description': '선택한 DB의 비밀번호'},
                    'custom_settings': {
                        'type': 'object',
                        'description': '사용자 정의 설정 (선택사항)',
                        'properties': {
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        }
    ],
    'responses': {
        200: {
            'description': 'Repo DB 설정 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'message': {'type': 'string'},
                    'selected_db': {
                        'type': 'object',
                        'properties': {
                            'id': {'type': 'string'},
                            'display_name': {'type': 'string'},
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        },
        400: {'description': '잘못된 요청 또는 선택한 DB를 찾을 수 없음'},
        500: {'description': '서버 오류'}
    }
})
def select_repo_db():
    """Repo DB 선택 및 설정"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': '요청 데이터가 없습니다.'
            }), 400
        
        selected_db_id = data.get('selected_db_id')
        password = data.get('password')
        custom_settings = data.get('custom_settings', {})
        
        if not selected_db_id or not password:
            return jsonify({
                'status': 'error',
                'message': '선택한 DB ID와 비밀번호가 필요합니다.'
            }), 400
        
        # 선택 가능한 DB 목록에서 선택한 DB 찾기
        from app.services.db_config_service import DBConfigService
        service = DBConfigService()
        configs = service.get_all_db_configs()
        
        selected_config = None
        for config in configs:
            if config.get('config_id') == selected_db_id:
                selected_config = config
                break
        
        if not selected_config:
            return jsonify({
                'status': 'error',
                'message': f'선택한 DB (ID: {selected_db_id})를 찾을 수 없습니다.'
            }), 400
        
        # 사용자 정의 설정이 있으면 적용
        final_config = {
            'host': custom_settings.get('host', selected_config.get('host')),
            'port': custom_settings.get('port', selected_config.get('port')),
            'service_name': custom_settings.get('service_name', selected_config.get('service_name')),
            'username': custom_settings.get('username', selected_config.get('username')),
            'password': password
        }
        
        # Repo DB로 설정
        from app.shared_db import save_repo_config_to_mongodb
        success = save_repo_config_to_mongodb(final_config)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': f'Repo DB가 성공적으로 설정되었습니다.',
                'selected_db': {
                    'id': selected_db_id,
                    'display_name': selected_config.get('display_name', ''),
                    'host': final_config['host'],
                    'port': final_config['port'],
                    'service_name': final_config['service_name'],
                    'username': final_config['username']
                }
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Repo DB 설정 저장에 실패했습니다.'
            }), 500
            
    except Exception as e:
        logger.error(f"Repo DB 선택 실패: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Repo DB 선택 중 오류: {str(e)}'
        }), 500

@db_selector_bp.route('/current-settings', methods=['GET'])
@swag_from({
    'tags': ['DB Selector'],
    'summary': '⚙️ 현재 Target/Repo DB 설정 조회',
    'description': '현재 설정된 Target DB와 Repo DB 정보 조회',
    'responses': {
        200: {
            'description': '현재 설정 조회 성공',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'target_db': {
                        'type': 'object',
                        'properties': {
                            'configured': {'type': 'boolean'},
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    },
                    'repo_db': {
                        'type': 'object',
                        'properties': {
                            'configured': {'type': 'boolean'},
                            'host': {'type': 'string'},
                            'port': {'type': 'integer'},
                            'service_name': {'type': 'string'},
                            'username': {'type': 'string'}
                        }
                    }
                }
            }
        }
    }
})
def get_current_settings():
    """현재 Target/Repo DB 설정 조회"""
    try:
        from app.shared_db import get_target_config_from_mongodb, get_repo_config_from_mongodb
        
        # Target DB 설정
        target_config = get_target_config_from_mongodb()
        target_info = {
            'configured': target_config is not None
        }
        if target_config:
            target_info.update({
                'host': target_config.get('host', ''),
                'port': target_config.get('port', 1521),
                'service_name': target_config.get('service_name', ''),
                'username': target_config.get('username', '')
            })
        
        # Repo DB 설정
        repo_config = get_repo_config_from_mongodb()
        repo_info = {
            'configured': repo_config is not None
        }
        if repo_config:
            repo_info.update({
                'host': repo_config.get('host', ''),
                'port': repo_config.get('port', 1521),
                'service_name': repo_config.get('service_name', ''),
                'username': repo_config.get('username', '')
            })
        
        return jsonify({
            'status': 'success',
            'target_db': target_info,
            'repo_db': repo_info
        })
        
    except Exception as e:
        logger.error(f"현재 설정 조회 실패: {e}")
        return jsonify({
            'status': 'error',
            'message': f'현재 설정 조회 중 오류: {str(e)}'
        }), 500

@db_selector_bp.route('/debug-mongodb', methods=['GET'])
@swag_from({
    'tags': ['DB Selector'],
    'summary': '🔍 MongoDB 디버깅 정보',
    'description': 'MongoDB 연결 상태 및 데이터 구조 확인',
    'responses': {
        200: {
            'description': 'MongoDB 디버깅 정보',
            'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'mongodb_connected': {'type': 'boolean'},
                    'database_name': {'type': 'string'},
                    'collections': {'type': 'array', 'items': {'type': 'string'}},
                    'collection_details': {'type': 'object'},
                    'sample_documents': {'type': 'object'}
                }
            }
        }
    }
})
def debug_mongodb():
    """MongoDB 디버깅 정보"""
    try:
        from app.services.db_config_service import DBConfigService
        import os
        
        service = DBConfigService()
        
        debug_info = {
            'status': 'success',
            'mongodb_connected': service.is_connected(),
            'database_name': os.getenv('MONGODB_DATABASE', 'qs1_db'),
            'collections': [],
            'collection_details': {},
            'sample_documents': {},
            'client_status': service.client is not None,
            'db_status': hasattr(service, 'db') and service.db is not None
        }
        
        if service.is_connected():
            # 컬렉션 목록
            collections = service.db.list_collection_names()
            debug_info['collections'] = collections
            
            # 각 컬렉션의 상세 정보
            for collection_name in collections[:5]:  # 최대 5개만
                collection = service.db[collection_name]
                count = collection.count_documents({})
                debug_info['collection_details'][collection_name] = {
                    'document_count': count,
                    'has_entries_field': collection.count_documents({"entries": {"$exists": True}}),
                    'has_host_field': collection.count_documents({"host": {"$exists": True}})
                }
                
                # 샘플 문서 (첫 번째 문서)
                sample = collection.find_one()
                if sample:
                    # ObjectId는 문자열로 변환
                    if '_id' in sample:
                        sample['_id'] = str(sample['_id'])
                    debug_info['sample_documents'][collection_name] = sample
        
        return jsonify(debug_info)
        
    except Exception as e:
        logger.error(f"MongoDB 디버깅 실패: {e}")
        return jsonify({
            'status': 'error',
            'message': f'MongoDB 디버깅 중 오류: {str(e)}',
            'mongodb_connected': False
        }), 500
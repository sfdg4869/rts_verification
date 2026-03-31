"""
MongoDB 기반 DB 설정 관리 서비스
Target DB, Repo DB 설정의 CRUD 및 연결 관리
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
import os
import uuid

from app.models.db_config import DBConfigModel, MONGODB_CONFIG

class DBConfigService:
    """MongoDB 기반 DB 설정 관리 서비스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        try:
            self.client = MongoClient(MONGODB_CONFIG['uri'])
            self.db = self.client[MONGODB_CONFIG['database']]  # admin DB
            self.db_configs_collection = self.db[MONGODB_CONFIG['collections']['db_configs']]  # qs1_db 컬렉션
            self.profiles_collection = self.db[MONGODB_CONFIG['collections']['connection_profiles']]
            # 호환성을 위한 별칭 추가
            self.connection_profiles = self.profiles_collection
            self.logger.info(f"MongoDB 연결 성공: {MONGODB_CONFIG['database']}.{MONGODB_CONFIG['collections']['db_configs']}")
        except Exception as e:
            self.logger.error(f"MongoDB 연결 실패: {str(e)}")
            self.client = None
            self.db = None
            self.db_configs_collection = None
            self.profiles_collection = None
            self.connection_profiles = None
    
    
    def is_connected(self) -> bool:
        """MongoDB 연결 상태 확인"""
        try:
            return self.client is not None and hasattr(self, 'db') and self.db is not None
        except:
            return False
    
    def test_connection(self) -> bool:
        """MongoDB 연결 테스트"""
        try:
            if not self.client:
                return False
            
            # ping 명령으로 연결 상태 확인
            self.client.admin.command('ping')
            return True
        except Exception as e:
            self.logger.error(f"MongoDB 연결 테스트 실패: {e}")
            return False
    
    # Target DB 설정 관리
    def save_target_config(self, config: Dict[str, Any]) -> bool:
        """Target DB 설정 저장"""
        try:
            if not self.is_connected():
                return False
            
            if not DBConfigModel.validate_target_config(config):
                self.logger.error("Target DB 설정 유효성 검증 실패")
                return False
            
            target_config = DBConfigModel.create_target_config(**config)
            
            # upsert 수행
            result = self.db_configs.replace_one(
                {"config_id": target_config["config_id"]},
                target_config,
                upsert=True
            )
            
            self.logger.info(f"Target DB 설정 저장 완료: {target_config['config_id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Target DB 설정 저장 실패: {e}")
            return False
    
    def get_target_config(self, name: str) -> Optional[Dict[str, Any]]:
        """Target DB 설정 조회"""
        try:
            if not self.is_connected():
                return None
            
            config = self.db_configs.find_one({
                "config_id": f"target_{name}",
                "is_active": True
            })
            
            return config
            
        except Exception as e:
            self.logger.error(f"Target DB 설정 조회 실패: {e}")
            return None
    
    # Repo DB 설정 관리
    def save_repo_config(self, config: Dict[str, Any]) -> bool:
        """Repo DB 설정 저장"""
        try:
            if not self.is_connected():
                return False
            
            if not DBConfigModel.validate_repo_config(config):
                self.logger.error("Repo DB 설정 유효성 검증 실패")
                return False
            
            repo_config = DBConfigModel.create_repo_config(**config)
            
            # upsert 수행
            result = self.db_configs.replace_one(
                {"config_id": repo_config["config_id"]},
                repo_config,
                upsert=True
            )
            
            self.logger.info(f"Repo DB 설정 저장 완료: {repo_config['config_id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Repo DB 설정 저장 실패: {e}")
            return False
    
    def get_repo_config(self, name: str) -> Optional[Dict[str, Any]]:
        """Repo DB 설정 조회"""
        try:
            if not self.is_connected():
                return None
            
            config = self.db_configs.find_one({
                "config_id": f"repo_{name}",
                "is_active": True
            })
            
            return config
            
        except Exception as e:
            self.logger.error(f"Repo DB 설정 조회 실패: {e}")
            return None
    
    def get_all_db_configs(self) -> List[Dict[str, Any]]:
        """모든 DB 설정 조회 - 실제 MongoDB 구조에 맞게 수정"""
        try:
            if self.client is None or not hasattr(self, 'db') or self.db is None:
                self.logger.error("MongoDB 클라이언트가 연결되지 않음")
                return []
            
            configs = []
            
            # 모든 컬렉션에서 DB 설정 형태의 데이터 찾기
            collections = self.db.list_collection_names()
            self.logger.info(f"발견된 컬렉션: {collections}")
            
            for collection_name in collections:
                collection = self.db[collection_name]
                total_docs = collection.count_documents({})
                self.logger.info(f"컬렉션 {collection_name}: {total_docs}개 문서")
                
                if total_docs == 0:
                    self.logger.info(f"컬렉션 {collection_name}이 비어있음, 건너뜀")
                    continue
                
                # entries 배열이 있는 문서 찾기
                entries_docs = list(collection.find({"entries": {"$exists": True}}))
                self.logger.info(f"컬렉션 {collection_name}에서 entries가 있는 문서: {len(entries_docs)}개")
                
                if entries_docs:
                    for doc_idx, doc in enumerate(entries_docs):
                        entries = doc.get('entries', [])
                        self.logger.info(f"문서 {doc_idx}의 entries 개수: {len(entries)} (타입: {type(entries)})")
                        
                        if not isinstance(entries, list):
                            self.logger.warning(f"entries가 배열이 아님: {type(entries)}")
                            continue
                        
                        for i, entry in enumerate(entries):
                            if not isinstance(entry, dict):
                                self.logger.debug(f"Entry {i}는 dict가 아님: {type(entry)}")
                                continue
                                
                            self.logger.debug(f"Entry {i} 키들: {list(entry.keys())}")
                            
                            if 'host' in entry:
                                # 실제 데이터 구조에 맞게 설정 추출 - config_id를 entry_X 형태로 통일
                                config = {
                                    'config_id': f"entry_{i}",  # 드롭다운과 일치하도록 수정
                                    'display_name': f"{entry.get('host', 'Unknown')}:{entry.get('port', 1521)} ({entry.get('service', 'N/A')})",
                                    'config_type': 'general',
                                    'host': entry.get('host', ''),
                                    'port': int(entry.get('port', 1521)),
                                    'service_name': entry.get('service', ''),  # 'service' 필드를 'service_name'으로 매핑
                                    'username': entry.get('user', ''),         # 'user' 필드를 'username'으로 매핑
                                    'password': entry.get('password', ''),     # password 필드 추가
                                    'description': f"OS: {entry.get('os', 'Unknown')}, Collection: {collection_name}",
                                    'source_collection': collection_name,
                                    'entry_index': i
                                }
                                configs.append(config)
                                self.logger.info(f"DB 설정 추가: {config['display_name']} (ID: {config['config_id']})")
                            else:
                                self.logger.debug(f"Entry {i}에 host 필드가 없음: {list(entry.keys())}")
                
                # 혹시 직접 host 필드가 있는 문서도 확인
                direct_docs = list(collection.find({
                    "host": {"$exists": True},
                    "entries": {"$exists": False}
                }))
                
                if direct_docs:
                    self.logger.info(f"컬렉션 {collection_name}에서 직접 host가 있는 문서: {len(direct_docs)}개")
                    
                    for doc in direct_docs:
                        config = {
                            'config_id': f"{collection_name}_direct_{str(doc.get('_id', 'unknown'))}",
                            'display_name': f"{doc.get('host', 'Unknown')}:{doc.get('port', 1521)} ({doc.get('service', doc.get('service_name', 'N/A'))})",
                            'config_type': doc.get('config_type', 'general'),
                            'host': doc.get('host', ''),
                            'port': int(doc.get('port', 1521)),
                            'service_name': doc.get('service', doc.get('service_name', '')),
                            'username': doc.get('user', doc.get('username', '')),
                            'description': doc.get('description', f"Direct config from {collection_name}"),
                            'source_collection': collection_name
                        }
                        configs.append(config)
                        self.logger.info(f"직접 DB 설정 추가: {config['display_name']}")
                
                # 컬렉션이 비어있지 않지만 위의 조건에 맞지 않는 경우, 첫 번째 문서 구조 확인
                if total_docs > 0 and not entries_docs and not direct_docs:
                    sample_doc = collection.find_one()
                    if sample_doc:
                        self.logger.info(f"컬렉션 {collection_name}의 샘플 문서 구조: {list(sample_doc.keys())}")
                        # 샘플 문서 내용도 로깅 (ObjectId는 제외)
                        safe_doc = {k: v for k, v in sample_doc.items() if k != '_id'}
                        self.logger.info(f"샘플 문서 내용 (일부): {str(safe_doc)[:200]}...")
            
            # 모든 항목 유지 (config_id 기준으로 고유성 보장)
            # entry_X 형태의 config_id는 이미 고유하므로 중복 제거 불필요
            unique_configs = configs  # 중복 제거 로직 비활성화
            
            self.logger.info(f"실제 MongoDB에서 DB 설정 {len(unique_configs)}개 조회 완료")
            return unique_configs
            
        except Exception as e:
            self.logger.error(f"DB 설정 목록 조회 실패: {e}")
            import traceback
            self.logger.error(f"에러 상세: {traceback.format_exc()}")
            return []
    
    def get_db_configs_by_type(self, config_type: str) -> List[Dict[str, Any]]:
        """타입별 DB 설정 조회"""
        try:
            if not self.is_connected():
                return []
            
            configs = list(self.db_configs.find({
                "config_type": config_type,
                "is_active": True
            }))
            
            # ObjectId 제거 및 정리
            for config in configs:
                if '_id' in config:
                    del config['_id']
            
            return configs
            
        except Exception as e:
            self.logger.error(f"{config_type} DB 설정 조회 실패: {e}")
            return []
    
    def search_db_configs(self, search_term: str) -> List[Dict[str, Any]]:
        """DB 설정 검색"""
        try:
            if not self.is_connected():
                return []
            
            # 다양한 필드에서 검색
            query = {
                "$or": [
                    {"config_id": {"$regex": search_term, "$options": "i"}},
                    {"host": {"$regex": search_term, "$options": "i"}},
                    {"service_name": {"$regex": search_term, "$options": "i"}},
                    {"username": {"$regex": search_term, "$options": "i"}},
                    {"description": {"$regex": search_term, "$options": "i"}}
                ],
                "is_active": True
            }
            
            configs = list(self.db_configs.find(query))
            
            # ObjectId 제거 및 정리
            for config in configs:
                if '_id' in config:
                    del config['_id']
            
            return configs
            
        except Exception as e:
            self.logger.error(f"DB 설정 검색 실패: {e}")
            return []
    
    # 연결 프로필 관리
    def save_connection_profile(self, profile: Dict[str, Any]) -> bool:
        """연결 프로필 저장"""
        try:
            if not self.is_connected():
                return False
            
            profile_data = DBConfigModel.create_connection_profile(**profile)
            
            # upsert 수행
            result = self.profiles_collection.replace_one(
                {"profile_id": profile_data["profile_id"]},
                profile_data,
                upsert=True
            )
            
            self.logger.info(f"연결 프로필 저장 완료: {profile_data['profile_id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"연결 프로필 저장 실패: {e}")
            return False
    
    def get_connection_profile(self, profile_id: str) -> Optional[Dict[str, Any]]:
        """연결 프로필 조회 (Repo 설정만 포함)"""
        try:
            if not self.is_connected():
                return None
            
            profile = self.connection_profiles.find_one({
                "profile_id": profile_id,
                "is_active": True
            })
            
            if not profile:
                return None
            
            # Repo 설정만 추가 (Target DB는 APM_DB_INFO에서 선택)
            repo_config = None
            if 'repo_config_id' in profile:
                repo_config_id = profile["repo_config_id"]
                
                # repo_config_id가 "entry_X" 형식이면 MongoDB entries에서 찾기
                if repo_config_id.startswith("entry_"):
                    try:
                        entry_index = int(repo_config_id.split("_")[1])
                        # entries 배열이 있는 문서 찾기
                        doc = self.db_configs_collection.find_one({"entries": {"$exists": True}})
                        if doc and 'entries' in doc:
                            entries = doc.get('entries', [])
                            if entry_index < len(entries):
                                repo_config = entries[entry_index]
                                # config_id 추가
                                repo_config['config_id'] = repo_config_id
                    except (ValueError, IndexError):
                        pass
                else:
                    # 기존 방식: db_configs 컬렉션에서 찾기
                    repo_config = self.db_configs_collection.find_one({
                        "config_id": repo_config_id,
                        "is_active": True
                    })
            
            if repo_config:
                profile["repo_config"] = repo_config
            
            return profile
            
        except Exception as e:
            self.logger.error(f"연결 프로필 조회 실패: {e}")
            return None
    
    def get_default_connection_profile(self) -> Optional[Dict[str, Any]]:
        """기본 연결 프로필 조회"""
        try:
            if not self.is_connected():
                return None
            
            profile = self.connection_profiles.find_one({
                "is_default": True,
                "is_active": True
            })
            
            if profile:
                return self.get_connection_profile(profile["profile_id"])
            
            return None
            
        except Exception as e:
            self.logger.error(f"기본 연결 프로필 조회 실패: {e}")
            return None
    
    def set_default_profile(self, profile_id: str) -> bool:
        """기본 프로필 설정"""
        try:
            if not self.is_connected():
                return False
            
            # 기존 기본 프로필 해제
            self.profiles_collection.update_many(
                {"is_default": True},
                {"$set": {"is_default": False, "updated_at": datetime.utcnow()}}
            )
            
            # 새 기본 프로필 설정
            result = self.profiles_collection.update_one(
                {"profile_id": profile_id, "is_active": True},
                {"$set": {"is_default": True, "updated_at": datetime.utcnow()}}
            )
            
            if result.modified_count > 0:
                self.logger.info(f"기본 프로필 설정 완료: {profile_id}")
                return True
            else:
                self.logger.warning(f"프로필을 찾을 수 없음: {profile_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"기본 프로필 설정 실패: {e}")
            return False
    
    def get_connection_profiles(self) -> List[Dict[str, Any]]:
        """모든 활성 연결 프로필 목록 조회"""
        try:
            if not self.is_connected():
                return []
            
            profiles = list(self.profiles_collection.find({
                "is_active": True
            }).sort("created_at", -1))
            
            return profiles
            
        except Exception as e:
            self.logger.error(f"연결 프로필 목록 조회 실패: {e}")
            return []
    
    # DB 선택 관련 메서드들
    def select_target_db(self, config_id: str) -> bool:
        """Target DB 선택 및 설정 저장"""
        try:
            if not self.is_connected():
                return False
            
            # 선택된 설정을 조회
            all_configs = self.get_all_db_configs()
            selected_config = None
            
            for config in all_configs:
                if config['config_id'] == config_id:
                    selected_config = config
                    break
            
            if not selected_config:
                self.logger.error(f"선택된 Target DB 설정을 찾을 수 없습니다: {config_id}")
                return False
            
            # Target DB 설정으로 저장
            target_data = {
                'config_id': 'current_target',
                'config_type': 'target',
                'name': f"target_{selected_config['host']}_{selected_config['port']}",
                'host': selected_config['host'],
                'port': selected_config['port'],
                'service_name': selected_config['service_name'],
                'username': selected_config['username'],
                'description': f"Selected Target DB: {selected_config['display_name']}",
                'source_config_id': config_id,
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # 기존 Target DB 설정을 비활성화
            self.db_configs_collection.update_many(
                {"config_type": "target", "config_id": "current_target"},
                {"$set": {"is_active": False, "updated_at": datetime.utcnow()}}
            )
            
            # 새 Target DB 설정 저장
            result = self.db_configs_collection.replace_one(
                {"config_id": "current_target", "config_type": "target"},
                target_data,
                upsert=True
            )
            
            self.logger.info(f"Target DB 선택 완료: {selected_config['display_name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Target DB 선택 실패: {e}")
            return False
    
    def select_repo_db(self, config_id: str) -> bool:
        """Repo DB 선택 및 설정 저장"""
        try:
            if not self.is_connected():
                return False
            
            # 선택된 설정을 조회
            all_configs = self.get_all_db_configs()
            selected_config = None
            
            for config in all_configs:
                if config['config_id'] == config_id:
                    selected_config = config
                    break
            
            if not selected_config:
                self.logger.error(f"선택된 Repo DB 설정을 찾을 수 없습니다: {config_id}")
                return False
            
            # Repo DB 설정으로 저장
            repo_data = {
                'config_id': 'current_repo',
                'config_type': 'repo',
                'name': f"repo_{selected_config['host']}_{selected_config['port']}",
                'host': selected_config['host'],
                'port': selected_config['port'],
                'service_name': selected_config['service_name'],
                'username': selected_config['username'],
                'description': f"Selected Repo DB: {selected_config['display_name']}",
                'source_config_id': config_id,
                'db_id': 1,  # Repo DB의 경우 db_id 필수
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # 기존 Repo DB 설정을 비활성화
            self.db_configs_collection.update_many(
                {"config_type": "repo", "config_id": "current_repo"},
                {"$set": {"is_active": False, "updated_at": datetime.utcnow()}}
            )
            
            # 새 Repo DB 설정 저장
            result = self.db_configs_collection.replace_one(
                {"config_id": "current_repo", "config_type": "repo"},
                repo_data,
                upsert=True
            )
            
            self.logger.info(f"Repo DB 선택 완료: {selected_config['display_name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Repo DB 선택 실패: {e}")
            return False
    
    def get_current_target_db(self) -> Optional[Dict[str, Any]]:
        """현재 선택된 Target DB 설정 조회"""
        try:
            if not self.is_connected():
                return None
            
            config = self.db_configs_collection.find_one({
                "config_id": "current_target",
                "config_type": "target",
                "is_active": True
            })
            
            if config and '_id' in config:
                del config['_id']
                
            return config
            
        except Exception as e:
            self.logger.error(f"현재 Target DB 설정 조회 실패: {e}")
            return None
    
    def get_current_repo_db(self) -> Optional[Dict[str, Any]]:
        """현재 선택된 Repo DB 설정 조회"""
        try:
            if not self.is_connected():
                return None
            
            config = self.db_configs_collection.find_one({
                "config_id": "current_repo",
                "config_type": "repo",
                "is_active": True
            })
            
            if config and '_id' in config:
                del config['_id']
                
            return config
            
        except Exception as e:
            self.logger.error(f"현재 Repo DB 설정 조회 실패: {e}")
            return None
    
    def set_target_db(self, config_id: str) -> bool:
        """선택된 config_id를 Target DB로 설정"""
        try:
            # 모든 DB 설정에서 해당 config_id 찾기
            all_configs = self.get_all_db_configs()
            selected_config = None
            
            for config in all_configs:
                if config['config_id'] == config_id:
                    selected_config = config
                    break
            
            if not selected_config:
                self.logger.error(f"config_id를 찾을 수 없습니다: {config_id}")
                return False
            
            # Target DB 설정으로 저장 (JSON 파일에도 저장)
            target_config = {
                "host": selected_config['host'],
                "port": selected_config['port'],
                "service": selected_config['service_name'],
                "user": selected_config['username'],
                "password": selected_config.get('password', '')
            }
            
            # db_config.json 파일 업데이트
            import json
            import os
            
            config_file = "db_config.json"
            current_config = {}
            
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    current_config = json.load(f)
            
            current_config['target_db'] = target_config
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(current_config, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Target DB 설정 완료: {selected_config['display_name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Target DB 설정 실패: {e}")
            return False
    
    def set_repo_db(self, config_id: str) -> bool:
        """선택된 config_id를 Repo DB로 설정"""
        try:
            # 모든 DB 설정에서 해당 config_id 찾기
            all_configs = self.get_all_db_configs()
            selected_config = None
            
            for config in all_configs:
                if config['config_id'] == config_id:
                    selected_config = config
                    break
            
            if not selected_config:
                self.logger.error(f"config_id를 찾을 수 없습니다: {config_id}")
                return False
            
            # Repo DB 설정으로 저장 (JSON 파일에도 저장)
            repo_config = {
                "host": selected_config['host'],
                "port": selected_config['port'],
                "service": selected_config['service_name'],
                "user": selected_config['username'],
                "password": selected_config.get('password', '')
            }
            
            # db_config.json 파일 업데이트
            import json
            import os
            
            config_file = "db_config.json"
            current_config = {}
            
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    current_config = json.load(f)
            
            current_config['repo_db'] = repo_config
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(current_config, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Repo DB 설정 완료: {selected_config['display_name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Repo DB 설정 실패: {e}")
            return False
    
    # DB 접속 정보 CRUD 메서드
    def _check_name_host_duplicate(self, name: str, host: str, collection) -> bool:
        """
        name과 host 조합이 이미 존재하는지 확인
        
        Args:
            name: 설정 이름
            host: 호스트 주소
            collection: MongoDB 컬렉션 객체
        
        Returns:
            bool: 중복이 있으면 True, 없으면 False
        """
        # name과 host 조합으로 중복 확인
        existing = collection.find_one({"name": name, "host": host})
        return existing is not None
    
    def create_db_entry(self, entry_data: Dict[str, Any], collection_name: str = None) -> Optional[str]:
        """MongoDB에 DB 접속 정보를 개별 문서로 저장"""
        try:
            if not self.is_connected():
                return None
            
            # 필수 필드 검증
            if 'host' not in entry_data:
                self.logger.error("필수 필드 누락: host")
                return None
            
            if 'name' not in entry_data:
                self.logger.error("필수 필드 누락: name")
                return None
            
            # 컬렉션 선택 (기본값: qs1_db)
            if collection_name is None:
                collection_name = MONGODB_CONFIG['collections']['db_configs']
            
            collection = self.db[collection_name]
            
            # name과 host 조합 중복 체크
            name = entry_data.get('name', '')
            host = entry_data['host']
            if self._check_name_host_duplicate(name, host, collection):
                self.logger.error(f"중복된 name+host 조합: {name}_{host}")
                return None
            
            # UUID4로 고유 ID 생성
            entry_id = str(uuid.uuid4())
            
            # 새로운 문서 생성 (개별 문서로 저장)
            # POST 요청 형식 그대로 저장 (db_user, db_password, db_port, os_type)
            new_doc = {
                "id": entry_id,
                "name": entry_data.get('name', ''),
                "host": entry_data['host'],
                "database": entry_data.get('database', ''),
                "db_user": entry_data.get('db_user', entry_data.get('user', '')),  # db_user 우선, 없으면 user
                "db_password": entry_data.get('db_password', entry_data.get('password', '')),  # db_password 우선, 없으면 password
                "db_port": int(entry_data.get('db_port', entry_data.get('port', 1521))),  # db_port 우선, 없으면 port
                "db_type": entry_data.get('db_type', ''),
                "ssh_user": entry_data.get('ssh_user', ''),
                "ssh_password": entry_data.get('ssh_password', ''),
                "ssh_port": int(entry_data.get('ssh_port', 0)) if entry_data.get('ssh_port') else 0,
                "os_type": entry_data.get('os_type', entry_data.get('os', 'Unknown')),  # os_type 우선, 없으면 os
                "schema_name": entry_data.get('schema_name', ''),
                "service": entry_data.get('service', ''),
                "service_type": entry_data.get('service_type', ''),
                "dg_home": entry_data.get('dg_home', '/home/dg'),
                "description": entry_data.get('description', '')
            }
            
            # 개별 문서로 저장
            collection.insert_one(new_doc)
            
            self.logger.info(f"DB 접속 정보 저장 완료: {entry_id}")
            return entry_id
                
        except Exception as e:
            self.logger.error(f"DB 접속 정보 생성 실패: {e}")
            return None
    
    def update_db_entry(self, entry_id: str, entry_data: Dict[str, Any], collection_name: str = None) -> bool:
        """MongoDB에서 id 필드로 문서를 찾아 수정"""
        try:
            if not self.is_connected():
                return False
            
            # 컬렉션 선택
            if collection_name is None:
                collection_name = MONGODB_CONFIG['collections']['db_configs']
            
            collection = self.db[collection_name]
            
            # id 필드로 문서 찾기
            existing_doc = collection.find_one({"id": entry_id})
            
            if not existing_doc:
                self.logger.error(f"Entry not found: {entry_id}")
                return False
            
            # 업데이트할 데이터 준비 (id는 유지)
            # POST 요청 형식 그대로 저장 (db_user, db_password, db_port, os_type)
            update_data = {
                "id": entry_id,  # ID는 변경하지 않음
                "name": entry_data.get('name', existing_doc.get('name', '')),
                "host": entry_data.get('host', existing_doc.get('host', '')),
                "database": entry_data.get('database', existing_doc.get('database', '')),
                "db_user": entry_data.get('db_user', existing_doc.get('db_user', existing_doc.get('user', ''))),
                "db_password": entry_data.get('db_password', existing_doc.get('db_password', existing_doc.get('password', ''))),
                "db_port": int(entry_data.get('db_port', existing_doc.get('db_port', existing_doc.get('port', 1521)))),
                "db_type": entry_data.get('db_type', existing_doc.get('db_type', '')),
                "ssh_user": entry_data.get('ssh_user', existing_doc.get('ssh_user', '')),
                "ssh_password": entry_data.get('ssh_password', existing_doc.get('ssh_password', '')),
                "ssh_port": int(entry_data.get('ssh_port', existing_doc.get('ssh_port', 0))) if entry_data.get('ssh_port') or existing_doc.get('ssh_port') else 0,
                "os_type": entry_data.get('os_type', existing_doc.get('os_type', existing_doc.get('os', 'Unknown'))),
                "schema_name": entry_data.get('schema_name', existing_doc.get('schema_name', '')),
                "service": entry_data.get('service', existing_doc.get('service', '')),
                "service_type": entry_data.get('service_type', existing_doc.get('service_type', '')),
                "dg_home": entry_data.get('dg_home', existing_doc.get('dg_home', '/home/dg')),
                "description": entry_data.get('description', existing_doc.get('description', ''))
            }
            
            # 문서 업데이트
            collection.update_one(
                {"id": entry_id},
                {"$set": update_data}
            )
            
            self.logger.info(f"DB 접속 정보 수정 완료: {entry_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"DB 접속 정보 수정 실패: {e}")
            return False
    
    def delete_db_entry(self, entry_id: str, collection_name: str = None) -> bool:
        """MongoDB에서 id 필드로 문서를 찾아 삭제"""
        try:
            if not self.is_connected():
                return False
            
            # 컬렉션 선택
            if collection_name is None:
                collection_name = MONGODB_CONFIG['collections']['db_configs']
            
            collection = self.db[collection_name]
            
            # id 필드로 문서 찾아서 삭제
            result = collection.delete_one({"id": entry_id})
            
            if result.deleted_count == 0:
                self.logger.error(f"Entry not found: {entry_id}")
                return False
            
            self.logger.info(f"DB 접속 정보 삭제 완료: {entry_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"DB 접속 정보 삭제 실패: {e}")
            return False
    
    def update_connection_profile(self, profile_id: str, profile_data: Dict[str, Any]) -> bool:
        """연결 프로필 수정"""
        try:
            if not self.is_connected():
                return False
            
            # 기존 프로필 확인
            existing_profile = self.profiles_collection.find_one({
                "profile_id": profile_id,
                "is_active": True
            })
            
            if not existing_profile:
                self.logger.error(f"프로필을 찾을 수 없습니다: {profile_id}")
                return False
            
            # 업데이트할 데이터 준비
            update_data = {
                "updated_at": datetime.utcnow()
            }
            
            # 프로필 이름 업데이트
            if 'profile_name' in profile_data:
                update_data['profile_name'] = profile_data['profile_name']
            
            # 설명 업데이트
            if 'description' in profile_data:
                update_data['description'] = profile_data['description']
            
            # Repo 설정만 업데이트 (Target DB는 APM_DB_INFO에서 선택)
            if 'repo_config' in profile_data:
                repo_config = profile_data['repo_config']
                
                # 기존 repo_config_id가 있으면 업데이트, 없으면 새로 생성
                if 'repo_config_id' in existing_profile:
                    repo_config_id = existing_profile['repo_config_id']
                    
                    # repo_config_id가 "entry_X" 형식이면 MongoDB entries에서 업데이트
                    if repo_config_id.startswith("entry_"):
                        try:
                            entry_index = int(repo_config_id.split("_")[1])
                            # MongoDB entries 업데이트
                            self.update_db_entry(repo_config_id, repo_config)
                        except (ValueError, IndexError):
                            # 업데이트 실패 시 새로 생성
                            new_config_id = self.create_db_entry(repo_config)
                            if new_config_id:
                                update_data['repo_config_id'] = new_config_id
                    else:
                        # 기존 방식: db_configs 컬렉션에서 업데이트
                        repo_config_doc = {
                            "config_id": repo_config_id,
                            "config_type": "repo",
                            "name": f"repo_{profile_id}",
                            "host": repo_config.get('host', ''),
                            "port": int(repo_config.get('db_port', repo_config.get('port', 5432))),
                            "user": repo_config.get('db_user', repo_config.get('user', '')),
                            "password": repo_config.get('db_password', repo_config.get('password', '')),
                            "service": repo_config.get('service', ''),
                            "database": repo_config.get('database', ''),
                            "db_id": int(repo_config.get('db_id', 1)),
                            "description": repo_config.get('description', f"Repo config for profile {profile_id}"),
                            "is_active": True,
                            "created_at": existing_profile.get('created_at', datetime.utcnow()),
                            "updated_at": datetime.utcnow()
                        }
                        
                        self.db_configs_collection.replace_one(
                            {"config_id": repo_config_id},
                            repo_config_doc,
                            upsert=True
                        )
                else:
                    # 새로 생성
                    new_config_id = self.create_db_entry(repo_config)
                    if new_config_id:
                        update_data['repo_config_id'] = new_config_id
            
            # is_default 업데이트
            if 'is_default' in profile_data:
                if profile_data['is_default']:
                    # 기존 기본 프로필 해제
                    self.profiles_collection.update_many(
                        {"is_default": True, "profile_id": {"$ne": profile_id}},
                        {"$set": {"is_default": False, "updated_at": datetime.utcnow()}}
                    )
                update_data['is_default'] = profile_data['is_default']
            
            # 프로필 업데이트
            result = self.profiles_collection.update_one(
                {"profile_id": profile_id, "is_active": True},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                self.logger.info(f"연결 프로필 수정 완료: {profile_id}")
                return True
            else:
                self.logger.warning(f"프로필 수정 실패 (변경사항 없음): {profile_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"연결 프로필 수정 실패: {e}")
            return False

# 전역 서비스 인스턴스
db_config_service = DBConfigService()
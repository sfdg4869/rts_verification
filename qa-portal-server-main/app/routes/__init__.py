
# routes 패키지에서 모든 Blueprint를 관리
from .mongodb_config import mongodb_config_bp  # MongoDB 기반 DB 설정 관리 라우트
from .db_selector import db_selector_bp  # DB 선택기 라우트
from .db_selection import db_selection_bp  # DB 선택 API 라우트
from .rts_check_routes import bp as rts_check_bp  # RTS 프로세스 상태 점검 라우트

blueprints = [mongodb_config_bp, db_selector_bp, db_selection_bp, rts_check_bp]

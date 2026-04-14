"""
Oracle 데이터베이스 서비스
Oracle 연결 및 쿼리 실행 관련 비즈니스 로직을 담당합니다.
"""

import logging
import threading

import oracledb

# Thick 모드(Instant Client)는 app.create_app()에서 ORACLE_CLIENT_LIB_DIR 등으로 1회 초기화.
# Docker/Linux에서는 기본적으로 Thin 모드(TCP)로 연결합니다.

from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from app.services.dg_password_service import decrypt_dg_password

_logger = logging.getLogger(__name__)

# ── 커넥션 풀 매니저 ──────────────────────────────────────────────
_oracle_pools: Dict[tuple, oracledb.ConnectionPool] = {}
_pool_key_locks: Dict[tuple, threading.Lock] = {}
_pool_meta_lock = threading.Lock()  # _pool_key_locks dict 접근용

_POOL_MIN = 1
_POOL_MAX = 5
_POOL_INCREMENT = 1
_POOL_TIMEOUT = 30  # 유휴 커넥션 30초 후 반납


def _get_or_create_pool(dsn: str, user: str, password: str) -> oracledb.ConnectionPool:
    """DSN + user 조합으로 풀을 조회하거나 새로 생성합니다.
    서로 다른 DB 키는 독립적으로 병렬 생성됩니다.
    """
    key = (dsn, user)
    if key in _oracle_pools:
        return _oracle_pools[key]

    # 키별 개별 lock 확보 (다른 DB 키는 서로 블로킹하지 않음)
    with _pool_meta_lock:
        if key not in _pool_key_locks:
            _pool_key_locks[key] = threading.Lock()
        key_lock = _pool_key_locks[key]

    with key_lock:
        if key not in _oracle_pools:
            _logger.info(f"Oracle SessionPool 생성: {user}@{dsn}")
            pool = oracledb.create_pool(
                user=user,
                password=password,
                dsn=dsn,
                min=_POOL_MIN,
                max=_POOL_MAX,
                increment=_POOL_INCREMENT,
                timeout=_POOL_TIMEOUT,
                getmode=oracledb.POOL_GETMODE_WAIT,
            )
            _oracle_pools[key] = pool
    return _oracle_pools[key]


class OracleService:
    """Oracle 데이터베이스 서비스 클래스"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Oracle 서비스 초기화
        
        Args:
            config: Oracle 연결 설정
                - host: 호스트 주소
                - port: 포트 번호 (기본값: 1521)
                - service_name 또는 sid: 서비스명 또는 SID
                - user 또는 db_user: 사용자명
                - password 또는 db_password: 비밀번호
        """
        self.config = config
    
    def _build_dsn(self) -> str:
        """DSN 생성 — type/service_type/serviceType + service/service_name/sid/database 키 모두 지원"""
        host = self.config.get('host', 'localhost')
        port = int(self.config.get('port', self.config.get('db_port', 1521)) or 1521)

        # service type 키: type, service_type, serviceType 모두 허용
        raw_type = (
            self.config.get('type')
            or self.config.get('service_type')
            or self.config.get('serviceType')
        )
        service_type = (raw_type or '').lower().strip() or None

        # service value 키: service, service_name, serviceName, database, sid 우선순위
        service_value = (
            self.config.get('service')
            or self.config.get('service_name')
            or self.config.get('serviceName')
            or self.config.get('database')
        )
        sid_value = self.config.get('sid')

        if service_type == 'sid':
            identifier = sid_value or service_value
            if not identifier:
                raise ValueError("sid 또는 service 값이 필요합니다.")
            return oracledb.makedsn(host, port, sid=identifier)

        if service_type == 'service_name' or service_type == 'service':
            if not service_value:
                raise ValueError("service_name 또는 service 값이 필요합니다.")
            return oracledb.makedsn(host, port, service_name=service_value)

        # service_type 미지정: service_name 우선, 없으면 sid
        if service_value:
            return oracledb.makedsn(host, port, service_name=service_value)

        if sid_value:
            return oracledb.makedsn(host, port, sid=sid_value)

        raise ValueError("service_name/service/sid/database 중 하나는 필수입니다.")
    
    def _get_credentials(self):
        """DSN, user, password 튜플 반환"""
        dsn = self._build_dsn()
        user = self.config.get('user') or self.config.get('db_user') or self.config.get('username') or ''
        raw_password = self.config.get('password') or self.config.get('db_password') or ''
        password = decrypt_dg_password(raw_password)
        return dsn, user, password

    def connect(self) -> Optional[oracledb.Connection]:
        """
        Oracle 데이터베이스 연결 (커넥션 풀에서 acquire).

        Returns:
            oracledb.Connection: 풀에서 꺼낸 커넥션 객체
            None: 연결 실패시
        """
        try:
            dsn, user, password = self._get_credentials()
            pool = _get_or_create_pool(dsn, user, password)
            return pool.acquire()
        except Exception as e:
            _logger.error(f"Oracle 연결 실패: {e}")
            return None

    def connect_or_raise(self) -> oracledb.Connection:
        """풀에서 커넥션을 가져오며, 실패 시 예외를 그대로 올립니다(진단·API 오류 메시지용)."""
        dsn, user, password = self._get_credentials()
        pool = _get_or_create_pool(dsn, user, password)
        return pool.acquire()

    def release_connection(self, conn: oracledb.Connection) -> None:
        """
        커넥션을 풀에 반납합니다. close() 대신 이 메서드를 사용하세요.

        Args:
            conn: acquire()로 얻은 커넥션 객체
        """
        if not conn:
            return
        try:
            dsn, user, _ = self._get_credentials()
            key = (dsn, user)
            pool = _oracle_pools.get(key)
            if pool:
                pool.release(conn)
            else:
                conn.close()
        except Exception as e:
            _logger.warning(f"Oracle 커넥션 반납 실패, close() 시도: {e}")
            try:
                conn.close()
            except Exception:
                pass
    
    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """
        Oracle 연결 테스트
        
        Returns:
            Tuple[bool, str, Optional[str]]: (성공여부, 메시지, 버전정보)
        """
        conn = self.connect()
        if not conn:
            return False, "Oracle 연결 실패", None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'")
            version_result = cursor.fetchone()
            version = version_result[0] if version_result else None
            cursor.close()
            return True, "Oracle 연결 성공", version
        except Exception as e:
            return False, f"연결 테스트 중 오류 발생: {str(e)}", None
        finally:
            self.release_connection(conn)
    
    def execute_query(self, query: str, params: List[Any] = None) -> Tuple[bool, Any, str]:
        """
        Oracle 쿼리 실행
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터
            
        Returns:
            Tuple[bool, Any, str]: (성공여부, 결과데이터, 에러메시지)
        """
        if params is None:
            params = []
            
        start_time = datetime.now()
        conn = self.connect()
        if not conn:
            return False, None, "Oracle 연결 실패"
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)

            if query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                data_list = [dict(zip(columns, row)) for row in rows]
                row_count = len(data_list)
            else:
                conn.commit()
                data_list = []
                row_count = cursor.rowcount

            cursor.close()
            end_time = datetime.now()
            return True, {
                'data': data_list,
                'row_count': row_count,
                'execution_time': (end_time - start_time).total_seconds(),
                'timestamp': end_time.isoformat()
            }, ""
        except Exception as e:
            return False, None, str(e)
        finally:
            self.release_connection(conn)
    
    def get_apm_db_info(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        APM_DB_INFO 테이블에서 데이터베이스 정보 조회
        
        Returns:
            Tuple[bool, List[Dict], str]: (성공여부, 데이터목록, 에러메시지)
        """
        conn = self.connect()
        if not conn:
            return False, [], "Oracle 연결 실패"
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT db_id, instance_name, host_ip, host_name, host_id,
                       db_user, db_password, sid, lsnr_ip, lsnr_port, os_type, oracle_version
                FROM apm_db_info
                ORDER BY db_id ASC
            """)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            info = []
            for row in rows:
                info.append({
                    'db_id': row[0],
                    'instance_name': row[1],
                    'host_ip': row[2],
                    'host_name': row[3],
                    'host_id': row[4],
                    'db_user': row[5],
                    'db_password': row[6],
                    'db_password_encrypted': row[6],
                    'sid': row[7],
                    'lsnr_ip': row[8],
                    'lsnr_port': row[9],
                    'os_type': row[10],
                    'oracle_version': row[11]
                })
            cursor.close()
            return True, info, ""
        except Exception as e:
            return False, [], str(e)
        finally:
            self.release_connection(conn)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        현재 Oracle 연결 정보 조회 (비밀번호 제외)
        
        Returns:
            Dict[str, Any]: 연결 정보
        """
        return {
            'host': self.config.get('host'),
            'port': self.config.get('port', self.config.get('db_port', 1521)),
            'service_name': self.config.get('service_name') or self.config.get('service'),
            'sid': self.config.get('sid'),
            'user': self.config.get('user') or self.config.get('db_user'),
            'password': '***'  # 보안상 비밀번호는 숨김
        }


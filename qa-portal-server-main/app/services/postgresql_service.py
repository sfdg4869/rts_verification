"""
PostgreSQL 데이터베이스 서비스
PostgreSQL 연결 및 쿼리 실행 관련 비즈니스 로직을 담당합니다.
"""

import logging
import threading

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from app.services.dg_password_service import decrypt_dg_password

_logger = logging.getLogger(__name__)

# ── 커넥션 풀 매니저 (oracle_service.py 패턴과 동일) ─────────────────
_pg_pools: Dict[tuple, psycopg2.pool.ThreadedConnectionPool] = {}
_pg_pool_meta_lock = threading.Lock()

_PG_POOL_MIN = 1
_PG_POOL_MAX = 5


def _get_or_create_pg_pool(
    host: str, port: int, database: str, user: str, password: str
) -> psycopg2.pool.ThreadedConnectionPool:
    """host+port+database+user 조합으로 풀을 조회하거나 새로 생성합니다."""
    key = (host, int(port), database, user)
    if key in _pg_pools:
        return _pg_pools[key]

    with _pg_pool_meta_lock:
        if key not in _pg_pools:
            _logger.info(f"PostgreSQL ThreadedConnectionPool 생성: {user}@{host}:{port}/{database}")
            _pg_pools[key] = psycopg2.pool.ThreadedConnectionPool(
                minconn=_PG_POOL_MIN,
                maxconn=_PG_POOL_MAX,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                sslmode='disable',
            )
    return _pg_pools[key]


class PostgreSQLService:
    """PostgreSQL 데이터베이스 서비스 클래스"""

    def __init__(self, config: Dict[str, Any]):
        """
        PostgreSQL 서비스 초기화

        Args:
            config: PostgreSQL 연결 설정
        """
        self.config = config

    def _get_pool(self) -> psycopg2.pool.ThreadedConnectionPool:
        """설정 기반으로 커넥션 풀을 가져옵니다."""
        raw_password = self.config.get('password', '')
        password = decrypt_dg_password(raw_password)
        return _get_or_create_pg_pool(
            host=self.config['host'],
            port=int(self.config['port']),
            database=self.config['database'],
            user=self.config['user'],
            password=password,
        )

    def connect(self) -> Optional[psycopg2.extensions.connection]:
        """
        커넥션 풀에서 PostgreSQL 연결을 획득합니다.
        사용 후 반드시 release_connection()을 호출해 풀에 반납해야 합니다.

        Returns:
            psycopg2.connection: 풀에서 획득한 연결 객체
            None: 획득 실패 시
        """
        try:
            pool = self._get_pool()
            return pool.getconn()
        except Exception as e:
            _logger.error(f"PostgreSQL 연결 획득 실패: {e}")
            return None

    def release_connection(self, conn: psycopg2.extensions.connection) -> None:
        """
        연결을 풀에 반납합니다. connect() 대응 쌍으로 반드시 호출해야 합니다.
        conn.close() 대신 이 메서드를 사용하세요.

        Args:
            conn: connect()로 획득한 연결 객체
        """
        if not conn:
            return
        try:
            pool = self._get_pool()
            pool.putconn(conn)
        except Exception as e:
            _logger.warning(f"PostgreSQL 연결 반납 실패, close() 시도: {e}")
            try:
                conn.close()
            except Exception:
                pass

    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """
        PostgreSQL 연결 테스트

        Returns:
            Tuple[bool, str, Optional[str]]: (성공여부, 메시지, 버전정보)
        """
        conn = self.connect()
        if not conn:
            return False, "PostgreSQL 연결 실패", None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            return True, "PostgreSQL 연결 성공", version
        except Exception as e:
            return False, f"연결 테스트 중 오류 발생: {str(e)}", None
        finally:
            self.release_connection(conn)

    def execute_query(self, query: str, params: List[Any] = None) -> Tuple[bool, Any, str]:
        """
        PostgreSQL 쿼리 실행

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
            return False, None, "PostgreSQL 연결 실패"
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params)

            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                data_list = [dict(row) for row in results]
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
            return False, [], "PostgreSQL 연결 실패"
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT db_id, instance_name, host_ip, host_name, host_id,
                       db_user, db_password, sid, lsnr_ip, lsnr_port, os_type, oracle_version
                FROM apm_db_info
                ORDER BY db_id ASC;
            """)
            info = []
            for row in cursor.fetchall():
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
        현재 PostgreSQL 연결 정보 조회 (비밀번호 제외)

        Returns:
            Dict[str, Any]: 연결 정보
        """
        return {
            'host': self.config['host'],
            'port': self.config['port'],
            'database': self.config['database'],
            'user': self.config['user'],
            'password': '***'  # 보안상 비밀번호는 숨김
        }

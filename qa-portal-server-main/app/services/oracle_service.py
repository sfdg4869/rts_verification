"""
Oracle database service helpers.
"""

from datetime import datetime
import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

import oracledb

from app.services.dg_password_service import decrypt_dg_password

_logger = logging.getLogger(__name__)

_oracle_pools: Dict[tuple, oracledb.ConnectionPool] = {}
_pool_key_locks: Dict[tuple, threading.Lock] = {}
_pool_meta_lock = threading.Lock()
_POOL_MIN = 1
_POOL_MAX = 5
_POOL_INCREMENT = 1
_POOL_TIMEOUT = 30


def _friendly_oracle_error(exc: Exception) -> str:
    message = str(exc)
    if "DPY-3010" in message:
        return (
            f"{message}\n"
            "This Oracle server version cannot be used with python-oracledb thin mode. "
            "Run the app in Oracle thick mode by staging an Oracle Instant Client zip on the server, "
            "setting ORACLE_CLIENT_ZIP_PATH for deploy.sh, rebuilding the container image, "
            "and keeping ORACLE_CLIENT_LIB_DIR set."
        )
    return message


def _get_or_create_pool(dsn: str, user: str, password: str) -> oracledb.ConnectionPool:
    key = (dsn, user)
    if key in _oracle_pools:
        return _oracle_pools[key]

    with _pool_meta_lock:
        if key not in _pool_key_locks:
            _pool_key_locks[key] = threading.Lock()
        key_lock = _pool_key_locks[key]

    with key_lock:
        if key not in _oracle_pools:
            _logger.info("Oracle SessionPool created: %s@%s", user, dsn)
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
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def _build_dsn(self) -> str:
        host = self.config.get("host", "localhost")
        port = int(self.config.get("port", self.config.get("db_port", 1521)) or 1521)

        raw_type = (
            self.config.get("type")
            or self.config.get("service_type")
            or self.config.get("serviceType")
        )
        service_type = (raw_type or "").lower().strip() or None

        service_value = (
            self.config.get("service")
            or self.config.get("service_name")
            or self.config.get("serviceName")
            or self.config.get("database")
        )
        sid_value = self.config.get("sid")

        if service_type == "sid":
            identifier = sid_value or service_value
            if not identifier:
                raise ValueError("sid or service is required")
            return oracledb.makedsn(host, port, sid=identifier)

        if service_type in ("service_name", "service"):
            if not service_value:
                raise ValueError("service_name or service is required")
            return oracledb.makedsn(host, port, service_name=service_value)

        if service_value:
            return oracledb.makedsn(host, port, service_name=service_value)

        if sid_value:
            return oracledb.makedsn(host, port, sid=sid_value)

        raise ValueError("service_name/service/sid/database is required")

    def _get_credentials(self) -> Tuple[str, str, str]:
        dsn = self._build_dsn()
        user = self.config.get("user") or self.config.get("db_user") or self.config.get("username") or ""
        raw_password = self.config.get("password") or self.config.get("db_password") or ""
        password = decrypt_dg_password(raw_password)
        return dsn, user, password

    def connect(self) -> Optional[oracledb.Connection]:
        try:
            dsn, user, password = self._get_credentials()
            pool = _get_or_create_pool(dsn, user, password)
            return pool.acquire()
        except Exception as exc:
            _logger.error("Oracle connect failed: %s", _friendly_oracle_error(exc))
            return None

    def connect_or_raise(self) -> oracledb.Connection:
        try:
            dsn, user, password = self._get_credentials()
            pool = _get_or_create_pool(dsn, user, password)
            return pool.acquire()
        except Exception as exc:
            raise RuntimeError(_friendly_oracle_error(exc)) from exc

    def release_connection(self, conn: oracledb.Connection) -> None:
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
        except Exception as exc:
            _logger.warning("Oracle release failed, closing directly: %s", exc)
            try:
                conn.close()
            except Exception:
                pass

    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
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
        except Exception as exc:
            return False, f"연결 테스트 중 오류 발생: {exc}", None
        finally:
            self.release_connection(conn)

    def execute_query(self, query: str, params: List[Any] = None) -> Tuple[bool, Any, str]:
        if params is None:
            params = []

        start_time = datetime.now()
        conn = self.connect()
        if not conn:
            return False, None, "Oracle 연결 실패"
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)

            if query.strip().upper().startswith("SELECT"):
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
                "data": data_list,
                "row_count": row_count,
                "execution_time": (end_time - start_time).total_seconds(),
                "timestamp": end_time.isoformat(),
            }, ""
        except Exception as exc:
            return False, None, str(exc)
        finally:
            self.release_connection(conn)

    def get_apm_db_info(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        conn = self.connect()
        if not conn:
            return False, [], "Oracle 연결 실패"
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT db_id, instance_name, host_ip, host_name, host_id,
                       db_user, db_password, sid, lsnr_ip, lsnr_port, os_type, oracle_version
                FROM apm_db_info
                ORDER BY db_id ASC
                """
            )
            rows = cursor.fetchall()
            info = []
            for row in rows:
                info.append(
                    {
                        "db_id": row[0],
                        "instance_name": row[1],
                        "host_ip": row[2],
                        "host_name": row[3],
                        "host_id": row[4],
                        "db_user": row[5],
                        "db_password": row[6],
                        "db_password_encrypted": row[6],
                        "sid": row[7],
                        "lsnr_ip": row[8],
                        "lsnr_port": row[9],
                        "os_type": row[10],
                        "oracle_version": row[11],
                    }
                )
            cursor.close()
            return True, info, ""
        except Exception as exc:
            return False, [], str(exc)
        finally:
            self.release_connection(conn)

    def get_connection_info(self) -> Dict[str, Any]:
        return {
            "host": self.config.get("host"),
            "port": self.config.get("port", self.config.get("db_port", 1521)),
            "service_name": self.config.get("service_name") or self.config.get("service"),
            "sid": self.config.get("sid"),
            "user": self.config.get("user") or self.config.get("db_user"),
            "password": "***",
        }

# rts_compare.py 변경 사항 정리

## 📋 변경 개요
**목적**: UNION ALL 쿼리의 성능 문제를 해결하기 위해 병렬 처리 방식으로 변경

---

## 🔄 주요 변경 사항

### 1️⃣ Import 추가

#### ❌ 변경 전
```python
from flask import Blueprint, jsonify, request, Response
import oracledb
import os
import time
import threading
import json
from datetime import datetime
```

#### ✅ 변경 후
```python
from flask import Blueprint, jsonify, request, Response
import oracledb
import os
import time
import threading
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed  # ✨ 추가
```

---

### 2️⃣ 쿼리 구조 변경

#### ❌ 변경 전: 단일 UNION ALL 쿼리
```python
# Repo DB 쿼리 (최신 데이터 조회)
repo_query = """
SELECT * FROM (
    SELECT time, stat_name AS name, sigma_value FROM (
        SELECT b.stat_name, a.time, a.sigma_value FROM ora_db_stat_temp a, ora_stat_name b
        WHERE a.db_id = b.db_id AND a.stat_id = b.stat_id AND a.STAT_VERSION = b.STAT_VERSION 
        AND a.db_id = :db_id 
        AND b.stat_name IN (
        'session logical reads', 'DB time',
        'physical read bytes',
        'physical read total bytes', 
        'CPU used by this session',
        'redo synch long waits',
        'total sessions', 'active user session'
        ) ORDER BY a.time DESC
    ) WHERE rownum < 9
    UNION ALL
    SELECT time, event_name AS name, sigma_wait_time AS sigma_value FROM (
        SELECT b.event_name, a.time, a.sigma_wait_time FROM ora_db_wait_temp a, ora_event_name b
        WHERE a.db_id = b.db_id AND a.event_id = b.event_id AND a.EVENT_VERSION = b.EVENT_VERSION 
        AND a.db_id = :db_id 
        AND b.event_name IN (
        'enq: TX - row lock contention',
        'enq: TM - contention'
        ) ORDER BY a.time DESC
    ) WHERE rownum < 3
)
ORDER BY decode(name, 
    'session logical reads', 1,
    'DB time', 2, 
    'physical read bytes', 3,
    'physical read total bytes', 4, 
    'CPU used by this session', 5,
    'redo synch long waits', 6,
    'total sessions', 7, 
    'active user session', 8,     
    'enq: TX - row lock contention', 9,
    'enq: TM - contention', 10) ASC
"""
```

#### ✅ 변경 후: 지표 분리 + 병렬 처리
```python
# 지표 리스트 정의
STAT_METRICS = [
    'session logical reads', 'DB time',
    'physical read bytes',
    'physical read total bytes', 
    'CPU used by this session',
    'redo synch long waits',
    'total sessions', 'active user session'
]

EVENT_METRICS = [
    'enq: TX - row lock contention',
    'enq: TM - contention'
]

# 정렬 순서 정의
METRIC_ORDER = {
    'session logical reads': 1,
    'DB time': 2, 
    'physical read bytes': 3,
    'physical read total bytes': 4, 
    'CPU used by this session': 5,
    'redo synch long waits': 6,
    'total sessions': 7, 
    'active user session': 8,     
    'enq: TX - row lock contention': 9,
    'enq: TM - contention': 10
}

# 단일 stat 지표 조회 함수
def _fetch_stat_metric(db_id, stat_name):
    """단일 stat 지표 조회 (각 스레드에서 독립적인 연결 사용)"""
    query = """
    SELECT time, stat_name AS name, sigma_value FROM (
        SELECT b.stat_name, a.time, a.sigma_value FROM ora_db_stat_temp a, ora_stat_name b
        WHERE a.db_id = b.db_id AND a.stat_id = b.stat_id AND a.STAT_VERSION = b.STAT_VERSION 
        AND a.db_id = :db_id 
        AND b.stat_name = :stat_name
        ORDER BY a.time DESC
    ) WHERE rownum = 1
    """
    try:
        with _connect_repo() as conn:
            with conn.cursor() as cur:
                cur.execute(query, db_id=db_id, stat_name=stat_name)
                columns = [col[0] for col in cur.description]
                rows = cur.fetchall()
                if rows:
                    return [dict(zip(columns, row)) for row in rows]
                return []
    except Exception as e:
        return []

# 단일 event 지표 조회 함수
def _fetch_event_metric(db_id, event_name):
    """단일 event 지표 조회 (각 스레드에서 독립적인 연결 사용)"""
    query = """
    SELECT time, event_name AS name, sigma_wait_time AS sigma_value FROM (
        SELECT b.event_name, a.time, a.sigma_wait_time FROM ora_db_wait_temp a, ora_event_name b
        WHERE a.db_id = b.db_id AND a.event_id = b.event_id AND a.EVENT_VERSION = b.EVENT_VERSION 
        AND a.db_id = :db_id 
        AND b.event_name = :event_name
        ORDER BY a.time DESC
    ) WHERE rownum = 1
    """
    try:
        with _connect_repo() as conn:
            with conn.cursor() as cur:
                cur.execute(query, db_id=db_id, event_name=event_name)
                columns = [col[0] for col in cur.description]
                rows = cur.fetchall()
                if rows:
                    return [dict(zip(columns, row)) for row in rows]
                return []
    except Exception as e:
        return []

# 병렬 처리 함수
def _fetch_repo_data_parallel(db_id):
    """병렬 처리로 Repo DB 데이터 조회"""
    results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Stat 지표 병렬 조회 (8개)
        stat_futures = {
            executor.submit(_fetch_stat_metric, db_id, stat_name): stat_name 
            for stat_name in STAT_METRICS
        }
        
        # Event 지표 병렬 조회 (2개)
        event_futures = {
            executor.submit(_fetch_event_metric, db_id, event_name): event_name 
            for event_name in EVENT_METRICS
        }
        
        # 모든 결과 수집
        all_futures = {**stat_futures, **event_futures}
        
        for future in as_completed(all_futures):
            metric_name = all_futures[future]
            try:
                data = future.result()
                if data:
                    results.extend(data)
            except Exception as e:
                # 개별 지표 실패는 무시하고 계속 진행
                pass
    
    # 정렬 (METRIC_ORDER 기준)
    results.sort(key=lambda x: METRIC_ORDER.get(x.get('NAME') or x.get('name'), 999))
    
    return results
```

---

### 3️⃣ 사용처 변경

#### ❌ 변경 전: stream_compare_db_data() 함수
```python
# Repo DB 데이터 수집
try:
    with _connect_repo() as conn:
        with conn.cursor() as cur:
            cur.execute(repo_query, db_id=db_id)  # 단일 쿼리 실행
            columns = [col[0] for col in cur.description]
            repo_data = [dict(zip(columns, row)) for row in cur.fetchall()]
except Exception as e:
    yield f"data: {json.dumps({'type': 'error', 'seq': seq, 'error': f'Repo DB 오류: {str(e)}'})}\n\n"
```

#### ✅ 변경 후: stream_compare_db_data() 함수
```python
# Repo DB 데이터 수집 (병렬 처리)
try:
    repo_data = _fetch_repo_data_parallel(db_id)  # 병렬 처리 함수 호출
except Exception as e:
    yield f"data: {json.dumps({'type': 'error', 'seq': seq, 'error': f'Repo DB 오류: {str(e)}'})}\n\n"
```

---

#### ❌ 변경 전: collect_repo_data() 함수
```python
def collect_repo_data(seq):
    """Repo DB 데이터 수집 함수"""
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with _connect_repo() as conn:
            with conn.cursor() as cur:
                cur.execute(repo_query, db_id=db_id)  # 단일 쿼리 실행
                columns = [col[0] for col in cur.description]
                data = [dict(zip(columns, row)) for row in cur.fetchall()]
                repo_collections.append({
                    "collection_seq": seq,
                    "timestamp": timestamp,
                    "data": data
                })
    except Exception as e:
        collection_errors.append(f"Repo DB collection {seq} failed: {e}")
```

#### ✅ 변경 후: collect_repo_data() 함수
```python
def collect_repo_data(seq):
    """Repo DB 데이터 수집 함수 (병렬 처리)"""
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = _fetch_repo_data_parallel(db_id)  # 병렬 처리 함수 호출
        repo_collections.append({
            "collection_seq": seq,
            "timestamp": timestamp,
            "data": data
        })
    except Exception as e:
        collection_errors.append(f"Repo DB collection {seq} failed: {e}")
```

---

## 📊 성능 비교

| 항목 | 변경 전 | 변경 후 |
|------|---------|---------|
| **쿼리 방식** | 단일 UNION ALL 쿼리 | 10개 개별 쿼리 병렬 실행 |
| **처리 방식** | 순차 처리 | 병렬 처리 (ThreadPoolExecutor) |
| **DB 연결** | 1개 연결 | 각 지표마다 독립적인 연결 (10개) |
| **예상 성능** | 느림 (UNION ALL 비용) | 빠름 (병렬 처리) |

---

## 🎯 핵심 개선 사항

1. **병렬 처리 도입**: 10개 지표를 동시에 조회하여 전체 시간 단축
2. **독립적인 연결**: 각 스레드가 별도의 DB 연결을 사용하여 안정성 향상
3. **에러 처리**: 개별 지표 실패 시에도 다른 지표는 정상 조회
4. **코드 구조화**: 지표 리스트와 정렬 순서를 상수로 분리하여 유지보수성 향상

---

## 📝 변경된 함수 목록

- ✅ `_fetch_stat_metric()` - 새로 추가
- ✅ `_fetch_event_metric()` - 새로 추가  
- ✅ `_fetch_repo_data_parallel()` - 새로 추가
- 🔄 `stream_compare_db_data()` - 수정됨
- 🔄 `collect_repo_data()` - 수정됨
- ❌ `repo_query` - 삭제됨



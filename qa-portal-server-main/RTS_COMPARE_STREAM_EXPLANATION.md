# stream_compare_db_data() 함수 동작 원리 설명

## 📋 개요
이 함수는 **Server-Sent Events (SSE)** 방식을 사용하여 30초 동안 실시간으로 Target DB와 Repo DB의 성능 데이터를 비교하고 스트리밍하는 API입니다.

---

## 🔄 전체 동작 흐름

```
1. API 요청 수신
   ↓
2. DB 설정 확인 (Target DB, Repo DB)
   ↓
3. 파라미터 검증 (db_id, threshold, interval)
   ↓
4. SSE 스트림 시작 (generate() 함수 실행)
   ↓
5. 30초간 반복 (매 1초마다):
   ├─ Target DB에서 실시간 성능 데이터 조회
   ├─ Repo DB에서 저장된 성능 데이터 조회 (병렬 처리)
   ├─ 두 데이터 비교 및 정합성 검증
   └─ SSE로 실시간 전송
   ↓
6. 완료 메시지 전송
```

---

## 📊 단계별 상세 설명

### 1️⃣ 초기화 및 검증 (69-83줄)

```python
# DB 설정 확인
if not is_target_db_configured():
    return jsonify({"error": "Target DB 설정이 필요합니다..."}), 400

if not is_repo_db_configured():
    return jsonify({"error": "Repo DB 설정이 필요합니다..."}), 400

# 파라미터 추출
db_id = request.args.get("db_id", type=int)      # 비교할 DB ID
threshold = request.args.get("threshold", default=10, type=int)  # 허용 오차
interval = request.args.get("interval", default=1, type=int)    # 조회 간격(초)
```

**역할**: 
- Target DB와 Repo DB가 설정되어 있는지 확인
- 필수 파라미터 `db_id` 검증
- 선택적 파라미터 설정 (기본값: threshold=10, interval=1초)

---

### 2️⃣ SSE 스트림 생성기 함수 (85-190줄)

#### 2-1. 시작 메시지 전송 (87-90줄)

```python
start_time = datetime.now()
yield f"data: {json.dumps({
    'type': 'start', 
    'message': f'30초 실시간 모니터링 시작 (DB ID: {db_id})',
    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S')
})}\n\n"
```

**역할**: 클라이언트에게 스트리밍 시작을 알림

---

#### 2-2. 30초간 반복 수집 (96-172줄)

```python
for seq in range(1, 31):  # 1부터 30까지 (30초)
    collection_start = time.time()
    
    # Target DB 데이터 수집
    # Repo DB 데이터 수집
    # 데이터 비교
    # 결과 전송
```

**주요 동작**:

##### A. Target DB 실시간 데이터 수집 (104-112줄)

```python
with _connect_target() as conn:
    with conn.cursor() as cur:
        cur.execute(target_query)  # Oracle 실시간 성능 뷰 조회
        columns = [col[0] for col in cur.description]
        target_data = [dict(zip(columns, row)) for row in cur.fetchall()]
```

**조회하는 데이터**:
- `v$sysstat`: 시스템 통계 (session logical reads, DB time, physical read bytes 등)
- `v$session`: 세션 정보 (total sessions, active user session)
- `V$SYSTEM_EVENT`: 시스템 이벤트 (row lock contention, TM contention)

**특징**: 
- **실시간 데이터**: Oracle의 동적 성능 뷰에서 직접 조회
- `SYSDATE`로 현재 시각 기준 데이터

---

##### B. Repo DB 저장된 데이터 수집 (114-118줄)

```python
repo_data = _fetch_repo_data_parallel(db_id)
```

**내부 동작** (`_fetch_repo_data_parallel()` 함수):

1. **병렬 처리 시작** (ThreadPoolExecutor, 최대 10개 워커)
   ```python
   with ThreadPoolExecutor(max_workers=10) as executor:
   ```

2. **Stat 지표 병렬 조회** (8개 지표)
   ```python
   stat_futures = {
       executor.submit(_fetch_stat_metric, db_id, stat_name): stat_name 
       for stat_name in STAT_METRICS
   }
   ```
   - 각 지표마다 별도 스레드에서 `_fetch_stat_metric()` 실행
   - `ora_db_stat_temp`와 `ora_stat_name` 테이블 조인
   - 최신 데이터 1건만 조회 (`rownum = 1`)

3. **Event 지표 병렬 조회** (2개 지표)
   ```python
   event_futures = {
       executor.submit(_fetch_event_metric, db_id, event_name): event_name 
       for event_name in EVENT_METRICS
   }
   ```
   - 각 이벤트마다 별도 스레드에서 `_fetch_event_metric()` 실행
   - `ora_db_wait_temp`와 `ora_event_name` 테이블 조인
   - 최신 데이터 1건만 조회 (`rownum = 1`)

4. **결과 수집 및 정렬**
   ```python
   for future in as_completed(all_futures):
       data = future.result()
       if data:
           results.extend(data)
   
   # METRIC_ORDER 기준으로 정렬
   results.sort(key=lambda x: METRIC_ORDER.get(x.get('NAME') or x.get('name'), 999))
   ```

**특징**:
- **저장된 데이터**: 이전에 수집되어 저장된 성능 데이터
- **병렬 처리**: 10개 지표를 동시에 조회하여 속도 향상
- **최신 데이터**: `ORDER BY a.time DESC`로 가장 최근 데이터만 조회

---

##### C. 데이터 비교 및 정합성 검증 (120-143줄)

```python
if target_data and repo_data:
    # 딕셔너리로 변환 (지표명을 키로 사용)
    target_dict = {item['NAME']: item['SIGMA_VALUE'] for item in target_data}
    repo_dict = {item['NAME']: item['SIGMA_VALUE'] for item in repo_data}
    
    # 각 지표별 비교
    for name in target_dict.keys():
        if name in repo_dict:
            target_val = float(target_dict[name])
            repo_val = float(repo_dict[name])
            diff = abs(target_val - repo_val)
            consistent = diff <= threshold  # 허용 오차 이내인지 확인
            
            comparison_results.append({
                "name": name,
                "target_value": target_val,
                "repo_value": repo_val,
                "diff": diff,
                "consistent": consistent
            })
```

**비교 로직**:
1. 두 데이터를 지표명(`NAME`)을 키로 하는 딕셔너리로 변환
2. 각 지표별로 값 차이(`diff`) 계산
3. `threshold`(기본 10) 이내면 `consistent=True`, 아니면 `False`

**비교하는 지표** (총 10개):
1. session logical reads
2. DB time
3. physical read bytes
4. physical read total bytes
5. CPU used by this session
6. redo synch long waits
7. total sessions
8. active user session
9. enq: TX - row lock contention
10. enq: TM - contention

---

##### D. 실시간 데이터 전송 (147-163줄)

```python
progress_data = {
    "type": "progress",
    "seq": seq,                    # 수집 순번 (1-30)
    "timestamp": timestamp,        # 수집 시각
    "target_data": target_data,    # Target DB 원본 데이터
    "repo_data": repo_data,        # Repo DB 원본 데이터
    "comparison": comparison_results,  # 비교 결과
    "collection_stats": {
        "total": collection_count,
        "success": success_count,
        "error": error_count
    },
    "elapsed_time": f"{time.time() - collection_start:.2f}s"  # 수집 소요 시간
}

yield f"data: {json.dumps(progress_data, ensure_ascii=False, default=str)}\n\n"
```

**SSE 형식**:
- `data: {JSON}\n\n` 형식으로 전송
- 클라이언트는 EventSource API로 실시간 수신 가능

---

##### E. 다음 수집까지 대기 (169-172줄)

```python
elapsed = time.time() - collection_start
if elapsed < interval:
    time.sleep(interval - elapsed)  # 정확히 1초 간격 유지
```

**역할**: 수집 소요 시간을 고려하여 정확한 간격(기본 1초) 유지

---

#### 2-3. 완료 메시지 전송 (174-190줄)

```python
completion_data = {
    "type": "complete",
    "message": "30초 모니터링 완료",
    "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
    "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
    "duration": f"{duration:.2f}초",
    "total_collections": collection_count,
    "success_collections": success_count,
    "error_collections": error_count,
    "success_rate": f"{(success_count/collection_count)*100:.1f}%"
}

yield f"data: {json.dumps(completion_data, ensure_ascii=False)}\n\n"
```

**역할**: 30초 모니터링 완료 후 통계 정보 전송

---

### 3️⃣ SSE Response 반환 (192-201줄)

```python
return Response(
    generate(),
    mimetype='text/event-stream',
    headers={
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Cache-Control'
    }
)
```

**역할**: 
- SSE 형식으로 응답 설정
- CORS 헤더 추가 (크로스 오리진 요청 허용)
- 캐시 비활성화 (실시간 데이터)

---

## 📈 성능 데이터 수집 방식 비교

| 구분 | Target DB | Repo DB |
|------|-----------|---------|
| **데이터 소스** | Oracle 동적 성능 뷰 (`v$sysstat`, `v$session`, `V$SYSTEM_EVENT`) | 저장된 테이블 (`ora_db_stat_temp`, `ora_db_wait_temp`) |
| **데이터 특성** | 실시간 현재 값 | 과거 수집된 값 (최신 1건) |
| **조회 방식** | 단일 쿼리 (UNION ALL) | 병렬 처리 (10개 개별 쿼리) |
| **데이터 시각** | `SYSDATE` (현재 시각) | 저장된 `time` 컬럼 값 |

---

## 🎯 핵심 포인트

### 1. 실시간 스트리밍
- SSE 방식으로 매초마다 데이터를 실시간 전송
- 클라이언트는 연결을 유지하며 지속적으로 데이터 수신

### 2. 병렬 처리 최적화
- Repo DB 조회 시 10개 지표를 동시에 조회하여 속도 향상
- 각 스레드가 독립적인 DB 연결 사용

### 3. 정합성 검증
- Target DB의 실시간 값과 Repo DB의 저장된 값을 비교
- `threshold`를 기준으로 데이터 일치 여부 판단

### 4. 에러 처리
- 개별 지표 실패 시에도 다른 지표는 정상 조회
- 전체 실패 시에도 다음 수집 계속 진행

---

## 📡 클라이언트 사용 예시

```javascript
const eventSource = new EventSource('/rts/compare/stream?db_id=123&threshold=10&interval=1');

eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    
    if (data.type === 'start') {
        console.log('모니터링 시작:', data.message);
    } else if (data.type === 'progress') {
        console.log(`[${data.seq}] 비교 결과:`, data.comparison);
        // 실시간 차트 업데이트 등
    } else if (data.type === 'complete') {
        console.log('모니터링 완료:', data.success_rate);
        eventSource.close();
    } else if (data.type === 'error') {
        console.error('오류:', data.error);
    }
};
```

---

## 🔍 데이터 흐름 다이어그램

```
[클라이언트]                    [서버]                    [Target DB]    [Repo DB]
     |                            |                           |              |
     |-- GET /stream?db_id=123 -->|                           |              |
     |                            |-- DB 설정 확인 --------->|              |
     |                            |                           |              |
     |<-- SSE 연결 시작 ----------|                           |              |
     |                            |                           |              |
     |                            |-- 1초마다 반복 (30회) ---|              |
     |                            |                           |              |
     |                            |-- target_query 실행 ----->|              |
     |                            |<-- 실시간 성능 데이터 ----|              |
     |                            |                           |              |
     |                            |-- 병렬 조회 (10개) -------|------------->|
     |                            |<-- 저장된 성능 데이터 ----|              |
     |                            |                           |              |
     |                            |-- 데이터 비교 및 정합성 검증            |
     |                            |                           |              |
     |<-- progress 데이터 전송 ---|                           |              |
     |                            |                           |              |
     |                            |-- (30초 반복)             |              |
     |                            |                           |              |
     |<-- complete 메시지 --------|                           |              |
```

---

## 💡 성능 최적화 포인트

1. **병렬 처리**: Repo DB 조회 시 10개 지표를 동시에 조회
2. **최신 데이터만**: `rownum = 1`로 최신 데이터만 조회하여 데이터량 최소화
3. **정확한 간격 유지**: 수집 소요 시간을 고려하여 정확한 간격 유지
4. **에러 복구**: 개별 지표 실패 시에도 전체 프로세스 중단 없이 계속 진행



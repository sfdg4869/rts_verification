import json
import os
import sys

# sys.path에 현재 디렉토리와 app 디렉토리 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'app'))

try:
    import oracledb
    from app.config.database import REPO_DB_HOST, REPO_DB_PORT, REPO_DB_SERVICE, REPO_DB_USER, REPO_DB_PASSWORD
    
    def connect_repo():
        """Repository DB 연결"""
        dsn = f"{REPO_DB_HOST}:{REPO_DB_PORT}/{REPO_DB_SERVICE}"
        return oracledb.connect(user=REPO_DB_USER, password=REPO_DB_PASSWORD, dsn=dsn)

    def create_sec_logging_json(db_id=2, output_path=None):
        """sec_logging.json 파일 생성"""
        
        if not output_path:
            # 현재 디렉토리에 저장
            output_path = os.path.join(current_dir, "sec_logging.json")
        
        print(f"🔄 sec_logging.json 생성 중...")
        print(f"📁 저장 경로: {output_path}")
        print(f"🎯 DB ID: {db_id}")
        
        # 모니터링할 stat 이름들
        stat_names = [
            'CPU used by this session',
            'DB time',
            'physical read bytes',
            'physical read total bytes',
            'session logical reads',
            'redo synch long waits',
            'total sessions',
            'active user session'
        ]
        
        # 모니터링할 wait event 이름들
        wait_names = [
            'enq: TX - row lock contention',
            'enq: TM - contention'
        ]
        
        try:
            # DB 연결
            with connect_repo() as conn:
                with conn.cursor() as cur:
                    print("✅ DB 연결 성공")
                    
                    # stat_id 조회
                    stat_placeholders = ','.join([f':stat_{i}' for i in range(len(stat_names))])
                    stat_query = f"""
                    SELECT stat_id FROM ora_stat_name 
                    WHERE stat_name IN ({stat_placeholders}) 
                    AND db_id = :db_id
                    """
                    
                    stat_params = {f'stat_{i}': name for i, name in enumerate(stat_names)}
                    stat_params['db_id'] = db_id
                    
                    cur.execute(stat_query, stat_params)
                    db_stat_id = [str(row[0]) for row in cur.fetchall()]
                    print(f"📊 조회된 stat_id 개수: {len(db_stat_id)}")
                    
                    # wait_id 조회
                    wait_placeholders = ','.join([f':wait_{i}' for i in range(len(wait_names))])
                    wait_query = f"""
                    SELECT event_id FROM ora_wait_event 
                    WHERE event_name IN ({wait_placeholders}) 
                    AND db_id = :db_id
                    """
                    
                    wait_params = {f'wait_{i}': name for i, name in enumerate(wait_names)}
                    wait_params['db_id'] = db_id
                    
                    cur.execute(wait_query, wait_params)
                    db_wait_id = [str(row[0]) for row in cur.fetchall()]
                    print(f"⏱️ 조회된 wait_id 개수: {len(db_wait_id)}")
            
            # JSON 데이터 구성
            json_data = {
                "out": "table",
                "instances": [
                    {
                        "id": str(db_id),
                        "db_stat_id": db_stat_id,
                        "db_wait_id": db_wait_id
                    }
                ]
            }
            
            # JSON 문자열로 변환
            json_str = json.dumps(json_data, ensure_ascii=False, indent=4)
            
            # Windows 경로 처리
            if output_path:
                output_path = output_path.replace("\\", "/")
            
            # 디렉토리 생성
            output_dir = os.path.dirname(os.path.abspath(output_path))
            os.makedirs(output_dir, exist_ok=True)
            
            # 로컬 파일 저장
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(json_str)
            
            print(f"\n✅ 생성 성공!")
            print(f"📄 파일 경로: {os.path.abspath(output_path)}")
            print(f"📊 Stat 개수: {len(db_stat_id)}")
            print(f"⏱️ Wait 개수: {len(db_wait_id)}")
            print(f"📏 파일 크기: {len(json_str)} bytes")
            
            # JSON 내용 미리보기
            print(f"\n📋 JSON 내용:")
            print(json_str)
            
            return os.path.abspath(output_path)
            
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            return None

    if __name__ == "__main__":
        print("🚀 sec_logging.json 직접 생성 도구")
        print("=" * 50)
        
        # DB ID 2로 파일 생성
        result_path = create_sec_logging_json(db_id=2)
        
        if result_path:
            print(f"\n🎉 완료! 파일이 생성되었습니다.")
            print(f"📂 파일 위치: {result_path}")
        else:
            print("❌ 파일 생성에 실패했습니다.")

except ImportError as e:
    print(f"❌ 모듈 임포트 실패: {e}")
    print("💡 필요한 패키지를 설치하세요: pip install oracledb")
except Exception as e:
    print(f"❌ 예상치 못한 오류: {e}")

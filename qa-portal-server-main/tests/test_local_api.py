import json
import requests

# 로컬 전용 모드로 API 호출 테스트
def test_local_only():
    url = "http://localhost:5000/repo/create_sec_logging_json"
    
    # SSH 없이 로컬에만 파일 생성
    payload = {
        "dg_home_dir": "C:/Users/jungkyungsoo/Desktop/jks/OJT_2",
        "db_id": 2,
        "local_only": True  # 이 옵션이 핵심!
    }
    
    print("🔄 로컬 전용 모드로 API 호출 중...")
    print(f"📝 요청 데이터:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ 성공!")
            print(f"📄 결과: {result['result']}")
            print(f"📁 파일 경로: {result['file_path']}")
            print(f"🏠 로컬 전용: {result['local_only']}")
            print(f"📊 Stat 개수: {result['stat_count']}")
            print(f"⏱️ Wait 개수: {result['wait_count']}")
            return True
        else:
            print(f"❌ 실패 (상태코드: {response.status_code})")
            try:
                error = response.json()
                print(f"오류: {error}")
            except:
                print(f"응답: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ 서버 연결 실패")
        print("💡 해결책: 새 터미널에서 'python app.py' 실행 후 다시 시도")
        return False
    except Exception as e:
        print(f"❌ 오류: {e}")
        return False

if __name__ == "__main__":
    print("🚀 로컬 전용 sec_logging.json 생성 테스트")
    print("=" * 50)
    
    # 서버 연결 테스트
    try:
        response = requests.get("http://localhost:5000/", timeout=5)
        print("✅ 서버 연결 성공")
    except:
        print("❌ 서버 연결 실패")
        print("💡 먼저 Flask 서버를 시작하세요: python app.py")
        exit(1)
    
    # 로컬 전용 모드 테스트
    success = test_local_only()
    
    if success:
        print("\n🎉 테스트 완료!")
    else:
        print("\n❌ 테스트 실패")

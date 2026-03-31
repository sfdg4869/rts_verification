#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSH 연결 없이 sec_logging.json 파일을 생성하는 스크립트
"""

import requests
import json
import os

def create_local_sec_logging(db_id=2, output_path=None):
    """로컬에 sec_logging.json 파일 생성"""
    
    if not output_path:
        # 기본 저장 경로를 사용자 데스크톱으로 설정
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        output_path = os.path.join(desktop, "sec_logging.json")
    
    url = "http://localhost:5000/repo/create_sec_logging_json_local"
    
    payload = {
        "db_id": db_id,
        "output_path": output_path
    }
    
    print(f"🔄 sec_logging.json 생성 중...")
    print(f"📁 저장 경로: {output_path}")
    print(f"🎯 DB ID: {db_id}")
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ 생성 성공!")
            print(f"📄 파일 경로: {result['local_path']}")
            print(f"📊 Stat 개수: {result['stat_count']}")
            print(f"⏱️  Wait 개수: {result['wait_count']}")
            print(f"📏 파일 크기: {result['file_size']}")
            
            # JSON 내용 미리보기
            print(f"\n📋 JSON 내용 미리보기:")
            json_content = result['json_content']
            print(json.dumps(json_content, ensure_ascii=False, indent=2))
            
            return result['local_path']
        else:
            result = response.json()
            print(f"❌ 생성 실패: {result.get('error', '알 수 없는 오류')}")
            return None
            
    except requests.exceptions.ConnectionError:
        print("❌ 서버 연결 실패: Flask 서버가 실행 중인지 확인하세요.")
        return None
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return None

def test_server_connection():
    """서버 연결 테스트"""
    try:
        response = requests.get("http://localhost:5000/", timeout=5)
        if response.status_code == 200:
            print("✅ Flask 서버 연결 성공")
            return True
        else:
            print(f"⚠️ 서버 응답 코드: {response.status_code}")
            return False
    except:
        print("❌ Flask 서버 연결 실패")
        return False

if __name__ == "__main__":
    print("🚀 sec_logging.json 로컬 생성 도구")
    print("=" * 50)
    
    # 서버 연결 확인
    if not test_server_connection():
        print("\n💡 해결 방법:")
        print("1. 새 터미널에서 'python app.py' 실행")
        print("2. 서버가 http://localhost:5000 에서 실행되는지 확인")
        exit(1)
    
    # 사용자 입력 받기
    try:
        db_id = input("\n📝 DB ID를 입력하세요 (기본값: 2): ").strip()
        if not db_id:
            db_id = 2
        else:
            db_id = int(db_id)
        
        custom_path = input("📁 저장 경로를 입력하세요 (Enter: 데스크톱): ").strip()
        output_path = custom_path if custom_path else None
        
        # 파일 생성 실행
        result_path = create_local_sec_logging(db_id, output_path)
        
        if result_path:
            print(f"\n🎉 완료! 파일이 생성되었습니다.")
            print(f"📂 파일 위치: {result_path}")
            
            # 파일 열기 여부 확인
            open_file = input("\n📖 파일을 열어보시겠습니까? (y/N): ").strip().lower()
            if open_file == 'y':
                os.startfile(result_path)
        
    except KeyboardInterrupt:
        print("\n\n👋 작업이 취소되었습니다.")
    except ValueError:
        print("❌ DB ID는 숫자여야 합니다.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")

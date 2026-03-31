#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""db_setup.json 파일을 읽어서 /rts/db/setup API를 호출하는 스크립트"""

import json
import requests
import sys
import os

def setup_db_from_json():
    """db_setup.json 파일을 읽어서 DB 설정 API 호출"""
    
    # db_setup.json 파일 경로
    json_file = "db_setup.json"
    
    # 파일 존재 확인
    if not os.path.exists(json_file):
        print(f"[ERROR] 파일을 찾을 수 없습니다: {json_file}")
        return False
    
    # JSON 파일 읽기
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[OK] JSON 파일 로드 성공: {json_file}")
    except Exception as e:
        print(f"[ERROR] JSON 파일 읽기 실패: {e}")
        return False
    
    # API 호출
    url = "http://localhost:5000/rts/db/setup"
    
    try:
        print(f"\n[INFO] API 호출 중: {url}")
        print(f"[INFO] 요청 데이터:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        response = requests.post(url, json=data, timeout=10)
        
        print(f"\n[INFO] 응답 상태 코드: {response.status_code}")
        print(f"[INFO] 응답 내용:")
        
        try:
            result = response.json()
            print(json.dumps(result, indent=2, ensure_ascii=False))
        except:
            print(response.text)
        
        if response.status_code == 200:
            print("\n[SUCCESS] DB 설정 성공!")
            return True
        else:
            print("\n[ERROR] DB 설정 실패")
            return False
            
    except requests.exceptions.ConnectionError:
        print("\n[ERROR] 서버 연결 실패")
        print("[TIP] Flask 서버가 실행 중인지 확인하세요 (python app.py)")
        return False
    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("DB 설정 스크립트 (db_setup.json 사용)")
    print("=" * 60)
    
    success = setup_db_from_json()
    
    if success:
        print("\n[SUCCESS] 완료! 이제 rts_compare API를 테스트할 수 있습니다.")
        print("   예: GET /rts/compare/db_status")
        print("   예: GET /rts/compare?db_id=1")
    else:
        print("\n[ERROR] 실패")
        sys.exit(1)


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Windows 경로 처리 테스트 스크립트
"""

import requests
import json

# 테스트할 Windows 경로
test_paths = [
    "C:\\Users\\jungkyungsoo\\Desktop\\jks\\OJT_2",
    "C:/Users/jungkyungsoo/Desktop/jks/OJT_2",
    "/home/dg",  # Unix 스타일 경로도 테스트
]

def test_sec_logging_preview(db_id=2):
    """미리보기 API 테스트"""
    url = "http://localhost:5000/repo/preview_sec_logging_json"
    
    payload = {
        "db_id": db_id
    }
    
    print(f"\n=== sec_logging.json 미리보기 테스트 (db_id: {db_id}) ===")
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ 미리보기 성공!")
            print(f"stat_count: {len(result.get('stat_details', []))}")
            print(f"wait_count: {len(result.get('wait_details', []))}")
        else:
            print("❌ 미리보기 실패:")
            print(response.text)
    except Exception as e:
        print(f"❌ 요청 오류: {e}")

def test_path_processing():
    """Windows 경로 처리 테스트"""
    print("\n=== Windows 경로 처리 테스트 ===")
    
    for path in test_paths:
        print(f"\n테스트 경로: {path}")
        
        # 경로 정규화 (실제 API에서 사용하는 로직)
        normalized = path.replace("\\", "/").rstrip('/')
        print(f"정규화된 경로: {normalized}")
        
        # JSON 직렬화 테스트
        test_json = {
            "original_path": path,
            "normalized_path": normalized
        }
        json_str = json.dumps(test_json, ensure_ascii=False, indent=2)
        print(f"JSON 직렬화 결과:")
        print(json_str)

def test_sec_logging_creation(path, db_id=2, ssh_host="test", ssh_user="test", ssh_password="test"):
    """실제 파일 생성 API 테스트 (dry run)"""
    url = "http://localhost:5000/repo/create_sec_logging_json"
    
    payload = {
        "dg_home_dir": path,
        "db_id": db_id,
        "ssh_host": ssh_host,
        "ssh_user": ssh_user,
        "ssh_password": ssh_password
    }
    
    print(f"\n=== sec_logging.json 생성 테스트 ===")
    print(f"경로: {path}")
    
    # 실제 요청은 하지 않고 payload만 확인
    print("요청 payload:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    print("Windows 경로 처리 테스트 시작")
    
    # 1. 경로 처리 로직 테스트
    test_path_processing()
    
    # 2. 미리보기 API 테스트 (실제 서버가 실행 중인 경우)
    test_sec_logging_preview()
    
    # 3. 생성 API payload 테스트
    for path in test_paths:
        test_sec_logging_creation(path)
    
    print("\n테스트 완료!")

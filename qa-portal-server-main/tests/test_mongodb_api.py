#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 기반 DB 설정 시스템 테스트
"""
import json
import requests
import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

BASE_URL = "http://localhost:5000"

def test_mongodb_status():
    """MongoDB 연결 상태 테스트"""
    print("\n=== MongoDB 상태 확인 ===")
    response = requests.get(f"{BASE_URL}/rts/mongodb/status")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def test_create_db_profile():
    """DB 프로파일 생성 테스트"""
    print("\n=== DB 프로파일 생성 테스트 ===")
    
    profile_data = {
        "profile_name": "test_profile_001",
        "description": "테스트용 프로파일",
        "target_config": {
            "host": "localhost",
            "port": 1521,
            "service_name": "ORCL",
            "username": "test_user",
            "password": "test_password"
        },
        "repo_config": {
            "host": "localhost",
            "port": 1521,
            "service_name": "XE",
            "username": "repo_user",
            "password": "repo_password"
        },
        "is_default": True
    }
    
    response = requests.post(
        f"{BASE_URL}/rts/mongodb/profiles",
        json=profile_data,
        headers={'Content-Type': 'application/json'}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def test_get_profiles():
    """DB 프로파일 목록 조회 테스트"""
    print("\n=== DB 프로파일 목록 조회 ===")
    response = requests.get(f"{BASE_URL}/rts/mongodb/profiles")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def test_activate_profile():
    """DB 프로파일 활성화 테스트"""
    print("\n=== DB 프로파일 활성화 테스트 ===")
    response = requests.post(f"{BASE_URL}/rts/mongodb/profiles/test_profile_001/activate")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def test_get_current_profile():
    """현재 활성화된 프로파일 조회 테스트"""
    print("\n=== 현재 프로파일 조회 ===")
    response = requests.get(f"{BASE_URL}/rts/mongodb/current-profile")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def test_save_individual_configs():
    """개별 DB 설정 저장 테스트"""
    print("\n=== 개별 Target DB 설정 저장 ===")
    target_config = {
        "host": "192.168.1.100",
        "port": 1521,
        "service_name": "PROD",
        "username": "target_user",
        "password": "target_password"
    }
    
    response = requests.post(
        f"{BASE_URL}/rts/mongodb/configs/target",
        json=target_config,
        headers={'Content-Type': 'application/json'}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    print("\n=== 개별 Repo DB 설정 저장 ===")
    repo_config = {
        "host": "192.168.1.101",
        "port": 1521,
        "service_name": "REPO",
        "username": "repo_user",
        "password": "repo_password"
    }
    
    response = requests.post(
        f"{BASE_URL}/rts/mongodb/configs/repo",
        json=repo_config,
        headers={'Content-Type': 'application/json'}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    return True

def test_get_configs():
    """DB 설정 조회 테스트"""
    print("\n=== Target DB 설정 조회 ===")
    response = requests.get(f"{BASE_URL}/rts/mongodb/configs/target")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    print("\n=== Repo DB 설정 조회 ===")
    response = requests.get(f"{BASE_URL}/rts/mongodb/configs/repo")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    return True

def test_legacy_compatibility():
    """기존 API와의 호환성 테스트"""
    print("\n=== 기존 /rts/db/setup API 호환성 테스트 ===")
    
    legacy_data = {
        "target_db": {
            "host": "localhost",
            "port": 1521,
            "service": "ORCL",  # 기존 형식
            "user": "legacy_user",  # 기존 형식
            "password": "legacy_password"
        },
        "repo_db": {
            "host": "localhost",
            "port": 1521,
            "service": "XE",  # 기존 형식
            "user": "legacy_repo",  # 기존 형식
            "password": "legacy_repo_password"
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/rts/db/setup",
        json=legacy_data,
        headers={'Content-Type': 'application/json'}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def test_delete_profile():
    """DB 프로파일 삭제 테스트"""
    print("\n=== DB 프로파일 삭제 테스트 ===")
    response = requests.delete(f"{BASE_URL}/rts/mongodb/profiles/test_profile_001")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200

def main():
    """모든 테스트 실행"""
    print("MongoDB 기반 DB 설정 시스템 테스트 시작")
    print("=" * 50)
    
    tests = [
        ("MongoDB 상태 확인", test_mongodb_status),
        ("DB 프로파일 생성", test_create_db_profile),
        ("DB 프로파일 목록 조회", test_get_profiles),
        ("DB 프로파일 활성화", test_activate_profile),
        ("현재 프로파일 조회", test_get_current_profile),
        ("개별 DB 설정 저장", test_save_individual_configs),
        ("DB 설정 조회", test_get_configs),
        ("기존 API 호환성", test_legacy_compatibility),
        ("DB 프로파일 삭제", test_delete_profile),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, "✅ PASS" if success else "❌ FAIL"))
        except Exception as e:
            print(f"테스트 실행 중 오류: {e}")
            results.append((test_name, f"❌ ERROR: {e}"))
    
    print("\n" + "=" * 50)
    print("테스트 결과 요약:")
    for test_name, result in results:
        print(f"{test_name}: {result}")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB에 샘플 DB 설정 데이터 추가
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime

# 환경 변수 로드
load_dotenv()

def add_sample_data():
    """샘플 DB 설정 데이터 추가"""
    try:
        # MongoDB 연결
        host = os.getenv('MONGODB_HOST', 'localhost')
        port = int(os.getenv('MONGODB_PORT', 27017))
        database = os.getenv('MONGODB_DATABASE', 'qs1_db')
        
        client = MongoClient(f"mongodb://{host}:{port}")
        db = client[database]
        
        # 기존 데이터 삭제 (테스트용)
        db.db_configs.delete_many({})
        db.connection_profiles.delete_many({})
        
        print("기존 데이터 삭제 완료")
        
        # 샘플 DB 설정들 추가
        sample_configs = [
            {
                "config_id": "target_oracle_prod",
                "config_type": "target",
                "host": "10.20.132.101",
                "port": 1521,
                "service_name": "oracle19",
                "username": "maxgauge",
                "password": "maxgauge",
                "description": "운영 Oracle 서버 (Target)",
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "config_id": "repo_oracle_test",
                "config_type": "repo",
                "host": "10.20.132.40",
                "port": 1521,
                "service_name": "orcl",
                "username": "C##TEST_2507",
                "password": "TEST_2507",
                "description": "테스트 Oracle 서버 (Repo)",
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "config_id": "general_oracle_dev",
                "config_type": "general",
                "host": "localhost",
                "port": 1521,
                "service_name": "XE",
                "username": "hr",
                "password": "hr",
                "description": "개발 Oracle Express",
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "config_id": "general_oracle_backup",
                "config_type": "general",
                "host": "192.168.1.100",
                "port": 1521,
                "service_name": "BACKUP",
                "username": "backup_user",
                "password": "backup_pass",
                "description": "백업 Oracle 서버",
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "config_id": "general_oracle_archive",
                "config_type": "general",
                "host": "192.168.1.200",
                "port": 1521,
                "service_name": "ARCHIVE",
                "username": "archive_user",
                "password": "archive_pass",
                "description": "아카이브 Oracle 서버",
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        
        # DB 설정 삽입
        result = db.db_configs.insert_many(sample_configs)
        print(f"DB 설정 {len(result.inserted_ids)}개 추가 완료")
        
        # 샘플 연결 프로파일 추가
        sample_profiles = [
            {
                "profile_id": "production_profile",
                "profile_name": "운영 환경",
                "description": "운영 서버 Target + 테스트 서버 Repo",
                "target_config_id": "target_oracle_prod",
                "repo_config_id": "repo_oracle_test",
                "is_default": True,
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "profile_id": "development_profile",
                "profile_name": "개발 환경",
                "description": "개발 서버 전용 프로파일",
                "target_config_id": "general_oracle_dev",
                "repo_config_id": "general_oracle_dev",
                "is_default": False,
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "profile_id": "backup_profile",
                "profile_name": "백업 환경",
                "description": "백업 및 아카이브 서버 조합",
                "target_config_id": "general_oracle_backup",
                "repo_config_id": "general_oracle_archive",
                "is_default": False,
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        
        # 연결 프로파일 삽입
        result = db.connection_profiles.insert_many(sample_profiles)
        print(f"연결 프로파일 {len(result.inserted_ids)}개 추가 완료")
        
        # 현재 설정 확인
        print("\n=== 현재 저장된 데이터 ===")
        
        configs = list(db.db_configs.find({}, {"_id": 0, "password": 0}))
        print(f"\nDB 설정 ({len(configs)}개):")
        for config in configs:
            print(f"  - {config['config_id']}: {config['host']}:{config['port']} ({config['config_type']})")
        
        profiles = list(db.connection_profiles.find({}, {"_id": 0}))
        print(f"\n연결 프로파일 ({len(profiles)}개):")
        for profile in profiles:
            print(f"  - {profile['profile_id']}: {profile['profile_name']} (기본: {profile['is_default']})")
        
        client.close()
        print("\n✅ 샘플 데이터 추가 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 샘플 데이터 추가 실패: {e}")
        return False

if __name__ == "__main__":
    add_sample_data()
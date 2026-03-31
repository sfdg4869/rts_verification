#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 연결 상태 직접 테스트
"""
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus  # 추가

# 환경 변수 로드
load_dotenv()

def test_mongodb_connection():
    """MongoDB 연결 테스트"""
    try:
        # 환경 변수 확인
        host = os.getenv('MONGODB_HOST', '10.10.49.250')
        port = int(os.getenv('MONGODB_PORT', 27017))
        database = os.getenv('MONGODB_DATABASE', 'qa_portal')
        username = os.getenv('MONGODB_USERNAME', 'maxgauge')
        password = os.getenv('MONGODB_PASSWORD', 'zbdptm@Max')
        auth_source = os.getenv('MONGODB_AUTH_SOURCE', 'admin')
        
        print(f"연결 정보:")
        print(f"  호스트: {host}")
        print(f"  포트: {port}")
        print(f"  데이터베이스: {database}")
        print(f"  사용자명: {username if username else '(없음)'}")
        print(f"  비밀번호: {'***' if password else '(없음)'}")
        print(f"  인증 소스: {auth_source}")
        
        # 연결 문자열 생성 (URL 인코딩 적용)
        if username and password:
            # 사용자명과 비밀번호를 URL 인코딩
            encoded_username = quote_plus(username)
            encoded_password = quote_plus(password)
            connection_string = f"mongodb://{encoded_username}:{encoded_password}@{host}:{port}/{database}?authSource={auth_source}"
            print(f"연결 문자열: mongodb://{username}:***@{host}:{port}/{database}?authSource={auth_source}")
        else:
            connection_string = f"mongodb://{host}:{port}"
            print(f"연결 문자열: {connection_string}")
        
        # MongoDB 연결 시도
        print("\n=== MongoDB 연결 시도 ===")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # 연결 테스트
        print("ping 명령 실행 중...")
        result = client.admin.command('ping')
        print(f"ping 결과: {result}")
        
        # 데이터베이스 접근
        db = client[database]
        print(f"데이터베이스 '{database}' 접근 성공")
        
        # 컬렉션 목록 확인
        collections = db.list_collection_names()
        print(f"기존 컬렉션: {collections}")
        
        # 간단한 테스트 문서 삽입
        test_collection = db['connection_test']
        test_doc = {"test": "connection", "timestamp": "2025-10-02"}
        
        # 기존 테스트 문서 삭제
        test_collection.delete_many({"test": "connection"})
        
        # 새 문서 삽입
        result = test_collection.insert_one(test_doc)
        print(f"테스트 문서 삽입 성공: {result.inserted_id}")
        
        # 문서 조회
        found_doc = test_collection.find_one({"test": "connection"})
        print(f"테스트 문서 조회: {found_doc}")
        
        # 정리
        test_collection.delete_one({"_id": result.inserted_id})
        print("테스트 문서 삭제 완료")
        
        client.close()
        print("\n✅ MongoDB 연결 테스트 성공!")
        return True
        
    except PyMongoError as e:
        print(f"\n❌ MongoDB 오류: {e}")
        print(f"오류 타입: {type(e).__name__}")
        return False
    except Exception as e:
        print(f"\n❌ 일반 오류: {e}")
        print(f"오류 타입: {type(e).__name__}")
        return False

if __name__ == "__main__":
    test_mongodb_connection()
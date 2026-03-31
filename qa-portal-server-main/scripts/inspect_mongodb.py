#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 실제 데이터 구조 확인
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

def inspect_mongodb():
    """MongoDB 실제 데이터 구조 확인"""
    try:
        # MongoDB 연결
        host = os.getenv('MONGODB_HOST', 'localhost')
        port = int(os.getenv('MONGODB_PORT', 27017))
        database = os.getenv('MONGODB_DATABASE', 'qs1_db')
        
        client = MongoClient(f"mongodb://{host}:{port}")
        db = client[database]
        
        print(f"=== MongoDB 데이터베이스: {database} ===")
        
        # 모든 컬렉션 목록 확인
        collections = db.list_collection_names()
        print(f"\n📋 컬렉션 목록 ({len(collections)}개):")
        for collection in collections:
            count = db[collection].count_documents({})
            print(f"  - {collection}: {count}개 문서")
        
        # 각 컬렉션의 샘플 데이터 확인
        for collection_name in collections:
            collection = db[collection_name]
            count = collection.count_documents({})
            
            print(f"\n🔍 컬렉션: {collection_name} ({count}개 문서)")
            
            if count > 0:
                # 첫 번째 문서 구조 확인
                sample = collection.find_one()
                if sample:
                    print("   샘플 문서 구조:")
                    for key, value in sample.items():
                        if key == '_id':
                            print(f"     {key}: {str(value)}")
                        else:
                            print(f"     {key}: {str(value)[:100]}...")
                
                # 문서들의 고유한 필드들 확인
                pipeline = [
                    {"$project": {"fields": {"$objectToArray": "$$ROOT"}}},
                    {"$unwind": "$fields"},
                    {"$group": {"_id": "$fields.k"}},
                    {"$sort": {"_id": 1}}
                ]
                
                try:
                    unique_fields = list(collection.aggregate(pipeline))
                    field_names = [field['_id'] for field in unique_fields if field['_id'] != '_id']
                    print(f"   모든 필드: {', '.join(field_names)}")
                except Exception as e:
                    print(f"   필드 분석 실패: {e}")
        
        # 우리가 찾는 데이터와 비슷한 구조 찾기
        print(f"\n🔎 DB 설정 관련 데이터 검색:")
        
        for collection_name in collections:
            collection = db[collection_name]
            
            # host 필드가 있는 문서들 찾기
            docs_with_host = list(collection.find({"host": {"$exists": True}}).limit(3))
            if docs_with_host:
                print(f"\n   {collection_name}에서 'host' 필드가 있는 문서:")
                for doc in docs_with_host:
                    print(f"     - host: {doc.get('host')}, port: {doc.get('port')}, service: {doc.get('service', doc.get('service_name', 'N/A'))}")
            
            # service 또는 service_name 필드가 있는 문서들 찾기
            docs_with_service = list(collection.find({"$or": [{"service": {"$exists": True}}, {"service_name": {"$exists": True}}]}).limit(3))
            if docs_with_service and not docs_with_host:  # 중복 출력 방지
                print(f"\n   {collection_name}에서 'service' 관련 필드가 있는 문서:")
                for doc in docs_with_service:
                    print(f"     - service: {doc.get('service', doc.get('service_name', 'N/A'))}")
        
        client.close()
        print("\n✅ MongoDB 구조 분석 완료!")
        
    except Exception as e:
        print(f"❌ MongoDB 구조 분석 실패: {e}")

if __name__ == "__main__":
    inspect_mongodb()
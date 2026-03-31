#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 직접 데이터 조회 테스트
"""
from pymongo import MongoClient
import json

def test_mongodb_direct():
    """MongoDB에서 직접 데이터 조회"""
    try:
        # MongoDB 연결
        client = MongoClient("mongodb://localhost:27017")
        db = client['qs1_db']
        
        print("=== MongoDB 연결 성공 ===")
        print(f"데이터베이스: {db.name}")
        
        # 모든 컬렉션 목록
        collections = db.list_collection_names()
        print(f"\n📋 컬렉션 목록 ({len(collections)}개):")
        for collection in collections:
            count = db[collection].count_documents({})
            print(f"  - {collection}: {count}개 문서")
        
        # 각 컬렉션에서 entries 필드가 있는 문서 찾기
        print(f"\n🔍 entries 필드가 있는 문서 검색:")
        for collection_name in collections:
            collection = db[collection_name]
            
            # entries 필드가 있는 문서 개수
            entries_count = collection.count_documents({"entries": {"$exists": True}})
            if entries_count > 0:
                print(f"\n  📁 {collection_name}: entries가 있는 문서 {entries_count}개")
                
                # 첫 번째 entries 문서 조회
                doc = collection.find_one({"entries": {"$exists": True}})
                if doc and 'entries' in doc:
                    entries = doc['entries']
                    print(f"     entries 배열 크기: {len(entries)}")
                    
                    # 첫 번째 entry 확인
                    if len(entries) > 0:
                        first_entry = entries[0]
                        print(f"     첫 번째 entry 키들: {list(first_entry.keys())}")
                        print(f"     첫 번째 entry 내용:")
                        for key, value in first_entry.items():
                            print(f"       {key}: {value}")
                        
                        # host가 있는 entries 개수 확인
                        host_count = sum(1 for entry in entries if isinstance(entry, dict) and 'host' in entry)
                        print(f"     host 필드가 있는 entry: {host_count}개")
                        
                        # 몇 개 더 샘플 출력
                        print(f"     처음 3개 entries 요약:")
                        for i, entry in enumerate(entries[:3]):
                            if isinstance(entry, dict) and 'host' in entry:
                                print(f"       [{i}] {entry.get('host')}:{entry.get('port')} ({entry.get('service', 'N/A')})")
        
        # host 필드가 직접 있는 문서들도 확인
        print(f"\n🔍 직접 host 필드가 있는 문서 검색:")
        for collection_name in collections:
            collection = db[collection_name]
            
            host_count = collection.count_documents({"host": {"$exists": True}})
            if host_count > 0:
                print(f"  📁 {collection_name}: host가 있는 문서 {host_count}개")
        
        client.close()
        print("\n✅ MongoDB 직접 조회 완료!")
        
    except Exception as e:
        print(f"❌ MongoDB 직접 조회 실패: {e}")
        import traceback
        print(f"에러 상세: {traceback.format_exc()}")

if __name__ == "__main__":
    test_mongodb_direct()
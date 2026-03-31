"""
demo_monitor.json → MongoDB disk_servers 컬렉션 마이그레이션 스크립트

사용법:
    cd qa-portal-server
    python migrate_disk_servers.py
"""

import json
import os
import sys
import uuid
from datetime import datetime

# 프로젝트 루트를 sys.path에 추가 (app 모듈 import 가능하도록)
sys.path.insert(0, os.path.dirname(__file__))

from app.models.db_config import MONGODB_CONFIG
from pymongo import MongoClient

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'app', 'config', 'demo_monitor.json')

# 서버별 읽기 쉬운 이름 매핑 (선택적으로 수정)
NAME_MAP = {
    '10.10.48.30': 'QS1-DEV-48-30',
    '10.10.47.80': 'QS1-DEV-47-80',
    '10.10.47.81': 'QS1-DEV-47-81',
    '10.10.47.82': 'QS1-DEV-47-82',
    '10.10.47.83': 'QS1-DEV-47-83',
}

def run():
    # JSON 읽기
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        servers = json.load(f)

    # MongoDB 연결
    client = MongoClient(MONGODB_CONFIG['uri'])
    db = client[MONGODB_CONFIG['database']]
    collection = db['disk_servers']

    inserted = 0
    skipped = 0

    for s in servers:
        host = s.get('host', '')

        # 이미 같은 host가 있으면 skip
        if collection.find_one({'host': host}):
            print(f'  [SKIP] {host} — 이미 존재')
            skipped += 1
            continue

        now = datetime.utcnow()
        doc = {
            'id': str(uuid.uuid4()),
            'name': NAME_MAP.get(host, host),
            'host': host,
            'port': int(s.get('port', 22)),
            'username': s.get('username', s.get('user', 'root')),
            'password': s.get('password', ''),
            'description': '',
            'enabled': True,
            'created_at': now,
            'updated_at': now,
        }

        collection.insert_one(doc)
        print(f'  [OK]   {host}  →  {doc["name"]}  (id: {doc["id"][:8]}...)')
        inserted += 1

    print(f'\n완료: {inserted}개 삽입, {skipped}개 스킵')
    print(f'컬렉션 전체 문서 수: {collection.count_documents({})}')
    client.close()

if __name__ == '__main__':
    run()

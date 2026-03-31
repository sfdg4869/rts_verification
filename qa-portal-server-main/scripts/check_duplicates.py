from pymongo import MongoClient
from collections import Counter

client = MongoClient('mongodb://localhost:27017/')
db = client['admin']
collection = db['qs1_db']

doc = collection.find_one()
if doc and 'entries' in doc:
    entries = doc['entries']
    print(f'전체 entries: {len(entries)}개')
    
    # host:port:service 조합 카운트
    combinations = []
    for i, entry in enumerate(entries):
        if isinstance(entry, dict) and 'host' in entry:
            host = entry.get('host', '')
            port = entry.get('port', 1521)
            service = entry.get('service', '')
            combo = f"{host}:{port}:{service}"
            combinations.append((i, combo))
    
    # 중복 찾기
    combo_counts = Counter([combo for _, combo in combinations])
    duplicates = {combo: count for combo, count in combo_counts.items() if count > 1}
    
    print(f'고유한 조합: {len(combo_counts)}개')
    print(f'중복된 조합: {len(duplicates)}개')
    
    if duplicates:
        print('\n중복된 조합들:')
        for combo, count in duplicates.items():
            print(f'  {combo}: {count}번 중복')
            
            # 중복된 항목들의 인덱스 찾기
            duplicate_indices = [i for i, c in combinations if c == combo]
            print(f'    인덱스: {duplicate_indices}')
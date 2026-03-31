from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['admin']
collection = db['qs1_db']

doc = collection.find_one()
if doc and 'entries' in doc:
    entries = doc['entries']
    print(f'Total entries: {len(entries)}')
    
    # 모든 항목 출력 (enum 생성용)
    enum_list = []
    for i in range(len(entries)):
        entry = entries[i]
        if 'host' in entry:
            display_name = f"{entry.get('host', '')}:{entry.get('port', 1521)} ({entry.get('service', 'N/A')})"
            config_id = f"entry_{i}"
            enum_list.append(f"'{config_id}',   # {display_name}")
            
    print("\n=== Enum List for Swagger ===")
    for item in enum_list:
        print(item)
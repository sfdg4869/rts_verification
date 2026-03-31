from pymongo import MongoClient

def generate_enum_list():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['admin']
    collection = db['qs1_db']

    doc = collection.find_one()
    if doc and 'entries' in doc:
        entries = doc['entries']
        print(f'Total entries: {len(entries)}')
        
        # 97개 전체 enum 목록 생성
        enum_items = []
        for i in range(len(entries)):
            entry = entries[i]
            if 'host' in entry:
                display_name = f"{entry.get('host', '')}:{entry.get('port', 1521)} ({entry.get('service', 'N/A')})"
                enum_items.append(f"'entry_{i}'")  # 단순히 entry_0, entry_1... 형태
                
        return enum_items
    return []

if __name__ == "__main__":
    enum_list = generate_enum_list()
    print(f"Generated {len(enum_list)} enum items")
    
    # Swagger에서 사용할 enum 배열 형태로 출력
    print("\n=== Swagger Enum Array ===")
    print("'enum': [")
    for i, item in enumerate(enum_list):
        if i < len(enum_list) - 1:
            print(f"    {item},")
        else:
            print(f"    {item}")
    print("]")
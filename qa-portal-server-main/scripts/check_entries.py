from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['admin']
collection = db['qs1_db']

doc = collection.find_one()
if doc and 'entries' in doc:
    entries = doc['entries']
    print(f'MongoDB에서 실제 entries 개수: {len(entries)}')
    
    # host 필드가 있는 항목만 카운트
    valid_entries = 0
    invalid_entries = []
    
    for i, entry in enumerate(entries):
        if isinstance(entry, dict) and 'host' in entry:
            valid_entries += 1
        else:
            invalid_entries.append(f'Entry {i}: {type(entry)} - {str(entry)[:50]}...')
    
    print(f'유효한 entries (host 필드 있음): {valid_entries}개')
    print(f'무효한 entries: {len(invalid_entries)}개')
    
    if invalid_entries:
        print('\n무효한 entries 샘플:')
        for invalid in invalid_entries[:5]:  # 처음 5개만 출력
            print(f'  {invalid}')
else:
    print('entries 배열을 찾을 수 없습니다.')
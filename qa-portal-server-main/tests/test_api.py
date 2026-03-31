import requests
import json

# API 테스트
url = "http://localhost:5000/api/v1/oracle/data-collection/missing-data"
params = {
    "db_ids": "2",
    "start_time": "2025-09-05 09:00:00",
    "end_time": "2025-09-05 09:59:59",
    "partition_date": "250905"
}

try:
    response = requests.get(url, params=params)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
except Exception as e:
    print(f"Error: {e}")

import json

# Windows 경로 테스트
payload = {
    "db_id": 2,
    "dg_home_dir": "C:\\Users\\jungkyungsoo\\Desktop\\jks\\OJT_2",
    "ssh_host": "10.20.132.40",
    "ssh_password": "!977255Ks",
    "ssh_user": "jungkyungsoo"
}

print("=== 원본 페이로드 ===")
print(f"dg_home_dir 원본: {payload['dg_home_dir']}")
print(f"dg_home_dir 타입: {type(payload['dg_home_dir'])}")

print("\n=== JSON 직렬화 테스트 ===")
json_str = json.dumps(payload, ensure_ascii=False, indent=2)
print(json_str)

print("\n=== 경로 정규화 테스트 ===")
original_path = payload['dg_home_dir']
normalized_path = original_path.replace("\\", "/")
print(f"정규화 전: {original_path}")
print(f"정규화 후: {normalized_path}")

print("\n=== JSON 역직렬화 테스트 ===")
parsed = json.loads(json_str)
print(f"파싱된 경로: {parsed['dg_home_dir']}")

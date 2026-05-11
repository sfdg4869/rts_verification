import json
from pathlib import Path

p = Path(
    r"C:\Users\jungkyungsoo\.cursor\projects\c-Users-jungkyungsoo-Desktop-jks-qa-portal-server-main-add2"
    r"\agent-transcripts\1639766a-2bdd-400a-9ad4-c152a2f349a0\1639766a-2bdd-400a-9ad4-c152a2f349a0.jsonl"
)
lines = p.read_text(encoding="utf-8").splitlines()
for idx in (1017, 1019, 1020, 1027, 1028, 1029, 1049):
    rec = json.loads(lines[idx])
    for part in rec.get("message", {}).get("content", []):
        if part.get("name") != "StrReplace":
            continue
        inp = part["input"]
        print("LINE", idx + 1, inp["path"].split("\\")[-1])
        print("OLD----\n", inp["old_string"][:800])
        print("NEW----\n", inp["new_string"][:1200])
        print("=" * 60)

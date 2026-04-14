import urllib.request, urllib.error
import urllib.parse
import json

try:
    data = json.dumps({"db_id": 255}).encode('utf-8')
    req = urllib.request.Request('http://127.0.0.1:5000/api/v2/rts/check/run-repo', data=data, headers={'Content-Type':'application/json'})
    print(urllib.request.urlopen(req).read().decode())
except urllib.error.HTTPError as e:
    print(e.read().decode())

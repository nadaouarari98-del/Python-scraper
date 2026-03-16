import urllib.request
import json
import time

time.sleep(2)

try:
    res = urllib.request.urlopen('http://127.0.0.1:5000/api/dashboard-data')
    data = json.loads(res.read())
    print('API Status: SUCCESS')
    print('Response keys:', list(data.keys()))
    print()
    print('pipeline_statuses count:', len(data.get('pipeline_statuses', {})))
    print('source_statuses count:', len(data.get('source_statuses', {})))
    print('activities count:', len(data.get('activities', [])))
    print()
    print('pipeline_statuses:', json.dumps(data.get('pipeline_statuses', {}), indent=2))
    print()
    print('source_statuses:', json.dumps(data.get('source_statuses', {}), indent=2))
    print()
    print('activities (first 5):', json.dumps(data.get('activities', [])[:5], indent=2))
except Exception as e:
    print('ERROR:', type(e).__name__, '-', str(e))
    import traceback
    traceback.print_exc()

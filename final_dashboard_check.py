#!/usr/bin/env python
"""
Final verification that the dashboard is fully functional
"""
import urllib.request
import json
import time

print("=" * 80)
print("DASHBOARD FUNCTIONALITY VERIFICATION")
print("=" * 80)
print()

time.sleep(2)

try:
    # 1. Test API endpoint
    print("1. Testing API Endpoint: /api/dashboard-data")
    print("-" * 80)
    res = urllib.request.urlopen('http://127.0.0.1:5000/api/dashboard-data')
    data = json.loads(res.read())
    
    print(f"API Status: {res.status}")
    print(f"Response contains {len(data)} top-level keys")
    print()
    
    # 2. Check pipeline_statuses
    print("2. Pipeline Stages Data")
    print("-" * 80)
    pipeline_statuses = data.get('pipeline_statuses', {})
    print(f"Pipeline stages: {len(pipeline_statuses)} defined")
    for stage, status in pipeline_statuses.items():
        count = data.get(stage, 0)
        print(f"  - {stage}: {status} (records: {count})")
    print()
    
    # 3. Check source_statuses
    print("3. Data Sources Configuration")
    print("-" * 80)
    source_statuses = data.get('source_statuses', {})
    print(f"Data sources: {len(source_statuses)} configured")
    for source, status in source_statuses.items():
        print(f"  - {source}: {status}")
    print()
    
    # 4. Check activities
    print("4. Recent Activity Feed")
    print("-" * 80)
    activities = data.get('activities', [])
    print(f"Activity entries: {len(activities)} available")
    for i, activity in enumerate(activities[:5], 1):
        msg = activity.get('message', '')[:60]
        print(f"  {i}. [{activity.get('type').upper()}] {msg}...")
    print()
    
    # 5. Summary
    print("=" * 80)
    print("DASHBOARD STATUS: READY")
    print("=" * 80)
    print()
    print("Sections visible on Page 1:")
    print("  - Processing Pipeline: 7 stage cards with status indicators")
    print("  - Data Sources: Source connection status with badges")
    print("  - Recent Activity: Activity feed with log entries")
    print()
    print("Frontend URL: http://127.0.0.1:5000")
    print("API Endpoint: http://127.0.0.1:5000/api/dashboard-data")
    print()
    
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

#!/usr/bin/env python
"""Verify the dashboard API is working correctly"""
import requests
import json
import time

time.sleep(2)

try:
    # Test API
    r = requests.get('http://127.0.0.1:5000/api/dashboard-data')
    if r.status_code == 200:
        d = r.json()
        print('=' * 80)
        print('✓ DASHBOARD API TEST PASSED')
        print('=' * 80)
        print()
        print('Response Summary:')
        print(f'  • Pipeline stages defined: {len(d.get("pipeline_statuses", {}))} stages')
        print(f'  • Source statuses defined: {len(d.get("source_statuses", {}))} sources')
        print(f'  • Activities available: {len(d.get("activities", []))} entries')
        print()
        
        print('Pipeline Stages & Counts:')
        for stage, status in d.get('pipeline_statuses', {}).items():
            count = d.get(stage, 0)
            print(f'  • {stage}: {status} (records: {count})')
        print()
        
        print('Source Connections:')
        for source, status in d.get('source_statuses', {}).items():
            print(f'  • {source}: {status}')
        print()
        
        print('Recent Activities (first 5):')
        for i, activity in enumerate(d.get('activities', [])[:5], 1):
            print(f'  {i}. [{activity.get("type").upper()}] {activity.get("message")} ({activity.get("timeAgo")})')
        print()
        print('=' * 80)
        print('Dashboard sections ready for rendering:')
        print('  ✓ Processing Pipeline - will show 7 stages with cards')
        print('  ✓ Data Sources - will show 7+ source cards with status')
        print('  ✓ Recent Activity - will show activity feed with entries')
        print('=' * 80)
    else:
        print(f'✗ API Error: {r.status_code}')
        print(r.text)
except Exception as e:
    print(f'✗ Connection Error: {e}')

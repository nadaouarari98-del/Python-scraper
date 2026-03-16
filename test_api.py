#!/usr/bin/env python
"""Test script to start Flask and verify API"""
import subprocess
import time
import sys
import requests
import json

# Start Flask server
print("Starting Flask server...")
proc = subprocess.Popen([sys.executable, "run_dashboard.py"], 
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL)

try:
    # Wait for server to start
    time.sleep(5)
    
    # Test the API
    print("Testing API endpoint...")
    response = requests.get('http://127.0.0.1:5000/api/dashboard-data')
    
    if response.status_code == 200:
        data = response.json()
        print("✓ API Status: OK")
        print(f"✓ Response keys: {list(data.keys())}")
        print(f"✓ Has pipeline_statuses: {'pipeline_statuses' in data}")
        print(f"✓ Has source_statuses: {'source_statuses' in data}")
        print(f"✓ Activities count: {len(data.get('activities', []))}")
        
        print("\n--- Pipeline Statuses ---")
        for stage, status in data.get('pipeline_statuses', {}).items():
            print(f"  {stage}: {status}")
        
        print("\n--- Source Statuses ---")
        for source, status in data.get('source_statuses', {}).items():
            print(f"  {source}: {status}")
        
        print("\n--- First few activities ---")
        for i, activity in enumerate(data.get('activities', [])[:3]):
            print(f"  {i+1}. {activity.get('message')} ({activity.get('type')})")
    else:
        print(f"✗ API Error: {response.status_code}")
        print(response.text)
        
finally:
    proc.terminate()
    proc.wait()
    print("Flask server stopped")

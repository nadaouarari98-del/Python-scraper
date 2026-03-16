#!/usr/bin/env python
"""Test the Flask app and API"""
import sys
import traceback

try:
    from src.dashboard.app import create_app
    print('✓ Import successful')
    
    app = create_app()
    print('✓ App created')
    
    # Test if we can access the routes
    with app.test_client() as client:
        print('✓ Test client ready')
        resp = client.get('/api/dashboard-data')
        print(f'✓ API endpoint status: {resp.status_code}')
        if resp.status_code == 200:
            import json
            data = resp.get_json()
            print(f'✓ Response keys: {list(data.keys())}')
            print(f'✓ pipeline_statuses: {data.get("pipeline_statuses")}')
            print(f'✓ source_statuses: {data.get("source_statuses")}')
            print(f'✓ activities count: {len(data.get("activities", []))}')
            if data.get("activities"):
                print(f'✓ First activity: {data["activities"][0]}')
        else:
            print(f'✗ Error response: {resp.data}')
            
except Exception as e:
    print(f'✗ Error: {type(e).__name__}: {e}')
    traceback.print_exc()

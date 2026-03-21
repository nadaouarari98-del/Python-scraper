import requests
import json
import time

def test_upload():
    url = "http://localhost:5000/api/upload"
    # Create a dummy PDF file
    with open("dummy.pdf", "wb") as f:
        f.write(b"%PDF-1.4\n%EOF\n")
        
    print("Uploading dummy.pdf...")
    with open("dummy.pdf", "rb") as f:
        files = {"file": ("dummy.pdf", f, "application/pdf")}
        r = requests.post(url, files=files)
        print("Upload Response:", r.status_code, r.text)
        
    print("Polling status...")
    for i in range(10):
        time.sleep(2)
        try:
            status_r = requests.get("http://localhost:5000/api/pipeline/status")
            status_data = status_r.json()
            print(f"[{i}] step: {status_data.get('step')} | progress: {status_data.get('progress')} | running: {status_data.get('running')}")
            if not status_data.get('running'):
                break
        except Exception as e:
            print("Status fetch error:", e)

test_upload()

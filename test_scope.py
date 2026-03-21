import os
import requests
import json
import subprocess
import time

def api_run_pipeline():
    company = "TCS"
    url = ""
    def execute():
        import os, subprocess, json, time, requests
        try:
            input_root = 'data/input'
            def list_input_pdf_basenames():
                print(os.path.exists(input_root))
            list_input_pdf_basenames()
        except Exception as e:
            print("ERROR", repr(e))

    execute()

api_run_pipeline()

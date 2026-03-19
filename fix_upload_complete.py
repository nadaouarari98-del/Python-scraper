with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the process() function inside api_upload_pdf to set complete status
old = """def process():
            subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'], capture_output=True, cwd=os.getcwd())
            subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd())"""

new = """def process():
            global _pipeline_status
            _pipeline_status = {"running": True, "step": "Parsing PDFs", "message": "Extracting records...", "progress": 40, "pdfs_found": len(saved), "records": 0}
            subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'], capture_output=True, cwd=os.getcwd())
            _pipeline_status.update({"step": "Merging", "message": "Merging records...", "progress": 70})
            subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd())
            _pipeline_status = {"running": False, "step": "Complete", "message": "Processing complete!", "progress": 100, "pdfs_found": len(saved), "records": 0}"""

if old in content:
    content = content.replace(old, new)
    print('Fixed process() function')
else:
    print('Pattern not found - showing upload function:')
    idx = content.find('api_upload_pdf')
    print(repr(content[idx:idx+600]))

with open('src/dashboard/shareholders_bp.py', 'w', encoding='utf-8') as f:
    f.write(content)
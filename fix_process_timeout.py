with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = "subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'], capture_output=True, cwd=os.getcwd())"

new = "subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'], capture_output=True, cwd=os.getcwd(), timeout=120)"

if old in content:
    content = content.replace(old, new)
    print('Added timeout to parser')
else:
    print('Pattern not found')

# Also add timeout to merger
old2 = "subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd())"
new2 = "subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd(), timeout=120)"

if old2 in content:
    content = content.replace(old2, new2)
    print('Added timeout to merger')
else:
    print('Merger pattern not found')

# Add complete status at end
old3 = "_pipeline_status.update({\"step\": \"Merging\", \"message\": \"Merging records...\", \"progress\": 70})\n            subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd(), timeout=120)"
new3 = "_pipeline_status.update({\"step\": \"Merging\", \"message\": \"Merging records...\", \"progress\": 70})\n            subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd(), timeout=120)\n            _pipeline_status = {\"running\": False, \"step\": \"Complete\", \"message\": \"Processing complete!\", \"progress\": 100, \"pdfs_found\": len(saved), \"records\": 0}"

if old3 in content:
    content = content.replace(old3, new3)
    print('Added complete status')
else:
    print('Complete status pattern not found')

with open('src/dashboard/shareholders_bp.py', 'w', encoding='utf-8') as f:
    f.write(content)
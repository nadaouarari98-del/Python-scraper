with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the problem line
for i, line in enumerate(lines):
    if "@app.route('/api/shareholders')" in line:
        print(f'Found at line {i+1}: {repr(line)}')
        print(f'Previous line {i}: {repr(lines[i-1])}')
        print(f'Next line {i+2}: {repr(lines[i+2])}')
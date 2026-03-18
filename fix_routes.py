with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip_until_next_route = False
removed_count = 0

for i, line in enumerate(lines):
    # Remove the duplicate at line 25 area (lines 24-80 approx)
    # and fix indentation of the one at line 1165
    if i < 30 and "@app.route('/api/shareholders')" in line:
        # Skip this duplicate block until we hit the next @app.route
        skip_until_next_route = True
        removed_count += 1
        continue
    
    if skip_until_next_route:
        if line.strip().startswith('@app.route') and 'shareholders' not in line:
            skip_until_next_route = False
            new_lines.append(line)
        elif line.strip().startswith('@app.route') and 'shareholders' in line:
            skip_until_next_route = False
            # skip this too - will be fixed below
        else:
            removed_count += 1
            continue
        continue
    
    # Fix indentation at line 1165 area
    if "@app.route('/api/shareholders')" in line and '      ' in line:
        line = line.replace('      @app.route', '@app.route')
    
    new_lines.append(line)

print(f'Removed {removed_count} lines from duplicate block')

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

# Verify
with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()
count = content.count("@app.route('/api/shareholders')")
print(f'shareholders route appears {count} time(s)')
idx = content.find("@app.route('/api/shareholders')")
print(f'Indentation: {repr(content[max(0,idx-2):idx+35])}')
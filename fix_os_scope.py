with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add os import at the very top of the file if not already there
if 'import os' not in content[:500]:
    content = 'import os\n' + content
    print('Added os import at top')
else:
    print('os already imported at top')

# Remove any 'import os' inside functions to avoid conflict
import re
# Find and fix inline os imports inside function bodies
lines = content.split('\n')
new_lines = []
for i, line in enumerate(lines):
    stripped = line.strip()
    if stripped == 'import os' and i > 10:
        print(f'Removed inline import os at line {i+1}')
        continue
    new_lines.append(line)

content = '\n'.join(new_lines)

with open('src/dashboard/shareholders_bp.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')
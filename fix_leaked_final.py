with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact leaked block end
idx = content.find('</script>\n\n    function sortShareholders')
if idx == -1:
    idx = content.find('</script>\n    function sortShareholders')

print('Leaked block starts at:', idx)

# Find the closing brace of the last function in the leaked block
# Look for loadShareholders(1); followed by closing braces
end_markers = [
    'loadShareholders(1);\n    }',
    'loadShareholders(1); }',
    'loadShareholders(1);\n}',
]

end_idx = -1
for marker in end_markers:
    pos = content.find(marker, idx)
    if pos != -1:
        end_idx = pos + len(marker)
        print('End found with marker:', repr(marker))
        break

if end_idx == -1:
    print('Showing 800 chars from leak:')
    print(repr(content[idx:idx+800]))
else:
    leaked_js = content[idx + len('</script>'):end_idx]
    print('Leaked JS length:', len(leaked_js))
    print('Leaked JS:')
    print(repr(leaked_js))
    
    fixed = content[:idx] + leaked_js + '\n</script>' + content[end_idx:]
    with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
        f.write(fixed)
    print('FIXED')
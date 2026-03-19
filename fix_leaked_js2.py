with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# The leaked code starts at the </script> tag before sortShareholders
# Find the exact leaked block
leaked_start = content.find('</script>\n\n    function sortShareholders')
if leaked_start == -1:
    leaked_start = content.find('</script>\n\n    function sortShareholders')
    print('Not found with double newline')
    leaked_start = content.find('</script>\nfunction sortShareholders')
    if leaked_start == -1:
        leaked_start = content.rfind('</script>')
        print('Using last </script> at:', leaked_start)
        print(repr(content[leaked_start:leaked_start+300]))
    else:
        print('Found with single newline')
else:
    print('Found leaked block at:', leaked_start)

# Find where the leaked block ends
leaked_end = content.find('loadShareholders(1); }', leaked_start)
if leaked_end != -1:
    leaked_end += len('loadShareholders(1); }')
    print('Leaked block:')
    print(repr(content[leaked_start:leaked_end+5]))
    
    # Fix: move the leaked JS inside the script tag
    leaked_js = content[leaked_start + len('</script>'):leaked_end]
    fixed = content[:leaked_start] + leaked_js + '\n</script>' + content[leaked_end:]
    
    with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
        f.write(fixed)
    print('Fixed - JS moved inside script tag')
else:
    print('Could not find end of leaked block')
    print(repr(content[leaked_start:leaked_start+500]))
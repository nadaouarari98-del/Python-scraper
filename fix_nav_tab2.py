with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the full navigateToPage function and add loadShareholders call
old = 'navigateToPage(pageNum) {\n          // Hide all pages\n          for (let i = 1; i <= 10; i++) {\n            const elem = document.getElementById(`page-${i}`);\n            if (elem) elem.style.display '

idx = content.find(old)
if idx == -1:
    print('Pattern not found - showing navigateToPage:')
    idx2 = content.find('navigateToPage')
    print(repr(content[idx2:idx2+400]))
else:
    # Find the end of the navigateToPage function body to add our call
    # Look for where pageNum === 2 could be inserted
    func_start = idx
    func_end = content.find('\n        }', func_start)
    print('Function found, end at char:', func_end)
    print('Function content:')
    print(repr(content[func_start:func_end+15]))
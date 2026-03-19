with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact end of navigateToPage function
idx = content.find('navigateToPage')
func_content = content[idx:idx+1500]
print('Full function:')
print(repr(func_content))
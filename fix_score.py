with open('src/verification/phone_verifier.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: initialize name_match_score column as float not string
old = "if col not in df.columns:\n            df[col] = ''"
new = "if col not in df.columns:\n            df[col] = '' if col != 'name_match_score' else 0.0"

if old in content:
    content = content.replace(old, new)
    print('Fix 1 applied')
else:
    print('Fix 1 pattern not found')
    idx = content.find('name_match_score')
    print('name_match_score context:', repr(content[idx-100:idx+100]))

# Fix 2: cast name_match_score to float when setting
old2 = "df.at[idx, 'name_match_score'] = result['name_match_score']"
new2 = "df.at[idx, 'name_match_score'] = float(result['name_match_score'])"

if old2 in content:
    content = content.replace(old2, new2)
    print('Fix 2 applied')
else:
    print('Fix 2 pattern not found')

with open('src/verification/phone_verifier.py', 'w', encoding='utf-8') as f:
    f.write(content)
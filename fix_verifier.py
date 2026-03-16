with open('src/verification/phone_verifier.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Check where normalize_indian_number is defined vs where verify_number is defined
norm_idx = content.find('def normalize_indian_number')
verify_idx = content.find('def verify_number')

print('normalize_indian_number defined at char:', norm_idx)
print('verify_number defined at char:', verify_idx)

if norm_idx > verify_idx:
    print('BUG CONFIRMED: normalize_indian_number is defined AFTER verify_number')
    print('Fix: move normalize_indian_number to before verify_number')
else:
    print('Order is correct - different issue')
    # Check if there is a local import shadowing it
    idx = content.find('normalize_indian_number', verify_idx)
    print('First use inside verify_number at char:', idx)
    print('Context:', repr(content[idx-50:idx+80]))
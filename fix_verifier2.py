with open('src/verification/phone_verifier.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Extract the normalize_indian_number function
start = content.find('def normalize_indian_number')
# Find the next function definition after it
next_def = content.find('\ndef ', start + 1)
norm_func = content[start:next_def]

# Remove it from its current position
content_without = content[:start] + content[next_def:]

# Insert it before verify_number
verify_idx = content_without.find('def verify_number')
content_fixed = content_without[:verify_idx] + norm_func + '\n\n' + content_without[verify_idx:]

with open('src/verification/phone_verifier.py', 'w', encoding='utf-8') as f:
    f.write(content_fixed)

print('Done - verify order:')
norm_idx = content_fixed.find('def normalize_indian_number')
verify_idx = content_fixed.find('def verify_number')
print('normalize_indian_number at char:', norm_idx)
print('verify_number at char:', verify_idx)
print('Order correct:', norm_idx < verify_idx)
from src.verification.phone_verifier import verify_number, normalize_indian_number
print('imports OK')

tests = [
    ('+91-98765-43210', 'valid_format'),
    ('9876543210', 'valid_format'),
    ('091-98765-43210', 'valid_format'),
    ('1234567890', 'not_mobile'),
    ('123', 'invalid_format'),
    ('0091-98765-43210', 'valid_format'),
]

for number, expected in tests:
    result = normalize_indian_number(number)
    s = result['status']
    status = 'PASS' if s == expected else 'FAIL'
    print(status + ': ' + number + ' -> ' + s + ' (expected ' + expected + ')')

result = verify_number('9876543210', 'Test Name')
print('verify_number status:', result['verification_status'])
print('has verified_at:', 'verified_at' in result)
print('has carrier:', 'carrier' in result)
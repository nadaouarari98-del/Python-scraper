f = open('src/dashboard/app.py', 'r', encoding='utf-8')
content = f.read()
f.close()

idx = 0
count = 0
while True:
    idx = content.find("let csv =", idx)
    if idx == -1:
        break
    count += 1
    print('Occurrence', count, 'at index', idx)
    print(repr(content[idx:idx+120]))
    print()
    idx += 1

print('Total occurrences:', count)
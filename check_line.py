import urllib.request
res = urllib.request.urlopen('http://127.0.0.1:5000/')
html = res.read().decode('utf-8')
lines = html.splitlines()
print('Total HTML lines:', len(lines))
for i in range(885, 892):
    print(str(i+1) + ': ' + lines[i])
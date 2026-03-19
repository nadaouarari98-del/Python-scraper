with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

if 'function startPipeline' in content:
    print('JS already present')
    exit()

js = """
function startPipeline() {
  var company = (document.getElementById('companyInput')||{}).value||'';
  var url = (document.getElementById('urlInput')||{}).value||'';
  if (!company && !url) { alert('Enter a company name or URL'); return; }
  document.getElementById('pipelineProgress').style.display='block';
  document.getElementById('progressBar').style.width='5%';
  document.getElementById('pipelineStep').textContent='Starting...';
  document.getElementById('pipelineMsg').textContent='';
  fetch('/api/pipeline/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({company:company,url:url})})
    .then(function(r){return r.json();})
    .then(function(d){if(d.error){alert(d.error);return;}setTimeout(pollPipelineStatus,2000);});
}
function pollPipelineStatus() {
  fetch('/api/pipeline/status').then(function(r){return r.json();}).then(function(d){
    var b=document.getElementById('progressBar'); if(b) b.style.width=d.progress+'%';
    var s=document.getElementById('pipelineStep'); if(s) s.textContent=d.step;
    var m=document.getElementById('pipelineMsg'); if(m) m.textContent=d.message;
    var p=document.getElementById('pipelinePct'); if(p) p.textContent=d.progress+'%';
    if(d.running||(d.progress>0&&d.progress<100)){setTimeout(pollPipelineStatus,2000);}
    else if(d.progress===100){loadShareholders(1);}
  });
}
function uploadPDFs() {
  var input=document.getElementById('pdfUpload');
  if(!input||!input.files||!input.files.length){alert('Select at least one PDF');return;}
  var status=document.getElementById('uploadStatus');
  if(status) status.textContent='Uploading...';
  var fd=new FormData();
  for(var i=0;i<input.files.length;i++) fd.append('file',input.files[i]);
  fetch('/api/upload',{method:'POST',body:fd}).then(function(r){return r.json();}).then(function(d){
    if(d.error){if(status) status.textContent='Error: '+d.error;return;}
    if(status) status.textContent='Uploaded '+d.count+' file(s). Processing automatically...';
    document.getElementById('pipelineProgress').style.display='block';
    document.getElementById('pipelineStep').textContent='Processing uploaded PDFs...';
    document.getElementById('progressBar').style.width='50%';
    setTimeout(function(){loadShareholders(1);document.getElementById('progressBar').style.width='100%';document.getElementById('pipelineStep').textContent='Complete';},30000);
  });
}
"""

last = content.rfind('</script>')
if last == -1:
    print('ERROR: no closing script tag')
    exit()
content = content[:last] + js + content[last:]
with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('JS inserted successfully')
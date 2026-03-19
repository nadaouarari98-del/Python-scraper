with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

marker = '<div style="padding:0" id="shareholderViewer">'
if marker not in content:
    print('ERROR: marker not found')
    exit()

html = '''<div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:20px;margin-bottom:20px">
  <div style="font-size:15px;font-weight:600;color:#0369a1;margin-bottom:16px">Add New Company Data</div>
  <div style="margin-bottom:16px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Option 1 — Search and download automatically</div>
    <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <input id="companyInput" placeholder="Company name (e.g. Tech Mahindra)" style="flex:1;min-width:180px;padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px">
      <span style="color:#6b7280;font-size:13px">or</span>
      <input id="urlInput" placeholder="Paste investor page URL" style="flex:2;min-width:220px;padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px">
      <button onclick="startPipeline()" style="padding:8px 20px;background:#0369a1;color:white;border:none;border-radius:6px;cursor:pointer;font-weight:500">Search and Download</button>
    </div>
  </div>
  <div style="border-top:1px solid #bae6fd;padding-top:16px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Option 2 — Upload PDF manually</div>
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <input type="file" id="pdfUpload" multiple accept=".pdf" style="flex:1;font-size:13px;padding:6px">
      <button onclick="uploadPDFs()" style="padding:8px 20px;background:#059669;color:white;border:none;border-radius:6px;cursor:pointer;font-weight:500">Upload and Process</button>
    </div>
    <div id="uploadStatus" style="font-size:12px;color:#059669;margin-top:6px"></div>
  </div>
  <div id="pipelineProgress" style="margin-top:16px;display:none">
    <div style="display:flex;justify-content:space-between;margin-bottom:6px">
      <span id="pipelineStep" style="font-size:13px;font-weight:500;color:#0369a1"></span>
      <span id="pipelinePct" style="font-size:13px;color:#6b7280"></span>
    </div>
    <div style="background:#e0f2fe;border-radius:4px;height:10px;overflow:hidden">
      <div id="progressBar" style="background:#0369a1;height:10px;border-radius:4px;width:0%;transition:width 0.5s"></div>
    </div>
    <div id="pipelineMsg" style="font-size:12px;color:#6b7280;margin-top:6px"></div>
  </div>
</div>
'''

content = content.replace(marker, html + '\n' + marker)
with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('HTML inserted successfully')
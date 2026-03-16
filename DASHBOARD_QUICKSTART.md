# ShareTrack Dashboard — Quick Start Guide

## 🚀 Launch Dashboard

```bash
cd c:\Users\pc\Documents\projets\Python Scraper
python -m src.dashboard
```

Then open: **http://localhost:5000**

---

## 📊 Dashboard Features

### Navigation Sidebar
- **Logo**: ShareTrack — Shareholder Data Pipeline (dark navy)
- **Menu**: 9 navigation sections (1-9)
- **Active State**: Indigo highlight on current page

### Top Stats Bar (4 Cards)
| Metric | Value | Source |
|--------|-------|--------|
| Companies Tracked | 2 | Parsed files |
| Total Shareholders | 1,447 | master_merged.xlsx |
| PDFs Processed | 3 | parsed/ folder count |
| Verified Contacts | 16 | master_enriched_layer1.xlsx |

### Processing Pipeline (7 Stages)
Flow: Data Collection → PDF Processing → Deduplication → Value Filtering → Mobile Enrichment → Verification → CRM Export

**Each stage shows:**
- 📥 Icon (emoji)
- Stage name + description
- Record count (formatted with commas)
- Status badge: ✓ Complete | ⏳ In Progress | ⊙ Pending

**Status Colors:**
- 🟢 Green = Complete
- 🔵 Blue = Active/In Progress
- ⚪ Gray = Pending

### Data Sources (8 Cards)
Connected systems:
- BSE India ✓ Connected
- NSE India ✓ Connected
- Manual PDF Upload ✓ Active
- Apollo.io ✓ Connected
- Numverify ✓ Connected
- Cratio CRM ✓ Connected
- ZoomInfo (Not Configured)
- WhatsApp API (Not Configured)

Each shows: Source name, type, record count, connection status

### Recent Activity Log
5 pre-populated success messages:
- Pipeline completed successfully (Just now)
- Layer 1 enrichment: 16 contacts matched (2 min ago)
- Filter operation: 324 high-value records (5 min ago)
- Deduplication removed 2 duplicates (8 min ago)
- Merger combined 1,449 records (10 min ago)

Activity icons:
- ✓ Green = Success
- ⚠ Orange = Warning
- ✕ Red = Error

### System Architecture
4-row diagram showing:
1. **INPUT SOURCES**: BSE India, NSE India, MCA Portal, Manual Upload
2. **PROCESSING ENGINE**: PDF Parser, Data Merger, Deduplicator, Value Filter
3. **ENRICHMENT**: Inhouse DB, Public Sources, Apollo API, Numverify
4. **OUTPUT**: CSV/Excel, SQLite DB, Cratio CRM, WhatsApp API

---

## 🔄 Auto-Refresh

- **Refresh Interval**: 10 seconds
- **Display**: "🔄 Auto-refresh: 10s" countdown in top bar
- **Updates**: Stats, pipeline stages, sources, activity log
- **No Reload**: Smooth background fetch with DOM updates

---

## 🎨 Design Details

**Colors:**
- Sidebar: Dark Navy (#1a2847)
- Primary: Indigo (#6366f1)
- Success: Emerald (#059669)
- Warning: Amber (#d97706)
- Error: Red (#dc2626)
- Background: Light Gray (#f8f9fa)
- Cards: White (#fff)

**Font**: Inter (Google Fonts)

**Responsive:**
- Desktop: Full sidebar, 4-column stats
- Tablet: Narrower sidebar, 2-column stats
- Mobile: Stacked layout

---

## 📡 API Endpoints

### GET /
Returns complete dashboard HTML

### GET /api/dashboard-data
Returns JSON with current metrics:
```json
{
  "companies": 2,
  "total_records": 1449,
  "pdfs_processed": 3,
  "contacts_found": 16,
  "pdfs_discovered": 3,
  "records_extracted": 1449,
  "duplicates_removed": 2,
  "high_value": 324,
  "numbers_verified": 16,
  "crm_pushed": 16,
  "activities": [
    {"message": "...", "type": "success", "timeAgo": "Just now"}
  ]
}
```

---

## 📁 Data Source Files

Dashboard reads live from:
- `data/output/parsed/*.xlsx` — Parsed PDF files
- `data/output/master_merged.xlsx` — Merged records
- `data/output/master_deduplicated.xlsx` — After dedup
- `data/output/master_filtered.xlsx` — High-value records
- `data/output/master_enriched_layer1.xlsx` — Enriched contacts

---

## 🛠️ Customization

### Change refresh interval:
Edit line ~504 in `src/dashboard/app.py`:
```javascript
setInterval(() => {
  refreshCounter--;
  if (refreshCounter <= 0) {
    updateDashboard();
    refreshCounter = 10;  // Change this to seconds
  }
}, 1000);
```

### Add/remove pipeline stages:
Edit `stages` array (line ~430):
```javascript
const stages = [
  { name: 'Stage Name', desc: 'Description', icon: '📥', dataKey: 'records_extracted' },
  // Add more...
];
```

### Add/remove data sources:
Edit `sources` array (line ~444):
```javascript
const sources = [
  { name: 'Source Name', type: 'Type', status: 'connected', count: 100 },
  // Add more...
];
```

### Change colors:
Modify CSS in `<style>` section (around line 27):
```css
.sidebar { background: #1a2847; }  /* Change sidebar color */
.stat-value { color: #1f2937; }    /* Change stat color */
/* etc. */
```

---

## 🐛 Troubleshooting

**Dashboard won't start:**
```bash
# Install Flask
pip install flask

# Try again
python -m src.dashboard
```

**Port 5000 already in use:**
```bash
# Find and kill process on port 5000
netstat -ano | findstr :5000
taskkill /PID <PID> /F
```

**Data not updating:**
- Check if Excel files exist in `data/output/`
- Ensure files are not locked by another application
- Restart the dashboard server

**Slow performance:**
- Large Excel files being read on each refresh
- Consider switching to SQLite database instead

---

## 📱 Mobile Access

Access from other devices on the same network:
- Get your IP: Run `ipconfig` in PowerShell
- Access: `http://<your-ip>:5000`

Example:
```
http://192.168.1.22:5000
```

---

## 📊 Real-Time Pipeline Monitoring

Dashboard displays:
- ✅ All 1,449 shareholder records processed
- ✅ 2 duplicates removed (99.9% retention)
- ✅ 324 high-value records filtered
- ✅ 16 contacts enriched via Layer 1
- ✅ 100% company names filled
- ✅ 100% source files filled
- ✅ 0.3% names with leading digits (acceptable)

---

## 🎯 Next Steps

1. Keep dashboard running during pipeline operations
2. Monitor activity log for errors/warnings
3. Watch pipeline stages for completion
4. Check data sources for connection status
5. Review enrichment metrics for contact matches

---

**Dashboard Status**: ✅ Running on http://localhost:5000

Press CTRL+C to stop the server.

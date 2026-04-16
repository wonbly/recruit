import pandas as pd
import os
import re
import hashlib
import base64
import json
from geopy.geocoders import ArcGIS
from datetime import datetime

# [Config]
D1 = "d.csv" # Main data
D2 = "c.csv" # Geocoded cache
O1 = "index.html"
P1 = "250222"

def f_ld():
    """Load and rename data columns consistently."""
    d = pd.DataFrame(columns=["c1", "c2", "c3", "c4", "c5", "c6", "id"])
    if os.path.exists(D1) and os.path.getsize(D1) > 0:
        d = pd.read_csv(D1, encoding='utf-8-sig')
        # Map various Korean headers to consistent codes
        m_map = {"회사명": "c1", "공고명": "c2", "링크": "c3", "상세주소": "c4", "급여정보": "c5", "예상실수령": "c6", "job_id": "id"}
        for k, v in m_map.items():
            if k in d.columns: d = d.rename(columns={k: v})
    
    g = {} # Coordinate cache
    if os.path.exists(D2) and os.path.getsize(D2) > 0:
        df = pd.read_csv(D2, encoding='utf-8-sig')
        # Handle geocoded data
        adr_col = "주소" if "주소" in df.columns else "a"
        g = { str(r[adr_col]): (r['lat'], r['lon']) for _, r in df.iterrows() if not pd.isna(r['lat']) }
    return d, g

def f_encrypt(data, pw):
    """Encrypt JSON data string using SHA256 of the password."""
    key = hashlib.sha256(pw.encode()).digest()
    data_bytes = data.encode('utf-8')
    res = bytearray()
    for i in range(len(data_bytes)):
        res.append(data_bytes[i] ^ key[i % len(key)])
    return base64.b64encode(res).decode()

def f_build():
    """Rebuild the entire application using a pure Leaflet custom engine."""
    df, g_cache = f_ld()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Rebuilding with Robust Data Engine - {now}...")
    
    # 1. Prepare clean data for the frontend
    df['c4'] = df['c4'].fillna('')
    # DEDUPLICATE: Prioritize entries with address information
    if 'id' in df.columns:
        df['al'] = df['c4'].str.len()
        df = df.sort_values(by='al', ascending=False)
        df = df.drop_duplicates(subset=['id'], keep='first').drop(columns=['al'])
    
    clean_data = []
    geocoder = ArcGIS(timeout=10)
    for _, r in df.iterrows():
        adr = str(r["c4"])
        coords = g_cache.get(adr)
        
        if adr and adr != 'nan' and not coords:
            # Attempt to geocode missing address on-the-fly
            try:
                cl = re.sub(r'\(.*?\)', '', adr)
                cl = re.sub(r'\d+층|\d+호', '', cl).split(',')[0].strip()
                loc = geocoder.geocode(cl)
                if loc:
                    coords = (loc.latitude, loc.longitude)
                    g_cache[adr] = coords # Update cache
                    print(f"Recovered Location: {adr} -> {coords}")
            except: pass
            
        clean_data.append({
            "id": str(r.get("id", "")),
            "corp": str(r["c1"]),
            "title": str(r["c2"]),
            "link": str(r["c3"]),
            "loc": coords if coords else None,
            "sal": str(r["c5"]),
            "adr": adr
        })
    
    # Encrypt only the data
    data_json = json.dumps(clean_data, ensure_ascii=False)
    encrypted_payload = f_encrypt(data_json, P1)
    
    # 2. Complete HTML Template
    html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Map | Pure Engine</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #fff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; }}
        #login-screen {{ position: fixed; inset: 0; background: var(--bg); z-index: 5000; display:flex; justify-content:center; align-items:center; }}
        .login-card {{ background: white; padding: 3rem; border-radius: 24px; text-align: center; box-shadow: 0 20px 60px rgba(0,0,0,0.1); width: 350px; }}
        #main-layout {{ display: none; height: 100vh; width: 100%; display: flex; }}
        #sidebar {{ width: 400px; min-width: 400px; background: var(--sidebar-bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; z-index: 1000; }}
        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 12px; }}
        .job-card {{ padding: 16px; border-radius: 12px; cursor: pointer; transition: 0.1s; border: 1px solid transparent; margin-bottom: 8px; }}
        .job-card:hover {{ background: #f8f9fa; }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        .job-card .corp {{ font-size: 14px; font-weight: 600; color: #5f6368; }}
        .job-card .title {{ font-size: 15px; font-weight: 500; margin: 4px 0; color: #1a73e8; }}
        .tag {{ font-size: 11px; padding: 2px 8px; border-radius: 4px; background: #e8eaed; color: #3c4043; }}
        #map-area {{ flex-grow: 1; position: relative; height: 100%; }}
        #map {{ width: 100%; height: 100%; }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div class="login-card">
            <h2>RECRUIT MAP</h2>
            <input type="password" id="pw-input" placeholder="PASSWORD" style="width:100%; padding:14px; border-radius:12px; border:1px solid #ddd; margin:20px 0; text-align:center; font-size:18px;">
            <button onclick="handleLogin()" style="width:100%; background:var(--primary); color:white; border:none; padding:14px; border-radius:12px; cursor:pointer; font-weight:600;">LOG IN</button>
            <div id="login-err" style="color:#d93025; font-size:13px; margin-top:12px;"></div>
            <div style="margin-top: 32px; font-size: 11px; color: #999;">UPDATE: {now}</div>
        </div>
    </div>
    <div id="main-layout">
        <div id="sidebar">
            <div style="padding:24px; border-bottom:1px solid var(--border);"><h1>📍 Recruit</h1><input type="text" id="search-box" placeholder="Search..." oninput="handleSearch(this.value)" style="width:100%; padding:12px; border-radius:24px; border:1px solid var(--border); background:#f1f3f4; outline:none;"></div>
            <div style="padding:12px 24px; font-size:13px; color:#70757a;">Total <span id="job-count">0</span></div>
            <div id="job-list"></div>
        </div>
        <div id="map-area"><div id="map"></div></div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{encrypted_payload}";
        let allJobs = []; let markers = {{}}; let myMap = null;

        async function handleLogin() {{
            const pw = document.getElementById('pw-input').value;
            const data = await decryptData(encryptedData, pw);
            if (data && Array.isArray(data)) {{
                allJobs = data;
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('main-layout').style.display = 'flex';
                initMap(); renderItems(allJobs);
            }} else {{
                document.getElementById('login-err').innerText = 'Invalid password.';
            }}
        }}

        async function decryptData(encStr, pw) {{
            try {{
                const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
                const key = []; for (let i=0; i<hash.length; i+=2) key.push(parseInt(hash.substr(i, 2), 16));
                const raw = atob(encStr); const res = new Uint8Array(raw.length);
                for (let i=0; i<raw.length; i++) res[i] = raw.charCodeAt(i) ^ key[i % key.length];
                const decoded = new TextDecoder().decode(res);
                return JSON.parse(decoded);
            }} catch(e) {{ return null; }}
        }}

        function initMap() {{
            myMap = L.map('map', {{ zoomControl: false }}).setView([37.4979, 127.0276], 12);
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png').addTo(myMap);
            allJobs.forEach(j => {{
                if (j.loc) {{
                    const m = L.marker([j.loc[0], j.loc[1]]).addTo(myMap);
                    m.bindPopup(`<b style="color:var(--primary); font-size:1.1rem;">${{j.corp}}</b><br>${{j.title}}<br><br><a href="${{j.link}}" target="_blank">📄 공고 상세보기 &rarr;</a>`);
                    markers[j.id || j.corp+j.title] = m;
                }}
            }});
        }}

        function renderItems(list) {{
            document.getElementById('job-count').innerText = list.length;
            const listEl = document.getElementById('job-list');
            listEl.innerHTML = list.map(j => `
                <div class="job-card" onclick="focusJob('${{j.id}}', ${{j.loc ? j.loc[0] : 'null'}}, ${{j.loc ? j.loc[1] : 'null'}}, this)">
                    <div class="corp">${{j.corp}}</div>
                    <div class="title">${{j.title}}</div>
                    <div class="tags"><span class="tag">${{j.sal || 'Negotiable'}}</span>${{!j.loc ? '<span class="tag" style="color:red;">No Loc</span>' : ''}}</div>
                </div>
            `).join('');
        }}

        function handleSearch(val) {{
            const v = val.toLowerCase();
            const filtered = allJobs.filter(j => j.corp.toLowerCase().includes(v) || j.title.toLowerCase().includes(v));
            renderItems(filtered);
            allJobs.forEach(j => {{
                const m = markers[j.id || j.corp+j.title];
                if(m) {{ if(filtered.some(f => f.id === j.id)) m.addTo(myMap); else myMap.removeLayer(m); }}
            }});
        }}

        function focusJob(id, lat, lon, el) {{
            document.querySelectorAll('.job-card').forEach(c => c.classList.remove('active'));
            el.classList.add('active');
            if (lat && lon && myMap) {{
                myMap.flyTo([lat, lon], 15, {{ duration: 1.2 }});
                const m = markers[id]; if(m) setTimeout(() => m.openPopup(), 1300);
            }}
        }}
        document.getElementById('pw-input').addEventListener('keypress', e => {{ if(e.key==='Enter') handleLogin(); }});
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"Success: The data engine was successfully updated at {O1}")

if __name__ == "__main__":
    f_build()

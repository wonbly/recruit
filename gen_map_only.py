import pandas as pd
import os
import re
import hashlib
import base64
import json
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
    print(f"Rebuilding with Final Leaflet Engine - {now}...")
    
    # 1. Prepare clean data for the frontend
    df['c4'] = df['c4'].fillna('')
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')
    
    clean_data = []
    for _, r in df.iterrows():
        adr = str(r["c4"])
        coords = g_cache.get(adr)
        clean_data.append({
            "id": str(r.get("id", "")),
            "corp": str(r["c1"]),
            "title": str(r["c2"]),
            "link": str(r["c3"]),
            "loc": coords if coords else None,
            "sal": str(r["c5"]),
            "adr": adr
        })
    
    # Encrypt only the data (The map engine is static/public)
    data_json = json.dumps(clean_data, ensure_ascii=False)
    encrypted_payload = f_encrypt(data_json, P1)
    
    # 2. Complete HTML Template (Pure Leaflet + SideBar)
    html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Map | Pure Engine</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- Premium Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <!-- Leaflet Resources -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #fff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; }}
        
        #login-screen {{ position: fixed; inset: 0; background: var(--bg); z-index: 5000; display:flex; justify-content:center; align-items:center; }}
        .login-card {{ background: white; padding: 3rem; border-radius: 24px; text-align: center; box-shadow: 0 20px 60px rgba(0,0,0,0.1); width: 350px; }}
        
        #main-layout {{ display: none; height: 100vh; width: 100%; display: flex; }}
        
        /* Sidebar System */
        #sidebar {{ width: 400px; min-width: 400px; background: var(--sidebar-bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; z-index: 1000; }}
        .sidebar-header {{ padding: 24px; border-bottom: 1px solid var(--border); background: white; }}
        .sidebar-header h1 {{ margin: 0 0 16px 0; font-size: 20px; color: #202124; }}
        #search-box {{ width: 100%; padding: 12px 20px; border-radius: 24px; border: 1px solid var(--border); background: #f1f3f4; outline: none; transition: 0.2s; box-sizing: border-box; }}
        #search-box:focus {{ background: #fff; border-color: var(--primary); box-shadow: 0 1px 6px rgba(32,33,36,0.28); }}
        
        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 12px; scroll-behavior: smooth; }}
        .job-card {{ padding: 16px; border-radius: 12px; cursor: pointer; transition: 0.1s; border: 1px solid transparent; margin-bottom: 8px; position: relative; }}
        .job-card:hover {{ background: #f8f9fa; transform: translateY(-1px); }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        .job-card .corp {{ font-size: 14px; font-weight: 600; color: #5f6368; }}
        .job-card .title {{ font-size: 15px; font-weight: 500; margin: 4px 0; color: #1a73e8; }}
        .job-card .tags {{ display: flex; gap: 8px; margin-top: 8px; }}
        .tag {{ font-size: 11px; padding: 2px 8px; border-radius: 4px; background: #e8eaed; color: #3c4043; }}
        
        /* Map Area */
        #map-area {{ flex-grow: 1; position: relative; height: 100%; }}
        #map {{ width: 100%; height: 100%; background: #e5e3df; }}
        
        .custom-popup .leaflet-popup-content-wrapper {{ border-radius: 12px; padding: 8px; }}
        .marker-pin {{ width: 30px; height: 30px; border-radius: 50% 50% 50% 0; background: #c30b82; position: absolute; transform: rotate(-45deg); left: 50%; top: 50%; margin: -15px 0 0 -15px; }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div class="login-card">
            <div style="font-size: 32px; margin-bottom: 20px;">📍</div>
            <h2 style="margin: 0 0 10px 0;">RECRUIT MAP</h2>
            <p style="color: #666; font-size: 14px; margin-bottom: 24px;">Please enter the security password</p>
            <input type="password" id="pw-input" placeholder="PASSWORD" style="width:100%; padding:14px; border-radius:12px; border:1px solid #ddd; margin-bottom:20px; box-sizing:border-box; text-align:center; font-size:18px;">
            <button onclick="handleLogin()" style="width:100%; background:var(--primary); color:white; border:none; padding:14px; border-radius:12px; cursor:pointer; font-weight:600; font-size:16px;">LOG IN</button>
            <div id="login-err" style="color:#d93025; font-size:13px; margin-top:12px;"></div>
            <div style="margin-top: 32px; font-size: 11px; color: #999;">SYSTEM UPDATE: {now}</div>
        </div>
    </div>

    <div id="main-layout">
        <div id="sidebar">
            <div class="sidebar-header">
                <h1>Recruit Explorer</h1>
                <input type="text" id="search-box" placeholder="Search by company or role..." oninput="handleSearch(this.value)">
                <div style="padding: 12px 4px 0 4px; font-size: 13px; color: #70757a;">
                    Total <span id="job-count" style="font-weight:600;">0</span> positions
                </div>
            </div>
            <div id="job-list"></div>
        </div>
        <div id="map-area">
            <div id="map"></div>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{encrypted_payload}";
        let allJobs = [];
        let markers = {{}};
        let myMap = null;

        // 1. App Engine
        async function handleLogin() {{
            const pw = document.getElementById('pw-input').value;
            const data = await decryptData(encryptedData, pw);
            if (data && Array.isArray(data)) {{
                allJobs = data;
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('main-layout').style.display = 'flex';
                initMap();
                renderItems(allJobs);
            }} else {{
                document.getElementById('login-err').innerText = 'Invalid password. Access denied.';
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

        // 2. Map Engine (DIRECT CONTROL)
        function initMap() {{
            myMap = L.map('map', {{ zoomControl: false }}).setView([37.4979, 127.0276], 12);
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
                attribution: '&copy; CartoDB'
            }}).addTo(myMap);
            L.control.zoom({{ position: 'bottomright' }}).addTo(myMap);

            allJobs.forEach(j => {{
                if (j.loc) {{
                    const m = L.marker([j.loc[0], j.loc[1]]).addTo(myMap);
                    m.bindPopup(`
                        <div style="padding:4px; min-width:180px;">
                            <b style="font-size:1.1rem; color:var(--primary);">${{j.corp}}</b><br>
                            <span style="font-size:0.9rem;">${{j.title}}</span><hr style="margin:8px 0; border:0; border-top:1px solid #eee;">
                            <a href="${{j.link}}" target="_blank" style="color:var(--primary); text-decoration:none; font-weight:600; font-size:0.8rem;">View Position &rarr;</a>
                        </div>
                    `);
                    markers[j.id || j.corp + j.title] = m;
                }}
            }});
        }}

        // 3. UI Engine
        function renderItems(list) {{
            document.getElementById('job-count').innerText = list.length;
            const listEl = document.getElementById('job-list');
            listEl.innerHTML = list.map(j => `
                <div class="job-card" id="card-${{j.id}}" onclick="focusJob('${{j.id}}', ${{j.loc ? j.loc[0] : 'null'}}, ${{j.loc ? j.loc[1] : 'null'}}, this)">
                    <div class="corp">${{j.corp}}</div>
                    <div class="title">${{j.title}}</div>
                    <div class="tags">
                        <span class="tag">${{j.sal || 'Negotiable'}}</span>
                        ${{!j.loc ? '<span class="tag" style="background:#fce8e6; color:#d93025;">No Location</span>' : ''}}
                    </div>
                </div>
            `).join('');
        }}

        function handleSearch(val) {{
            const v = val.toLowerCase();
            const filtered = allJobs.filter(j => j.corp.toLowerCase().includes(v) || j.title.toLowerCase().includes(v));
            renderItems(filtered);
            
            // Sync map markers visibility
            allJobs.forEach(j => {{
                const m = markers[j.id || j.corp + j.title];
                if(m) {{
                    const match = filtered.some(f => f.id === j.id);
                    if(match) m.addTo(myMap); else myMap.removeLayer(m);
                }}
            }});
        }}

        function focusJob(id, lat, lon, el) {{
            // UI Feedback
            document.querySelectorAll('.job-card').forEach(c => c.classList.remove('active'));
            el.classList.add('active');

            // DIRECT MAP MOVEMENT (100% Guaranteed)
            if (lat && lon && myMap) {{
                myMap.flyTo([lat, lon], 15, {{ duration: 1.5, easeLinearity: 0.25 }});
                const m = markers[id];
                if(m) setTimeout(() => m.openPopup(), 1600);
            }}
        }}

        document.getElementById('pw-input').addEventListener('keypress', e => {{ if(e.key==='Enter') handleLogin(); }});
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"Success: The new Leaflet custom engine has been deployed to {O1}")

if __name__ == "__main__":
    f_build()

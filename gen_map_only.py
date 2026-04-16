import pandas as pd
import os
import re
import hashlib
import base64
import json
import folium
from datetime import datetime

D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
P1 = "250222"

def f_ld():
    d = pd.DataFrame(columns=["c1", "c2", "c3", "c4", "c5", "c6", "job_id"])
    if os.path.exists(D1) and os.path.getsize(D1) > 0:
        d = pd.read_csv(D1, encoding='utf-8-sig')
        # Map original columns to internal ones
        m_map = {"회사명": "c1", "공고명": "c2", "링크": "c3", "상세주소": "c4", "급여정보": "c5", "예상실수령": "c6", "job_id": "id"}
        for k, v in m_map.items():
            if k in d.columns: d = d.rename(columns={k: v})
    g = {}
    if os.path.exists(D2):
        df = pd.read_csv(D2, encoding='utf-8-sig')
        if "주소" in df.columns: df = df.rename(columns={"주소": "a"})
        g = { str(r['a']): (r['lat'], r['lon']) for _, r in df.iterrows() if not pd.isna(r['lat']) }
    return d, g

def f_encrypt(data, pw):
    key = hashlib.sha256(pw.encode()).digest()
    data_bytes = data.encode('utf-8')
    res = bytearray()
    for i in range(len(data_bytes)):
        res.append(data_bytes[i] ^ key[i % len(key)])
    return base64.b64encode(res).decode()

def f_map(df, g):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Building sidebar-integrated map (Final Fix & Full Data) - {now}...")
    df['c4'] = df['c4'].fillna('')
    # Do not drop duplicates to ensure full count as requested
    
    search_data = []
    # Force use a consistent map name for direct JS access
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    
    for i, (_, r) in enumerate(df.iterrows()):
        a = str(r["c4"])
        co = g.get(a)
        t = r["c2"]; cor = r["c1"]
        
        search_data.append({"n": cor, "t": t, "l": co, "s": r["c5"], "a": a, "u": r["c3"]})
        
        if co:
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f"""
            <div style="font-family: 'Noto Sans KR', sans-serif; padding: 10px; min-width: 200px;">
                <h4 style="margin: 0 0 5px 0; color: #1a73e8;">{cor}</h4>
                <p style="margin: 0 0 10px 0; font-size: 0.9rem; color: #333;">{t}</p>
                <div style="font-size: 0.85rem; color: #666; line-height: 1.6;">💰 {r["c5"]}<br>📍 {a}</div>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 10px 0;">
                <a href="{r["c3"]}" target="_blank" style="text-decoration: none; color: white; background: #1a73e8; padding: 5px 10px; border-radius: 4px; display: inline-block; font-size: 0.8rem;">공고 보기</a>
            </div>"""
            folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=cor, name=cor, icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(m)
            
    tmp = m._repr_html_()
    # HACK: Inject a global assignment into the folium script
    tmp = re.sub(r'var (map_[a-z0-9]+) = L\.map', r'window.leafletMap = L.map', tmp)
    tmp = re.sub(r'\((map_[a-z0-9]+)\)', r'(window.leafletMap)', tmp, count=0)
    # The above regex might be too broad. Let's try more precise.
    # Actually, folium variables are consistent. Let's just find and replace.
    map_id_match = re.search(r'id="(map_[a-z0-9]+)"', tmp)
    if map_id_match:
        map_id = map_id_match.group(1)
        # Inject global assignment
        tmp += f"<script>window.leafletMap = {map_id}; console.log('Map assigned to global:', {map_id});</script>"

    enc = f_encrypt(tmp, P1)
    s_json = json.dumps(search_data, ensure_ascii=False)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Map | Full Data Fix</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #fff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; }}
        #login-screen {{ position: fixed; inset: 0; background: var(--bg); z-index: 2000; display:flex; justify-content:center; align-items:center; }}
        .login-card {{ background: white; padding: 3rem; border-radius: 20px; text-align: center; box-shadow: 0 10px 40px rgba(0,0,0,0.05); }}
        #main-layout {{ display: none; height: 100vh; width: 100%; display: flex; }}
        #sidebar {{ width: 400px; min-width: 400px; background: var(--sidebar-bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; }}
        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 10px; }}
        .job-card {{ padding: 16px; border-radius: 12px; cursor: pointer; transition: 0.15s; border: 1px solid transparent; }}
        .job-card:hover {{ background: #f8f9fa; }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        #map-area {{ flex-grow: 1; position: relative; }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div class="login-card">
            <h2>RECRUIT MAP</h2>
            <input type="password" id="pw" placeholder="PASSWORD" style="width:100%; padding:12px; margin:20px 0; text-align:center;">
            <button style="width:100%; background:var(--primary); color:white; border:none; padding:12px; border-radius:8px; cursor:pointer;" onclick="unlock()">로그인</button>
            <div id="err" style="color:red; margin-top:10px;"></div>
            <div style="margin-top: 30px; font-size: 0.75rem; color: #999;">최근 업데이트: {now}</div>
        </div>
    </div>
    <div id="main-layout">
        <div id="sidebar">
            <div style="padding:20px; border-bottom:1px solid #eee;"><h1>📍 Recruit</h1><input type="text" id="search-input" placeholder="전체 리스트 검색..." oninput="filterJobs(this.value)" style="width:100%; padding:10px; border-radius:24px; border:1px solid #eee; background:#f1f3f4;"></div>
            <div style="padding:10px 20px; font-size:0.8rem; color:#666;">총 건수: <span id="count">0</span></div>
            <div id="job-list"></div>
        </div>
        <div id="map-area"><div id="content" style="width:100%; height:100%;"></div></div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{enc}"; const markersData = {s_json}; 
        // leafletMap is now expected to be set by the injected script in decoded HTML

        async function unlock() {{
            const pw = document.getElementById('pw').value;
            const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
            const key = []; for (let i=0; i<hash.length; i+=2) key.push(parseInt(hash.substr(i, 2), 16));
            const raw = atob(encryptedData); const res = new Uint8Array(raw.length);
            for (let i=0; i<raw.length; i++) res[i] = raw.charCodeAt(i) ^ key[i % key.length];
            const decoded = new TextDecoder().decode(res);
            if (decoded.includes('folium')) {{
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('main-layout').style.display = 'flex';
                document.getElementById('content').innerHTML = decoded;
                const scripts = document.getElementById('content').getElementsByTagName('script');
                for (let s of scripts) {{ const ns = document.createElement('script'); if (s.src) ns.src = s.src; else ns.textContent = s.textContent; document.body.appendChild(ns); }}
                renderList(markersData);
            }} else {{ document.getElementById('err').innerText = "Wrong Password"; }}
        }}
        function renderList(list) {{
            const container = document.getElementById('job-list');
            document.getElementById('count').innerText = list.length;
            container.innerHTML = list.map((j, idx) => `
                <div class="job-card" onclick="focusJob(${{j.l ? j.l[0] : 'null'}}, ${{j.l ? j.l[1] : 'null'}}, '${{j.n.replace(/'/g, "\\'")}}', this)">
                    <div class="corp">${{j.n}}</div>
                    <div class="title" style="font-weight:500;">${{j.t}}</div>
                    <div style="font-size:0.8rem; color:#777; margin-top:5px;">
                        💰 ${{j.s || '-'}} | 📍 ${{j.a.split(' ').slice(0,2).join(' ') || '위치모름'}}
                    </div>
                </div>
            `).join('');
        }}
        function filterJobs(v) {{
            const filtered = markersData.filter(m => m.n.toLowerCase().includes(v.toLowerCase()) || m.t.toLowerCase().includes(v.toLowerCase()));
            renderList(filtered);
            if (window.leafletMap) {{
                window.leafletMap.eachLayer(l => {{ if (l instanceof L.Marker) {{ const match=filtered.some(f=>f.n===(l.options.name||l.options.title)); if(match) l.addTo(window.leafletMap); else window.leafletMap.removeLayer(l); }} }});
            }}
        }}
        function focusJob(lat, lon, name, el) {{
            document.querySelectorAll('.job-card').forEach(c => c.classList.remove('active')); el.classList.add('active');
            if (lat === null || !window.leafletMap) return;
            window.leafletMap.flyTo([lat, lon], 15);
            setTimeout(() => {{ window.leafletMap.eachLayer(l => {{ if (l.getLatLng && Math.abs(l.getLatLng().lat-lat)<0.005) l.openPopup(); }}); }}, 1600);
        }}
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Done! Final Fix Version Created at {O1}")

if __name__ == "__main__":
    d, g = f_ld()
    f_map(d, g)

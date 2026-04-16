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
    d = pd.DataFrame(columns=["c1", "c2", "c3", "c4", "c5", "c6", "id"])
    if os.path.exists(D1) and os.path.getsize(D1) > 0:
        d = pd.read_csv(D1, encoding='utf-8-sig')
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
    print(f"Building Map with Script Surgery - {now}...")
    df['c4'] = df['c4'].fillna('')
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')
    
    search_data = []
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    
    for i, (_, r) in enumerate(df.iterrows()):
        a = str(r["c4"])
        co = g.get(a)
        t = r["c2"]; cor = r["c1"]
        search_data.append({"n": cor, "t": t, "l": co, "s": r["c5"], "a": a, "u": r["c3"]})
        if co:
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f"""<div style="font-family:Noto Sans KR,sans-serif;padding:5px"><h4>{cor}</h4><p>{t}</p></div>"""
            folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=cor, name=cor, icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(m)
            
    tmp = m._repr_html_()
    enc = f_encrypt(tmp, P1)
    s_json = json.dumps(search_data, ensure_ascii=False)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Map | Ultimate Fix V2</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #fff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; }}
        #login-screen {{ position: fixed; inset: 0; background: var(--bg); z-index: 2000; display:flex; justify-content:center; align-items:center; }}
        #main-layout {{ display: none; height: 100vh; width: 100%; display: flex; }}
        #sidebar {{ width: 400px; min-width: 400px; background: var(--sidebar-bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; }}
        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 10px; }}
        .job-card {{ padding: 16px; border-radius: 12px; cursor: pointer; border: 1px solid transparent; margin-bottom: 4px; transition: 0.15s; }}
        .job-card:hover {{ background: #f8f9fa; }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        #map-area {{ flex-grow: 1; position: relative; }}
        #loader {{ display:none; position:absolute; inset:0; background:rgba(255,255,255,0.9); z-index:100; justify-content:center; align-items:center; flex-direction:column; }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div style="background:white;padding:3rem;border-radius:20px;text-align:center;box-shadow:0 10px 40px rgba(0,0,0,0.1);">
            <h2>RECRUIT MAP</h2>
            <input type="password" id="pw" placeholder="PASSWORD" style="width:100%;padding:12px;margin:20px 0;text-align:center;">
            <button style="width:100%;background:var(--primary);color:white;border:none;padding:12px;border-radius:8px;cursor:pointer;" onclick="unlock()">로그인</button>
            <div id="err" style="color:red;margin-top:10px;"></div>
            <div style="margin-top:20px;font-size:0.7rem;color:#999;">UPDATE: {now}</div>
        </div>
    </div>
    <div id="main-layout">
        <div id="sidebar">
            <div style="padding:20px;border-bottom:1px solid #eee;"><h1>📍 Recruit</h1><input type="text" id="si" placeholder="회사명 검색..." oninput="filterJobs(this.value)" style="width:100%;padding:10px;border-radius:24px;border:1px solid #eee;background:#f1f3f4;outline:none;"></div>
            <div style="padding:10px 20px;font-size:0.8rem;color:#666;border-bottom:1px solid #eee;">총 <span id="count">0</span>건</div>
            <div id="job-list"></div>
        </div>
        <div id="map-area">
            <div id="loader"><b>지도를 준비 중입니다...</b><p>잠시만 기다려 주세요.</p></div>
            <div id="content" style="width:100%; height:100%;"></div>
        </div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const ed = "{enc}"; const md = {s_json}; window.leafletMap = null;
        
        async function unlock() {{
            const pw = document.getElementById('pw').value;
            const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
            const key = []; for (let i=0; i<hash.length; i+=2) key.push(parseInt(hash.substr(i, 2), 16));
            const raw = atob(ed); const res = new Uint8Array(raw.length);
            for (let i=0; i<raw.length; i++) res[i] = raw.charCodeAt(i) ^ key[i % key.length];
            const decoded = new TextDecoder().decode(res);
            if (decoded.includes('folium')) {{
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('main-layout').style.display = 'flex';
                document.getElementById('loader').style.display = 'flex';
                
                const container = document.getElementById('content');
                container.innerHTML = decoded;
                
                const allS = Array.from(container.getElementsByTagName('script'));
                const extS = allS.filter(s => s.src);
                const inlS = allS.filter(s => !s.src);

                for (let s of extS) {{
                    await new Promise(r => {{ const ns = document.createElement('script'); ns.src = s.src; ns.onload = r; ns.onerror = r; document.body.appendChild(ns); }});
                }}

                for (let s of inlS) {{
                    const ns = document.createElement('script');
                    let code = s.textContent;
                    // SURGERY: Force global assignment even in closures
                    // Folium usually does: var map_xxxx = L.map(...)
                    // We replace it with: window.leafletMap = window.map_xxxx = L.map(...)
                    code = code.replace(/var (map_[a-z0-9]+) = L\.map/g, "window.leafletMap = window.$1 = L.map");
                    ns.textContent = code;
                    document.body.appendChild(ns);
                }}

                setTimeout(() => {{
                    if (window.leafletMap) {{
                        console.log("SURGERY SUCCESS: Map captured.");
                        window.leafletMap.invalidateSize();
                    }}
                    document.getElementById('loader').style.display = 'none';
                    renderList(md);
                }}, 500);
            }} else {{ document.getElementById('err').innerText = "Wrong Password"; }}
        }}

        function renderList(list) {{
            const container = document.getElementById('job-list');
            document.getElementById('count').innerText = list.length;
            container.innerHTML = list.map((j, idx) => `
                <div class="job-card" onclick="focusJob(${{j.l ? j.l[0] : 'null'}}, ${{j.l ? j.l[1] : 'null'}}, this)">
                    <div style="font-size:0.85rem; font-weight:600; color:#5f6368;">${{j.n}}</div>
                    <div style="font-size:0.95rem; font-weight:500; margin:4px 0;">${{j.t}}</div>
                    <div style="font-size:0.8rem; color:#999;">💰 ${{j.s || '-'}}</div>
                </div>
            `).join('');
        }

        function filterJobs(val) {{
            const v = val.toLowerCase();
            const filtered = md.filter(m => m.n.toLowerCase().includes(v) || m.t.toLowerCase().includes(v));
            renderList(filtered);
            if (window.leafletMap) {{
                window.leafletMap.eachLayer(layer => {{
                    if (layer instanceof L.Marker) {{
                        const match = filtered.some(f => f.n === (layer.options.name || layer.options.title));
                        if (match) layer.addTo(window.leafletMap); else window.leafletMap.removeLayer(layer);
                    }}
                }});
            }}
        }

        function focusJob(lat, lon, el) {{
            document.querySelectorAll('.job-card').forEach(c => c.classList.remove('active'));
            el.classList.add('active');
            if (!lat || !window.leafletMap) return;
            
            console.log("Flying to", lat, lon);
            window.leafletMap.flyTo([lat, lon], 15, {{ duration: 1.5 }});
            
            setTimeout(() => {{
                window.leafletMap.eachLayer(layer => {{
                    if (layer.getLatLng && Math.abs(layer.getLatLng().lat - lat) < 0.005) layer.openPopup();
                }});
            }}, 1600);
        }}
        document.getElementById('pw').addEventListener('keypress', e => {{ if (e.key === 'Enter') unlock(); }});
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Done! Surgery Version Created at {O1}")

if __name__ == "__main__":
    d, g = f_ld()
    f_map(d, g)

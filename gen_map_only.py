import pandas as pd
import os
import re
import asyncio
import base64
import hashlib
import json
from geopy.geocoders import ArcGIS
import folium

D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
P1 = "250222"

def f_ld():
    d = pd.DataFrame(columns=["c1", "c2", "c3", "c4", "c5", "c6", "id"])
    if os.path.exists(D1):
        d = pd.read_csv(D1, encoding='utf-8-sig')
        m_map = {"회사명": "c1", "공고명": "c2", "링크": "c3", "상세주소": "c4", "급여정보": "c5", "예상실수령": "c6", "job_id": "id"}
        d = d.rename(columns=m_map)
    g = {}
    if os.path.exists(D2):
        df = pd.read_csv(D2, encoding='utf-8-sig')
        g_map = {"주소": "a"}
        df = df.rename(columns=g_map)
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
    print("Building sidebar-integrated map...")
    df['c4'] = df['c4'].fillna('')
    v_df = df[df['c4'].str.len() > 5].copy()
    
    search_data = []
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    
    for i, (_, r) in enumerate(v_df.iterrows()):
        a = str(r["c4"])
        co = g.get(a)
        if co:
            t = r["c2"]; cor = r["c1"]
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f"""
            <div style="font-family: 'Noto Sans KR', sans-serif; padding: 10px; min-width: 200px;">
                <h4 style="margin: 0 0 5px 0; color: #1a73e8;">{cor}</h4>
                <p style="margin: 0 0 10px 0; font-size: 0.9rem; color: #333;">{t}</p>
                <div style="font-size: 0.85rem; color: #666; line-height: 1.6;">💰 {r["c5"]}<br>📍 {a}</div>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 10px 0;">
                <a href="{r["c3"]}" target="_blank" style="text-decoration: none; color: white; background: #1a73e8; padding: 5px 10px; border-radius: 4px; display: inline-block; font-size: 0.8rem;">공고 보기</a>
            </div>"""
            # Add marker with a specific class for searching if possible, or handle via JS
            marker = folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=cor, name=cor, icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(m)
            search_data.append({"n": cor, "t": t, "l": co, "s": r["c5"], "a": a, "u": r["c3"]})
            
    tmp = m._repr_html_()
    enc = f_encrypt(tmp, P1)
    s_json = json.dumps(search_data, ensure_ascii=False)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Project | Sidebar & List</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #ffffff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; background: var(--bg); }}
        
        /* Login Screen */
        #login-screen {{ 
            position: fixed; inset: 0; background: var(--bg); z-index: 2000;
            display: flex; justify-content: center; align-items: center; transition: 0.5s;
        }}
        .login-card {{ 
            background: white; padding: 3rem; border-radius: 20px; text-align: center;
            box-shadow: 0 10px 40px rgba(0,0,0,0.05); max-width: 400px; width: 90%;
        }}
        input[type="password"] {{ 
            width: 100%; padding: 14px; border: 1px solid #ddd; border-radius: 8px;
            margin: 20px 0; text-align: center; font-size: 1.1rem;
        }}
        .btn-unlock {{ 
            width: 100%; background: var(--primary); color: white; border: none; padding: 14px;
            border-radius: 8px; font-weight: 600; cursor: pointer; transition: 0.2s;
        }}

        /* Main Layout */
        #main-layout {{ display: none; height: 100vh; width: 100%; display: flex; }}
        
        /* Sidebar */
        #sidebar {{ 
            width: 380px; min-width: 380px; background: var(--sidebar-bg);
            border-right: 1px solid var(--border); display: flex; flex-direction: column;
            z-index: 1000; box-shadow: 2px 0 10px rgba(0,0,0,0.03); 
        }}
        .sidebar-header {{ padding: 20px; border-bottom: 1px solid var(--border); }}
        .sidebar-header h1 {{ font-size: 1.2rem; margin: 0 0 15px 0; color: var(--primary); display: flex; align-items: center; gap: 8px; }}
        
        .search-container {{ position: relative; }}
        #search-input {{ 
            width: 100%; padding: 12px 40px 12px 15px; border-radius: 24px;
            border: 1px solid var(--border); background: #f1f3f4; outline: none;
            font-size: 1rem; box-sizing: border-box; transition: 0.2s;
        }}
        #search-input:focus {{ background: white; border-color: var(--primary); box-shadow: 0 0 0 2px rgba(26,115,232,0.1); }}
        .search-icon-fixed {{ position: absolute; right: 15px; top: 50%; transform: translateY(-50%); color: #70757a; }}

        .stats-bar {{ padding: 10px 20px; font-size: 0.85rem; color: #70757a; background: #fafafa; border-bottom: 1px solid var(--border); }}

        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 10px; }}
        .job-card {{ 
            padding: 16px; border-radius: 12px; border: 1px solid transparent;
            cursor: pointer; transition: 0.2s; margin-bottom: 8px;
        }}
        .job-card:hover {{ background: #f8f9fa; border-color: var(--border); }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        .job-card .corp {{ font-size: 0.85rem; font-weight: 600; color: #5f6368; }}
        .job-card .title {{ font-size: 1rem; font-weight: 600; color: #202124; margin: 4px 0; line-height: 1.4; }}
        .job-card .info {{ font-size: 0.85rem; color: #70757a; display: flex; flex-wrap: wrap; gap: 10px; margin-top: 8px; }}
        .job-card .salary {{ color: #1e8e3e; font-weight: 500; }}

        /* Map Area */
        #map-area {{ flex-grow: 1; position: relative; }}
        
        /* Responsive */
        @media (max-width: 768px) {{
            #sidebar {{ width: 100%; position: absolute; height: 40%; bottom: 0; left: 0; border-right: none; border-top: 1px solid var(--border); }}
            #main-layout {{ flex-direction: column; }}
            #map-area {{ height: 60%; }}
        }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div class="login-card">
            <div style="font-size: 3rem; margin-bottom: 1rem;">💼</div>
            <h2 style="margin:0;">RECRUIT MAP</h2>
            <p style="color:#666; font-size:0.9rem;">프라이빗 채용 지도를 확인하려면 비밀번호를 입력하세요.</p>
            <input type="password" id="pw" placeholder="Password" autofocus>
            <button class="btn-unlock" onclick="unlock()">시작하기</button>
            <div id="err" style="color:#d93025; margin-top:15px; font-size:0.85rem;"></div>
        </div>
    </div>

    <div id="main-layout" style="display:none;">
        <div id="sidebar">
            <div class="sidebar-header">
                <h1><span>📍</span> Recruit Explorer</h1>
                <div class="search-container">
                    <input type="text" id="search-input" placeholder="회사명, 직무 키워드 검색..." oninput="filterJobs(this.value)">
                    <span class="search-icon-fixed">🔍</span>
                </div>
            </div>
            <div class="stats-bar">검색 결과: <span id="count">0</span>건</div>
            <div id="job-list"></div>
        </div>
        <div id="map-area">
            <div id="content" style="width:100%; height:100%;"></div>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{enc}";
        const markersData = {s_json};
        let leafletMap = null;

        async function unlock() {{
            const pw = document.getElementById('pw').value;
            const err = document.getElementById('err');
            try {{
                const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
                const key = [];
                for (let i=0; i<hash.length; i+=2) key.push(parseInt(hash.substr(i, 2), 16));
                
                const raw = atob(encryptedData);
                const res = new Uint8Array(raw.length);
                for (let i=0; i<raw.length; i++) res[i] = raw.charCodeAt(i) ^ key[i % key.length];
                
                const decoded = new TextDecoder().decode(res);
                if (decoded.includes('folium')) {{
                    document.getElementById('login-screen').style.opacity = '0';
                    setTimeout(() => {{
                        document.getElementById('login-screen').style.display = 'none';
                        document.getElementById('main-layout').style.display = 'flex';
                        const container = document.getElementById('content');
                        container.innerHTML = decoded;
                        
                        const scripts = container.getElementsByTagName('script');
                        for (let s of scripts) {{
                            const ns = document.createElement('script');
                            if (s.src) ns.src = s.src;
                            else ns.textContent = s.textContent;
                            document.body.appendChild(ns);
                        }}
                        
                        initMapObject();
                        renderList(markersData);
                    }}, 500);
                }} else {{ err.innerText = "비밀번호가 틀렸습니다."; }}
            }} catch (e) {{ err.innerText = "오류가 발생했습니다."; }}
        }}

        function initMapObject() {{
            let attempts = 0;
            const findMap = setInterval(() => {{
                attempts++;
                for (let key in window) {{
                    if (key.startsWith('map_') && window[key] && typeof window[key].flyTo === 'function') {{
                        leafletMap = window[key];
                        clearInterval(findMap);
                        break;
                    }}
                }}
                if (attempts > 20) clearInterval(findMap);
            }}, 500);
        }}

        function renderList(list) {{
            const container = document.getElementById('job-list');
            document.getElementById('count').innerText = list.length;
            container.innerHTML = list.map((j, idx) => `
                <div class="job-card" onclick="focusJob(${{j.l[0]}}, ${{j.l[1]}}, '${{j.n}}', this)">
                    <div class="corp">${{j.n}}</div>
                    <div class="title">${{j.t}}</div>
                    <div class="info">
                        <span class="salary">💰 ${{j.s || '정보없음'}}</span>
                        <span>📍 ${{j.a.split(' ').slice(0,2).join(' ')}}</span>
                    </div>
                </div>
            `).join('');
        }}

        function filterJobs(val) {{
            const filtered = markersData.filter(m => 
                m.n.toLowerCase().includes(val.toLowerCase()) || 
                m.t.toLowerCase().includes(val.toLowerCase()) ||
                m.a.toLowerCase().includes(val.toLowerCase())
            );
            renderList(filtered);
            
            // Optional: Hide/Show markers on map
            if (leafletMap) {{
                leafletMap.eachLayer(layer => {{
                    if (layer instanceof L.Marker) {{
                        const name = layer.options.name || layer.options.title || "";
                        const tooltip = layer.getTooltip() ? layer.getTooltip().getContent() : "";
                        const match = filtered.some(f => f.n === name || f.n === tooltip);
                        if (match) layer.addTo(leafletMap);
                        else leafletMap.removeLayer(layer);
                    }
                }});
            }}
        }}

        function focusJob(lat, lon, name, el) {{
            if (!leafletMap) return;
            
            // Active state
            document.querySelectorAll('.job-card').forEach(c => c.classList.remove('active'));
            el.classList.add('active');

            leafletMap.flyTo([lat, lon], 15, {{ duration: 1.5 }});
            
            setTimeout(() => {{
                leafletMap.eachLayer(layer => {{
                    if (layer instanceof L.Marker) {{
                        const ll = layer.getLatLng();
                        if (Math.abs(ll.lat - lat) < 0.001 && Math.abs(ll.lng - lon) < 0.001) {{
                            layer.openPopup();
                        }}
                    }
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
    print(f"Done! Sidebar Integration Created at {O1}")

if __name__ == "__main__":
    d, g = f_ld()
    f_map(d, g)

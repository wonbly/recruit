import pandas as pd
import os
import re
import asyncio
import base64
import hashlib
import json
from geopy.geocoders import ArcGIS
import folium
from folium.plugins import Search

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
    print("Building premium map from existing data...")
    df['c4'] = df['c4'].fillna('')
    v_df = df[df['c4'].str.len() > 5].copy()
    
    # Store markers info for JS search
    search_data = []
    
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    fg = folium.FeatureGroup(name="Data").add_to(m)
    
    for i, (_, r) in enumerate(v_df.iterrows()):
        a = str(r["c4"])
        co = g.get(a)
        if co:
            t = r["c2"]
            cor = r["c1"]
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f"""
            <div style="font-family: 'Noto Sans KR', sans-serif; padding: 10px; min-width: 200px;">
                <h4 style="margin: 0 0 5px 0; color: #1a73e8;">{cor}</h4>
                <p style="margin: 0 0 10px 0; font-size: 0.9rem; color: #333;">{t}</p>
                <div style="font-size: 0.85rem; color: #666; line-height: 1.6;">
                    💰 {r["c5"]}<br>
                    📍 {a}
                </div>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 10px 0;">
                <a href="{r["c3"]}" target="_blank" style="text-decoration: none; color: white; background: #1a73e8; padding: 5px 10px; border-radius: 4px; display: inline-block; font-size: 0.8rem;">공고 보기</a>
            </div>
            """
            marker = folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=cor, name=cor, icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(fg)
            search_data.append({"n": cor, "t": t, "l": co})
            
    tmp = m._repr_html_()
    enc = f_encrypt(tmp, P1)
    s_json = json.dumps(search_data, ensure_ascii=False)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Project | Premium Map</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --card: #ffffff; }}
        body {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; background: var(--bg); margin: 0; overflow: hidden; height: 100vh; }}
        
        /* Login UI */
        #login-screen {{ 
            display: flex; justify-content: center; align-items: center; 
            height: 100vh; width: 100%; position: fixed; z-index: 1000;
            background: var(--bg); transition: 0.5s;
        }}
        .login-card {{ 
            background: var(--card); padding: 3rem; border-radius: 16px; 
            box-shadow: 0 10px 40px rgba(0,0,0,0.08); text-align: center; 
            max-width: 400px; width: 85%; transform: translateY(-20px);
        }}
        .login-card h2 {{ font-weight: 600; color: #202124; margin-bottom: 0.5rem; }}
        .login-card p {{ color: #5f6368; font-size: 0.95rem; margin-bottom: 2rem; }}
        .input-group {{ margin-bottom: 1.5rem; position: relative; }}
        input[type="password"] {{ 
            width: 100%; padding: 14px; border: 1px solid #dadce0; border-radius: 8px;
            font-size: 1.1rem; box-sizing: border-box; text-align: center;
            transition: 0.3s; font-family: sans-serif;
        }}
        input[type="password"]:focus {{ outline: none; border-color: var(--primary); box-shadow: 0 0 0 2px rgba(26,115,232,0.2); }}
        .btn-unlock {{ 
            background: var(--primary); color: white; border: none; padding: 14px 40px;
            border-radius: 8px; font-weight: 600; cursor: pointer; font-size: 1rem;
            width: 100%; transition: 0.3s;
        }}
        .btn-unlock:hover {{ background: #1557b0; box-shadow: 0 4px 12px rgba(26,115,232,0.3); }}
        #error-msg {{ color: #d93025; font-size: 0.85rem; margin-top: 12px; height: 20px; }}

        /* Map UI */
        #map-container {{ display: none; width: 100%; height: 100vh; position: relative; }}
        
        /* Floating Search Bar */
        .search-box-wrapper {{
            position: absolute; top: 20px; left: 50%; transform: translateX(-50%);
            z-index: 999; width: 90%; max-width: 600px;
        }}
        .search-inner {{ 
            background: white; border-radius: 28px; display: flex; align-items: center;
            padding: 8px 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.15), 0 0 0 1px rgba(0,0,0,0.02);
            transition: 0.3s;
        }}
        .search-inner:focus-within {{ box-shadow: 0 4px 12px rgba(0,0,0,0.25); }}
        .search-icon {{ color: #5f6368; margin-right: 12px; }}
        #search-input {{ 
            border: none; outline: none; width: 100%; font-size: 1.1rem;
            padding: 8px 0; font-family: 'Noto Sans KR', sans-serif;
        }}
        
        /* Search Suggestions */
        .suggestions {{
            position: absolute; top: 60px; left: 0; right: 0;
            background: white; border-radius: 12px; overflow: hidden;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15); max-height: 300px; overflow-y: auto;
            display: none;
        }}
        .suggestion-item {{ 
            padding: 12px 20px; cursor: pointer; border-bottom: 1px solid #eee;
            transition: 0.2s; display: flex; flex-direction: column;
        }}
        .suggestion-item:hover {{ background: #f1f3f4; }}
        .suggestion-item .name {{ font-weight: 500; color: #202124; }}
        .suggestion-item .title {{ font-size: 0.8rem; color: #70757a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}

        /* Custom Popup Style */
        .leaflet-popup-content-wrapper {{ border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div class="login-card">
            <div style="font-size: 2.5rem; margin-bottom: 1rem;">📍</div>
            <h2>JOB MAP ACCESS</h2>
            <p>프로젝트 지도를 보려면 비밀번호를 입력하세요.</p>
            <div class="input-group">
                <input type="password" id="pw" placeholder="비밀번호 입력" autofocus>
                <div id="error-msg"></div>
            </div>
            <button class="btn-unlock" onclick="unlock()">지도 열기</button>
        </div>
    </div>

    <div id="map-container">
        <div class="search-box-wrapper">
            <div class="search-inner">
                <span class="search-icon">🔍</span>
                <input type="text" id="search-input" placeholder="회사명 또는 공고 키워드 검색..." oninput="handleSearch(this.value)">
                <span id="search-clear" style="cursor:pointer; display:none;" onclick="clearSearch()">✕</span>
            </div>
            <div class="suggestions" id="suggestions"></div>
        </div>
        <div id="content" style="width:100%; height:100%;"></div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{enc}";
        const markers = {s_json};
        let leafletMap = null;

        async function unlock() {{
            const pw = document.getElementById('pw').value;
            const err = document.getElementById('error-msg');
            try {{
                const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
                const key = [];
                for (let i = 0; i < hash.length; i += 2) {{
                    key.push(parseInt(hash.substr(i, 2), 16));
                }}
                
                const raw = atob(encryptedData);
                const res = new Uint8Array(raw.length);
                for (let i = 0; i < raw.length; i++) {{
                    res[i] = raw.charCodeAt(i) ^ key[i % key.length];
                }}
                
                const decoded = new TextDecoder().decode(res);
                if (decoded.includes('folium')) {{
                    document.getElementById('login-screen').style.opacity = '0';
                    setTimeout(() => {{
                        document.getElementById('login-screen').style.display = 'none';
                        document.getElementById('map-container').style.display = 'block';
                        const container = document.getElementById('content');
                        container.innerHTML = decoded;
                        
                        const scripts = container.getElementsByTagName('script');
                        for (let s of scripts) {{
                            const ns = document.createElement('script');
                            if (s.src) ns.src = s.src;
                            else ns.textContent = s.textContent;
                            document.body.appendChild(ns);
                        }}
                        
                        // Find the leaflet map object more robustly
                        let attempts = 0;
                        const findMap = setInterval(() => {{
                            attempts++;
                            for (let key in window) {{
                                if (key.startsWith('map_') && window[key] && typeof window[key].flyTo === 'function') {{
                                    leafletMap = window[key];
                                    console.log("Map found:", key);
                                    clearInterval(findMap);
                                    break;
                                }}
                            }}
                            if (attempts > 20) clearInterval(findMap);
                        }}, 500);
                    }}, 500);
                }} else {{
                    err.innerText = "비밀번호가 올바르지 않습니다.";
                }}
            }} catch (e) {{
                err.innerText = "비밀번호 오류가 발생했습니다.";
            }}
        }}

        function handleSearch(val) {{
            const list = document.getElementById('suggestions');
            const clear = document.getElementById('search-clear');
            if (!val) {{
                list.style.display = 'none';
                clear.style.display = 'none';
                return;
            }}
            clear.style.display = 'block';
            const filtered = markers.filter(m => m.n.includes(val) || m.t.includes(val)).slice(0, 10);
            if (filtered.length > 0) {{
                list.innerHTML = filtered.map(m => `
                    <div class="suggestion-item" onclick="goToMarker(${{m.l[0]}}, ${{m.l[1]}}, '${{m.n}}')">
                        <span class="name">${{m.n}}</span>
                        <span class="title">${{m.t}}</span>
                    </div>
                `).join('');
                list.style.display = 'block';
            }} else {{
                list.style.display = 'none';
            }}
        }}

        function clearSearch() {{
            document.getElementById('search-input').value = '';
            handleSearch('');
        }}

        function goToMarker(lat, lon, name) {{
            console.log("Navigating to:", name, lat, lon);
            if (!leafletMap) {{
                for (let key in window) {{
                    if (key.startsWith('map_') && window[key] && typeof window[key].flyTo === 'function') {{
                        leafletMap = window[key];
                        break;
                    }}
                }}
            }}
            if (!leafletMap) {{
                alert("지도를 불러오는 중입니다. 잠시 후 다시 시도해주세요.");
                return;
            }}
            document.getElementById('suggestions').style.display = 'none';
            leafletMap.flyTo([lat, lon], 15, {{ duration: 1.5 }});
            
            // Find marker in Leaflet's layers to open popup
            setTimeout(() => {{
                leafletMap.eachLayer(function(layer) {{
                    if (layer instanceof L.Marker || (layer.options && layer.options.icon)) {{
                        const ll = layer.getLatLng ? layer.getLatLng() : null;
                        if (ll && Math.abs(ll.lat - lat) < 0.001 && Math.abs(ll.lng - lon) < 0.001) {{
                            layer.openPopup();
                        }}
                    }}
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
    print(f"Done! Premium UX Created at {O1}")

if __name__ == "__main__":
    d, g = f_ld()
    f_map(d, g)

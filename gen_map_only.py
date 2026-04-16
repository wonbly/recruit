import pandas as pd
import os
import re
import asyncio
import base64
import hashlib
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
    print("Building map from existing data...")
    geocoder = ArcGIS(timeout=10)
    df['c4'] = df['c4'].fillna('')
    v_df = df[df['c4'].str.len() > 5].copy()
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    fg = folium.FeatureGroup(name="Data").add_to(m)
    for i, (_, r) in enumerate(v_df.iterrows()):
        a = str(r["c4"])
        co = g.get(a)
        if co:
            t = r["c2"]
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f'<div style="width:250px;"><b>{r["c1"]}</b><br>{t}<br><br>💰 {r["c5"]}<br>📍 {a}<br><a href="{r["c3"]}" target="_blank">Link</a></div>'
            folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=r["c1"], name=r["c1"], icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(fg)
    
    Search(layer=fg, geom_type="Point", placeholder="Search", collapsed=False, search_label="name").add_to(m)
    tmp = m._repr_html_()
    enc = f_encrypt(tmp, P1)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Project Access</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {{ font-family: sans-serif; background: #1a1a1a; color: white; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; overflow: hidden; }}
        .box {{ background: #2a2a2a; padding: 2.5rem; border-radius: 12px; box-shadow: 0 8px 30px rgba(0,0,0,0.7); text-align: center; max-width: 400px; width: 90%; }}
        h3 {{ margin-top: 0; color: #00befa; letter-spacing: 1px; }}
        p {{ color: #aaa; font-size: 0.9rem; margin-bottom: 1.5rem; }}
        input {{ padding: 12px; width: 80%; border: 1px solid #444; border-radius: 6px; margin-bottom: 20px; background: #333; color: white; text-align: center; font-size: 1.1rem; }}
        button {{ padding: 12px 30px; background: #00befa; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; transition: 0.3s; }}
        button:hover {{ background: #008ebc; transform: translateY(-2px); }}
        #error {{ color: #ff6b6b; margin-top: 15px; font-size: 0.9rem; height: 20px; }}
    </style>
</head>
<body>
    <div class="box" id="login">
        <h3>PRIVATE MAP</h3>
        <p>인출 프로젝트 결과물 확인을 위해<br>비밀번호를 입력해주세요.</p>
        <input type="password" id="pw" placeholder="PASSWORD"><br>
        <button onclick="unlock()">UNLOCK</button>
        <div id="error"></div>
    </div>
    <div id="content" style="display:none; width:100%; height:100vh;"></div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const data = "{enc}";
        async function unlock() {{
            const pw = document.getElementById('pw').value;
            const err = document.getElementById('error');
            try {{
                const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
                const key = [];
                for (let i = 0; i < hash.length; i += 2) {{
                    key.push(parseInt(hash.substr(i, 2), 16));
                }}
                
                const raw = atob(data);
                const res = new Uint8Array(raw.length);
                for (let i = 0; i < raw.length; i++) {{
                    res[i] = raw.charCodeAt(i) ^ key[i % key.length];
                }}
                
                const decoded = new TextDecoder().decode(res);
                if (decoded.includes('folium')) {{
                    document.getElementById('login').style.display = 'none';
                    document.body.style.display = 'block';
                    document.body.style.background = 'white';
                    const container = document.getElementById('content');
                    container.style.display = 'block';
                    container.innerHTML = decoded;
                    
                    const scripts = container.getElementsByTagName('script');
                    for (let s of scripts) {{
                        const ns = document.createElement('script');
                        if (s.src) ns.src = s.src;
                        else ns.textContent = s.textContent;
                        document.body.appendChild(ns);
                    }}
                }} else {{
                    err.innerText = "비밀번호가 올바르지 않습니다.";
                }}
            }} catch (e) {{
                err.innerText = "비밀번호가 올바르지 않습니다.";
            }}
        }}
        // Enter key support
        document.getElementById('pw').addEventListener('keypress', function(e) {{
            if (e.key === 'Enter') unlock();
        }});
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Done! Created {O1}")

if __name__ == "__main__":
    d, g = f_ld()
    f_map(d, g)

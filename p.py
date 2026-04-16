import pandas as pd
import os
import re
import asyncio
import base64
import json
import hashlib
from playwright.async_api import async_playwright
from geopy.geocoders import ArcGIS
import folium
from folium.plugins import Search

# [System Config]
C1 = "https://www.saramin.co.kr/zf_user/search?loc_cd=102250%2C102230%2C102240%2C102260%2C102220&cat_mcls=16%2C14&company_cd=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C9%2C10&search_optional_item=y&search_done=y&panel_count=y&recruitPageCount=40&recruitSort=reg_dt"
D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
W1 = 4 
P1 = "250222"

def f_idx(u):
    m = re.search(r'rec_idx=(\d+)', u)
    return m.group(1) if m else u

def f_sal(t):
    try:
        n = [int(i.replace(',', '')) for i in re.findall(r'[\d,]+', t)]
        if not n: return ""
        a = sum(n) / len(n)
        if a < 1000: return ""
        m = (a * 0.9) / 12
        return f"{int(m):,}만원"
    except: return ""

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

async def f_list(p, s_ids):
    print("Scanning list...")
    nj = []
    for i in range(1, 4): # Scaled down for reliability
        u = f"{C1}&recruitPage={i}"
        try:
            await p.goto(u, timeout=30000, wait_until="load")
            await p.wait_for_selector('.item_recruit', timeout=5000)
        except: break
        items = await p.query_selector_all('.item_recruit')
        if not items: break
        for it in items:
            j_el = await it.query_selector('.job_tit a')
            l = await j_el.get_attribute('href')
            if not l.startswith('http'): l = 'https://www.saramin.co.kr' + l
            jid = f_idx(l)
            if jid in s_ids: continue
            cor = await (await it.query_selector('.corp_name a')).inner_text()
            tit = await j_el.inner_text()
            nj.append({"c1": cor.strip(), "c2": tit.strip(), "c3": l, "id": jid})
        print(f"  - Page {i} done")
    return nj

async def f_deep(jobs):
    print(f"Deep scanning {len(jobs)} items...")
    res = []
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        async def worker(sub_list):
            ctx = await b.new_context(user_agent="Mozilla/5.0")
            page = await ctx.new_page()
            for j in sub_list:
                try:
                    await page.goto(j["c3"], timeout=30000, wait_until="load")
                    await asyncio.sleep(1)
                    data = await page.evaluate('''() => {
                        let a = "", s = "";
                        let e = document.querySelector('.address .txt_adr') || document.querySelector('.jw_address') || document.querySelector('address');
                        if(e) a = e.innerText;
                        if(!a) {
                            for(let d of document.querySelectorAll('dt')) {
                                if(d.innerText.match(/근무지|위치|주소/)) {
                                    a = d.nextElementSibling ? d.nextElementSibling.innerText : "";
                                    break;
                                }
                            }
                        }
                        if(!a) {
                            const m = document.body.innerText.match(/(?:경기|서울|인천|충남|강원|대전)\\\\s+[가-힣]+\\\\s+[가-힣]+[구군]\\\\s+[가-힣\\\\d-]+[로길]\\\\s+\\\\d+/);
                            if(m) a = m[0];
                        }
                        for(let d of document.querySelectorAll('dt')) {
                            if(d.innerText.includes('급여')) {
                                s = d.nextElementSibling ? d.nextElementSibling.innerText : "";
                                break;
                            }
                        }
                        return { "a": a, "s": s };
                    }''')
                    j["c4"] = data["a"].strip()
                    j["c5"] = data["s"].strip()
                    j["c6"] = f_sal(j["c5"])
                    res.append(j)
                except: continue
            await ctx.close()
        ch = (len(jobs) // W1) + 1
        ts = [worker(jobs[i:i + ch]) for i in range(0, len(jobs), ch)]
        await asyncio.gather(*ts)
        await b.close()
    return res

def f_save_g(g):
    d = [{"a": k, "lat": v[0], "lon": v[1]} for k, v in g.items() if v]
    pd.DataFrame(d).to_csv(D2, index=False, encoding='utf-8-sig')

def f_encrypt(data, pw):
    key = hashlib.sha256(pw.encode()).digest()
    data_bytes = data.encode('utf-8')
    res = bytearray()
    for i in range(len(data_bytes)):
        res.append(data_bytes[i] ^ key[i % len(key)])
    return base64.b64encode(res).decode()

def f_map(df, g):
    print("Building premium map...")
    df['c4'] = df['c4'].fillna('')
    v_df = df[df['c4'].str.len() > 5].copy()
    search_data = []

    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    fg = folium.FeatureGroup(name="Data").add_to(m)
    geocoder = ArcGIS(timeout=10)

    for i, (_, r) in enumerate(v_df.iterrows()):
        a = str(r["c4"])
        if a not in g:
            try:
                cl = re.sub(r'\(.*?\)', '', a)
                cl = re.sub(r'\d+층|\d+호|[A-Za-z]동', '', cl).split(',')[0].strip()
                loc = geocoder.geocode(cl)
                if loc: g[a] = (loc.latitude, loc.longitude)
                else: g[a] = None
            except: pass
        co = g.get(a)
        if co:
            t = r["c2"]; cor = r["c1"]
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f'''
            <div style="font-family: 'Noto Sans KR', sans-serif; padding: 10px; min-width: 200px;">
                <h4 style="margin: 0 0 5px 0; color: #1a73e8;">{cor}</h4>
                <p style="margin: 0 0 10px 0; font-size: 0.9rem; color: #333;">{t}</p>
                <div style="font-size: 0.85rem; color: #666; line-height: 1.6;">💰 {r["c5"]}<br>📍 {a}</div>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 10px 0;">
                <a href="{r["c3"]}" target="_blank" style="text-decoration: none; color: white; background: #1a73e8; padding: 5px 10px; border-radius: 4px; display: inline-block; font-size: 0.8rem;">공고 보기</a>
            </div>'''
            folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=cor, name=cor, icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(fg)
            search_data.append({"n": cor, "t": t, "l": co})

    f_save_g(g)
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
        #login-screen {{ display: flex; justify-content: center; align-items: center; height: 100vh; width: 100%; position: fixed; z-index: 1000; background: var(--bg); transition: 0.5s; }}
        .login-card {{ background: var(--card); padding: 3rem; border-radius: 16px; box-shadow: 0 10px 40px rgba(0,0,0,0.08); text-align: center; max-width: 400px; width: 85%; }}
        input[type="password"] {{ width: 100%; padding: 14px; border: 1px solid #dadce0; border-radius: 8px; font-size: 1.1rem; box-sizing: border-box; text-align: center; margin-bottom: 1.5rem; }}
        .btn-unlock {{ background: var(--primary); color: white; border: none; padding: 14px 40px; border-radius: 8px; font-weight: 600; cursor: pointer; width: 100%; }}
        #map-container {{ display: none; width: 100%; height: 100vh; position: relative; }}
        .search-box-wrapper {{ position: absolute; top: 20px; left: 50%; transform: translateX(-50%); z-index: 999; width: 90%; max-width: 600px; }}
        .search-inner {{ background: white; border-radius: 28px; display: flex; align-items: center; padding: 8px 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); }}
        #search-input {{ border: none; outline: none; width: 100%; font-size: 1.1rem; padding: 8px 0; }}
        .suggestions {{ position: absolute; top: 60px; left: 0; right: 0; background: white; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); max-height: 300px; overflow-y: auto; display: none; }}
        .suggestion-item {{ padding: 12px 20px; cursor: pointer; border-bottom: 1px solid #eee; }}
        .suggestion-item:hover {{ background: #f1f3f4; }}
    </style>
</head>
<body>
    <div id="login-screen"><div class="login-card"><h2>JOB MAP ACCESS</h2><p>비밀번호를 입력하세요.</p><input type="password" id="pw" placeholder="PASSWORD"><button class="btn-unlock" onclick="unlock()">지도 열기</button><div id="error-msg" style="color:red; margin-top:10px;"></div></div></div>
    <div id="map-container">
        <div class="search-box-wrapper"><div class="search-inner">🔍<input type="text" id="search-input" placeholder="회사명 검색..." oninput="handleSearch(this.value)"></div><div class="suggestions" id="suggestions"></div></div>
        <div id="content" style="width:100%; height:100%;"></div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{enc}"; const markers = {s_json}; let leafletMap = null;
        async function unlock() {{
            const pw = document.getElementById('pw').value;
            const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
            const key = []; for (let i=0; i<hash.length; i+=2) key.push(parseInt(hash.substr(i, 2), 16));
            const raw = atob(encryptedData); const res = new Uint8Array(raw.length);
            for (let i=0; i<raw.length; i++) res[i] = raw.charCodeAt(i) ^ key[i % key.length];
            const decoded = new TextDecoder().decode(res);
            if (decoded.includes('folium')) {{
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('map-container').style.display = 'block';
                const container = document.getElementById('content'); container.innerHTML = decoded;
                const scripts = container.getElementsByTagName('script');
                for (let s of scripts) {{ const ns = document.createElement('script'); if (s.src) ns.src = s.src; else ns.textContent = s.textContent; document.body.appendChild(ns); }}
                // Find the leaflet map object more robustly
                let attempts = 0;
                const findMap = setInterval(() => {
                    attempts++;
                    for (let key in window) {
                        if (key.startsWith('map_') && window[key] && typeof window[key].flyTo === 'function') {
                            leafletMap = window[key];
                            clearInterval(findMap);
                            break;
                        }
                    }
                    if (attempts > 20) clearInterval(findMap);
                }, 500);
            }} else {{ document.getElementById('error-msg').innerText = "Wrong Password"; }}
        }}
        function handleSearch(val) {{
            const list = document.getElementById('suggestions');
            if (!val) {{ list.style.display = 'none'; return; }}
            const filtered = markers.filter(m => m.n.includes(val) || m.t.includes(val)).slice(0, 10);
            if (filtered.length > 0) {{
                list.innerHTML = filtered.map(m => `<div class="suggestion-item" onclick="goToMarker(${{m.l[0]}}, ${{m.l[1]}}, '${{m.n}}')"><b>${{m.n}}</b><br><small>${{m.t}}</small></div>`).join('');
                list.style.display = 'block';
            }} else list.style.display = 'none';
        }}
        function goToMarker(lat, lon, name) {
            if (!leafletMap) {
                for (let key in window) {
                    if (key.startsWith('map_') && window[key] && typeof window[key].flyTo === 'function') {
                        leafletMap = window[key];
                        break;
                    }
                }
            }
            if (!leafletMap) return;
            document.getElementById('suggestions').style.display = 'none';
            leafletMap.flyTo([lat, lon], 15, { duration: 1.5 });
            
            setTimeout(() => {
                leafletMap.eachLayer(l => {
                    const ll = l.getLatLng ? l.getLatLng() : null;
                    if (ll && Math.abs(ll.lat-lat)<0.001 && Math.abs(ll.lng-lon)<0.001) {
                        if (l.openPopup) l.openPopup();
                    }
                });
            }, 1600);
        }
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Done! Premium UX Created at {O1}")

async def main():
    d, g = f_ld()
    d['c4'] = d['c4'].fillna('')
    s_ids = set(d[d['c4'].str.len() > 5]['id'].astype(str).tolist())
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True); ctx = await b.new_context(user_agent="Mozilla/5.0")
        pg = await ctx.new_page(); nj = await f_list(pg, s_ids); await b.close()
    if nj:
        sc = await f_deep(nj); d = pd.concat([d, pd.DataFrame(sc)], ignore_index=True); d.to_csv(D1, index=False, encoding='utf-8-sig')
    f_map(d, g)

if __name__ == "__main__":
    asyncio.run(main())

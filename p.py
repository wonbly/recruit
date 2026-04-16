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
from datetime import datetime

# [System Config] - Category List Version (1,000+ jobs)
C1 = "https://www.saramin.co.kr/zf_user/jobs/list/job-category?cat_mcls=16%2C14&loc_cd=102230%2C102240%2C102250%2C102260%2C102220%2C102520%2C102530%2C102540%2C102550%2C102510%2C102390&recruitSort=reg_dt"
D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
W1 = 5 # Parallel threads
P1 = "250222"

def f_idx(u):
    m = re.search(r'rec_idx=(\d+)', u)
    return m.group(1) if m else u

def f_sal(t):
    try:
        n = [int(i.replace(',', '')) for i in re.findall(r'[\d,]+', t)]
        if not n: return ""
        a = sum(n) / len(n); m = (a * 0.9) / 12
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
        adr_col = "주소" if "주소" in df.columns else "a"
        g = { str(r[adr_col]): (r['lat'], r['lon']) for _, r in df.iterrows() if not pd.isna(r['lat']) }
    return d, g

async def f_list(p, s_ids):
    print("Scanning list...")
    nj = []
    # Scan up to 12 pages for 1,000+ items (80-100 per page)
    for i in range(1, 13):
        u = f"{C1}&recruitPage={i}&recruitPageCount=100"
        try:
            await p.goto(u, timeout=30000, wait_until="load")
            await p.wait_for_selector('.item', timeout=5000)
        except: break
        
        items = await p.query_selector_all('.item')
        for it in items:
            try:
                # New selectors for job-category page
                j_el = await it.query_selector('.job_tit a')
                if not j_el: continue
                l = await j_el.get_attribute('href')
                if not l.startswith('http'): l = 'https://www.saramin.co.kr' + l
                jid = f_idx(l)
                if jid in s_ids: continue
                
                cor_el = await it.query_selector('.corp_name a')
                cor = (await cor_el.inner_text()).strip() if cor_el else "Unknown"
                tit = (await j_el.inner_text()).strip()
                
                # Location/Condition info
                conds = await it.query_selector_all('.job_condition span')
                loc_txt = ""
                if conds: loc_txt = (await conds[0].inner_text()).strip()
                
                sal_txt = ""
                if len(conds) > 1: sal_txt = (await conds[1].inner_text()).strip()
                
                nj.append({
                    "c1": cor, "c2": tit, "c3": l, "id": jid, 
                    "c4": loc_txt, "c5": sal_txt
                })
            except: continue
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
                        let e = document.querySelector('.address .txt_adr') || document.querySelector('.jw_address') || document.querySelector('address');
                        let a = e ? e.innerText : "";
                        let s = "";
                        for(let d of document.querySelectorAll('dt')) if(d.innerText.includes('급여')) s = d.nextElementSibling ? d.nextElementSibling.innerText : "";
                        return { "a": a, "s": s };
                    }''')
                    j["c4"] = data["a"].strip(); j["c5"] = data["s"].strip(); j["c6"] = f_sal(j["c5"]); res.append(j)
                except: continue
            await ctx.close()
        ch = (len(jobs) // W1) + 1
        ts = [worker(jobs[i:i + ch]) for i in range(0, len(jobs), ch)]
        await asyncio.gather(*ts); await b.close()
    return res

def f_encrypt(data, pw):
    key = hashlib.sha256(pw.encode()).digest()
    data_bytes = data.encode('utf-8')
    res = bytearray()
    for i in range(len(data_bytes)): res.append(data_bytes[i] ^ key[i % len(key)])
    return base64.b64encode(res).decode()

def f_map(df, g):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Final Robust Build - {now}...")
    df['c4'] = df['c4'].fillna('')
    # DEDUPLICATE: Prioritize entries with address
    if 'id' in df.columns:
        df['al'] = df['c4'].str.len()
        df = df.sort_values(by='al', ascending=False)
        df = df.drop_duplicates(subset=['id'], keep='first').drop(columns=['al'])
    
    clean_data = []
    geocoder = ArcGIS(timeout=10)
    for _, r in df.iterrows():
        adr = str(r["c4"]); coords = g.get(adr)
        if adr and adr != 'nan' and not coords:
            try:
                # Optimized cleaning for ArcGIS
                cl = re.sub(r'\(.*?\)', '', adr)
                cl = re.sub(r'\d+층|\d+호', '', cl).split(',')[0].strip()
                if not cl: continue
                loc = geocoder.geocode(cl)
                if loc: 
                    coords = (loc.latitude, loc.longitude)
                    g[adr] = coords
                    print(f"New Loc: {cl} -> {coords}")
            except: pass
        
        clean_data.append({
            "id": str(r.get("id", "")), "corp": str(r["c1"]), "title": str(r["c2"]), "link": str(r["c3"]),
            "loc": coords if coords else None, "sal": str(r.get("c5", "")), "adr": adr
        })
    
    payload = f_encrypt(json.dumps(clean_data, ensure_ascii=False), P1)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Map | Pure Engine</title>
    <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #fff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; display:flex; }}
        #login-screen {{ position: fixed; inset: 0; background: var(--bg); z-index: 5000; display:flex; justify-content:center; align-items:center; }}
        #main-layout {{ display: none; height: 100vh; width: 100%; display: flex; }}
        #sidebar {{ width: 400px; min-width: 400px; background: var(--sidebar-bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; }}
        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 12px; }}
        .job-card {{ padding: 16px; border-radius: 12px; cursor: pointer; border: 1px solid transparent; margin-bottom: 8px; transition: 0.1s; }}
        .job-card:hover {{ background: #f8f9fa; }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        #map {{ width: 100%; height: 100%; flex-grow: 1; }}
    </style>
</head>
<body>
    <div id="login-screen">
        <div style="background:white;padding:3rem;border-radius:24px;text-align:center;box-shadow:0 10px 40px rgba(0,0,0,0.1);width:350px;">
            <h2>RECRUIT MAP</h2>
            <input type="password" id="pw-input" placeholder="PASSWORD" style="width:100%;padding:14px;border-radius:12px;border:1px solid #ddd;margin:20px 0;text-align:center;">
            <button onclick="handleLogin()" style="width:100%;background:var(--primary);color:white;padding:14px;border:none;border-radius:12px;cursor:pointer;font-weight:600;">LOG IN</button>
            <div id="login-err" style="color:red;margin-top:10px;"></div>
        </div>
    </div>
    <div id="main-layout">
        <div id="sidebar">
            <div style="padding:24px;border-bottom:1px solid var(--border);"><h1>📍 Recruit</h1><input type="text" id="search-box" placeholder="Search..." oninput="handleSearch(this.value)" style="width:100%;padding:12px;border-radius:24px;border:1px solid var(--border);background:#f1f3f4;outline:none;"></div>
            <div style="padding:12px 24px; font-size:13px; color:#70757a;">Total <span id="job-count">0</span></div>
            <div id="job-list"></div>
        </div>
        <div id="map"></div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const encryptedData = "{payload}"; let allJobs = []; let markers = {{}}; let myMap = null;
        async function handleLogin() {{
            const pw = document.getElementById('pw-input').value;
            const hash = CryptoJS.SHA256(pw).toString(CryptoJS.enc.Hex);
            const key = []; for (let i=0; i<hash.length; i+=2) key.push(parseInt(hash.substr(i, 2), 16));
            try {{
                const raw = atob(encryptedData); const res = new Uint8Array(raw.length);
                for (let i=0; i<raw.length; i++) res[i] = raw.charCodeAt(i) ^ key[i % key.length];
                allJobs = JSON.parse(new TextDecoder().decode(res));
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('main-layout').style.display = 'flex';
                initMap(); renderItems(allJobs);
            }} catch(e) {{ document.getElementById('login-err').innerText = 'Invalid'; }}
        }}
        function initMap() {{
            myMap = L.map('map', {{ zoomControl:false }}).setView([37.4979, 127.0276], 11);
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png').addTo(myMap);
            allJobs.forEach(j => {{
                if (j.loc) {{
                    const m = L.marker([j.loc[0], j.loc[1]]).addTo(myMap);
                    m.bindPopup(`<b style="color:var(--primary);">${{j.corp}}</b><br>${{j.title}}<br><br><a href="${{j.link}}" target="_blank">View Post</a>`);
                    markers[j.id || j.corp+j.title] = m;
                }}
            }});
        }}
        function renderItems(list) {{
            const listEl = document.getElementById('job-list');
            document.getElementById('job-count').innerText = list.length;
            listEl.innerHTML = list.map(j => `<div class="job-card" onclick="focusJob('${{j.id}}', ${{j.loc ? j.loc[0] : 'null'}}, ${{j.loc ? j.loc[1] : 'null'}}, this)"><div class="corp">${{j.corp}}</div><div class="title">${{j.title}}</div></div>`).join('');
        }}
        function handleSearch(val) {{
            const v = val.toLowerCase(); const filtered = allJobs.filter(j => j.corp.toLowerCase().includes(v) || j.title.toLowerCase().includes(v));
            renderItems(filtered); allJobs.forEach(j => {{ const m = markers[j.id || j.corp+j.title]; if(m) {{ if(filtered.some(f => f.id===j.id)) m.addTo(myMap); else myMap.removeLayer(m); }} }});
        }}
        function focusJob(id, lat, lon, el) {{
            document.querySelectorAll('.job-card').forEach(c => c.classList.remove('active')); el.classList.add('active');
            if (lat && lon && myMap) {{
                myMap.flyTo([lat, lon], 15, {{ duration:1.5 }});
                const m = markers[id]; if(m) setTimeout(() => m.openPopup(), 1600);
            }}
        }}
        document.getElementById('pw-input').addEventListener('keypress', (e) => {{ if(e.key==='Enter') handleLogin(); }});
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f: f.write(html)
    print(f"Success: p.py synced at {O1}")

async def main():
    d, g = f_ld(); s_ids = set(d['id'].astype(str).tolist())
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True); ctx = await b.new_context(user_agent="Mozilla/5.0")
        pg = await ctx.new_page(); nj = await f_list(pg, s_ids); await b.close()
    if nj:
        sc = await f_deep(nj); d = pd.concat([d, pd.DataFrame(sc)], ignore_index=True); d.to_csv(D1, index=False, encoding='utf-8-sig')
    f_map(d, g)

if __name__ == "__main__": asyncio.run(main())

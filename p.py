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
    for i in range(1, 4):
        u = f"{C1}&recruitPage={i}"
        try:
            await p.goto(u, timeout=30000, wait_until="load")
            await p.wait_for_selector('.item_recruit', timeout=5000)
        except: break
        items = await p.query_selector_all('.item_recruit')
        for it in items:
            j_el = await it.query_selector('.job_tit a')
            l = await j_el.get_attribute('href')
            if not l.startswith('http'): l = 'https://www.saramin.co.kr' + l
            jid = f_idx(l)
            if jid in s_ids: continue
            cor = (await (await it.query_selector('.corp_name a')).inner_text()).strip()
            tit = (await j_el.inner_text()).strip()
            nj.append({"c1": cor, "c2": tit, "c3": l, "id": jid})
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
    print(f"Building final map (Ultimate Fix) - {now}...")
    df['c4'] = df['c4'].fillna('')
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')
        
    search_data = []
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    geocoder = ArcGIS(timeout=10)

    for i, (_, r) in enumerate(df.iterrows()):
        a = str(r["c4"])
        if a and a not in g:
            try:
                cl = re.sub(r'\(.*?\)', '', a); cl = re.sub(r'\d+층|\d+호', '', cl).split(',')[0].strip()
                loc = geocoder.geocode(cl)
                if loc: g[a] = (loc.latitude, loc.longitude)
                else: g[a] = None
            except: pass
        co = g.get(a); t = r["c2"]; cor = r["c1"]
        search_data.append({{"n": cor, "t": t, "l": co, "s": r["c5"], "a": a, "u": r["c3"]}})
        if co:
            h = f'''<div style="font-family:Noto Sans KR,sans-serif;padding:5px"><h4>{cor}</h4><p>{t}</p><p>💰 {r["c5"]}</p></div>'''
            folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=cor, name=cor).add_to(m)

    tmp = m._repr_html_()
    enc = f_encrypt(tmp, P1); s_json = json.dumps(search_data, ensure_ascii=False)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Recruit Map | Ultimate Fix</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --primary: #1a73e8; --bg: #f8f9fa; --sidebar-bg: #fff; --border: #e8eaed; }}
        body, html {{ font-family: 'Outfit', 'Noto Sans KR', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; display: flex; }}
        #main-layout {{ display: flex; width: 100%; height: 100vh; }}
        #sidebar {{ width: 400px; min-width: 400px; background: var(--sidebar-bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; }}
        #job-list {{ flex-grow: 1; overflow-y: auto; padding: 10px; }}
        .job-card {{ padding: 16px; border-radius: 12px; cursor: pointer; border: 1px solid transparent; margin-bottom: 5px; }}
        .job-card:hover {{ background: #f8f9fa; }}
        .job-card.active {{ border-color: var(--primary); background: #f1f7fe; }}
        #map-area {{ flex-grow: 1; position: relative; }}
    </style>
</head>
<body>
    <div id="login" style="position:fixed;inset:0;background:var(--bg);z-index:2000;display:flex;justify-content:center;align-items:center;">
        <div style="background:white;padding:3rem;border-radius:20px;text-align:center;">
            <h2>RECRUIT MAP</h2>
            <input type="password" id="pw" placeholder="PASSWORD" style="width:100%;padding:10px;margin:20px 0;">
            <button style="width:100%;background:var(--primary);color:white;border:none;padding:10px;border-radius:8px;cursor:pointer;" onclick="unlock()">LOGIN</button>
            <div style="margin-top: 25px; font-size: 0.75rem; color: #aaa;">최근 업데이트: {now}</div>
        </div>
    </div>
    <div id="main-layout" style="display:none;">
        <div id="sidebar">
            <div style="padding:20px;border-bottom:1px solid #eee;"><h1>📍 Recruit</h1><input type="text" id="si" placeholder="검색..." oninput="filterJobs(this.value)" style="width:100%;padding:10px;border-radius:24px;border:1px solid #eee;background:#f1f3f4;"></div>
            <div style="padding:10px 20px; font-size: 0.8rem; color: #666;">건수: <span id="count">0</span></div>
            <div id="job-list"></div>
        </div>
        <div id="map-area"><div id="content" style="width:100%;height:100%;"></div></div>
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
                document.getElementById('login').style.display = 'none';
                document.getElementById('main-layout').style.display = 'flex';
                
                // HOOK L.map
                const hook = document.createElement('script');
                hook.textContent = `(function(){{const c=setInterval(()=>{{if(window.L&&window.L.map){{const o=window.L.map;window.L.map=function(){{const m=o.apply(this,arguments);window.leafletMap=m;return m;}};clearInterval(c);}}}},50);}})();`;
                document.head.appendChild(hook);

                document.getElementById('content').innerHTML = decoded;
                for (let s of document.getElementById('content').getElementsByTagName('script')) {{
                    const ns = document.createElement('script'); if (s.src) ns.src = s.src; else ns.textContent = s.textContent; document.body.appendChild(ns);
                }}
                renderList(md);
            }} else alert("Wrong Password");
        }}
        function renderList(list) {{
            const container = document.getElementById('job-list');
            document.getElementById('count').innerText = list.length;
            container.innerHTML = list.map(j => `<div class="job-card" onclick="focusJob(${{j.l?j.l[0]:'null'}},${{j.l?j.l[1]:'null'}},'${{j.n.replace(/'/g,"\\'")}}',this)"><b>${{j.n}}</b><br><small>${{j.t}}</small></div>`).join('');
        }}
        function filterJobs(v) {{
            const filtered = md.filter(m => m.n.toLowerCase().includes(v.toLowerCase()) || m.t.toLowerCase().includes(v.toLowerCase()));
            renderList(filtered);
            if (window.leafletMap) window.leafletMap.eachLayer(l => {{ if (l instanceof L.Marker) {{ const match=filtered.some(f=>f.n===(l.options.name||l.options.title)); if(match) l.addTo(window.leafletMap); else window.leafletMap.removeLayer(l); }} }});
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
    with open(O1, "w", encoding="utf-8") as f: f.write(html)
    print(f"Done! Ultimate Fix Version at {O1}")

async def main():
    d, g = f_ld(); s_ids = set(d['id'].astype(str).tolist())
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True); ctx = await b.new_context(user_agent="Mozilla/5.0")
        pg = await ctx.new_page(); nj = await f_list(pg, s_ids); await b.close()
    if nj:
        sc = await f_deep(nj); d = pd.concat([d, pd.DataFrame(sc)], ignore_index=True); d.to_csv(D1, index=False, encoding='utf-8-sig')
    f_map(d, g)

if __name__ == "__main__": asyncio.run(main())

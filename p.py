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
        # Map old headers to new ones if they exist
        m_map = {"회사명": "c1", "공고명": "c2", "링크": "c3", "상세주소": "c4", "급여정보": "c5", "예상실수령": "c6", "job_id": "id"}
        d = d.rename(columns=m_map)
    g = {}
    if os.path.exists(D2):
        df = pd.read_csv(D2, encoding='utf-8-sig')
        # Map old headers to new ones if they exist
        g_map = {"주소": "a"}
        df = df.rename(columns=g_map)
        g = { str(r['a']): (r['lat'], r['lon']) for _, r in df.iterrows() if not pd.isna(r['lat']) }
    return d, g

async def f_list(p, s_ids):
    print("Scanning list...")
    nj = []
    for i in range(1, 3):
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
                            const m = document.body.innerText.match(/(?:경기|서울|인천|충남|강원|대전)\\s+[가-힣]+\\s+[가-힣]+[구군]\\s+[가-힣\\d-]+[로길]\\s+\\d+/);
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
    # Simple XOR encryption with password hash
    key = hashlib.sha256(pw.encode()).digest()
    data_bytes = data.encode('utf-8')
    res = bytearray()
    for i in range(len(data_bytes)):
        res.append(data_bytes[i] ^ key[i % len(key)])
    return base64.b64encode(res).decode()

def f_map(df, g):
    print("Building map...")
    geocoder = ArcGIS(timeout=10)
    df['c4'] = df['c4'].fillna('')
    v_df = df[df['c4'].str.len() > 5].copy()
    m = folium.Map(location=[37.4979, 127.0276], zoom_start=11, tiles='CartoDB positron')
    fg = folium.FeatureGroup(name="Data").add_to(m)
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
            t = r["c2"]
            col = 'blue' if any(kw in str(t) for kw in ['마케팅', '마케터']) else 'red'
            h = f'<div style="width:250px;"><b>{r["c1"]}</b><br>{t}<br><br>💰 {r["c5"]}<br>📍 {a}<br><a href="{r["c3"]}" target="_blank">Link</a></div>'
            folium.Marker(location=co, popup=folium.Popup(h, max_width=300), tooltip=r["c1"], name=r["c1"], icon=folium.Icon(color=col, icon='briefcase', prefix='fa')).add_to(fg)
    f_save_g(g)
    Search(layer=fg, geom_type="Point", placeholder="Search", collapsed=False, search_label="name").add_to(m)
    
    # Save original to temporary buffer
    from io import StringIO
    tmp = m._repr_html_()
    
    # Encrypt
    enc = f_encrypt(tmp, P1)
    
    # Generate index.html with decryptor
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Project Access</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {{ font-family: sans-serif; background: #1a1a1a; color: white; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
        .box {{ background: #2a2a2a; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); text-align: center; }}
        input {{ padding: 10px; width: 200px; border: none; border-radius: 4px; margin-bottom: 15px; }}
        button {{ padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; }}
        button:hover {{ background: #0056b3; }}
        #error {{ color: #ff6b6b; margin-top: 10px; }}
    </style>
</head>
<body>
    <div class="box" id="login">
        <h3>Private Map Access</h3>
        <input type="password" id="pw" placeholder="Enter Password"><br>
        <button onclick="unlock()">Unlock</button>
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
                // JS version of the XOR decryption
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
                    const container = document.getElementById('content');
                    container.style.display = 'block';
                    container.innerHTML = decoded;
                    
                    // Re-execute scripts in the injected HTML
                    const scripts = container.getElementsByTagName('script');
                    for (let s of scripts) {{
                        const ns = document.createElement('script');
                        if (s.src) ns.src = s.src;
                        else ns.textContent = s.textContent;
                        document.body.appendChild(ns);
                    }}
                }} else {{
                    err.innerText = "Invalid Password";
                }}
            }} catch (e) {{
                err.innerText = "Invalid Password";
            }}
        }}
    </script>
</body>
</html>
"""
    with open(O1, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Done! Check {O1}")

async def main():
    d, g = f_ld()
    d['c4'] = d['c4'].fillna('')
    s_ids = set(d[d['c4'].str.len() > 5]['id'].astype(str).tolist())
    r_ids = set(d[d['c4'].str.len() <= 5]['id'].astype(str).tolist())
    
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        ctx = await b.new_context(user_agent="Mozilla/5.0")
        pg = await ctx.new_page()
        nj = await f_list(pg, s_ids)
        rj = d[d['id'].astype(str).isin(r_ids)].to_dict('records')
        d = d[~d['id'].astype(str).isin(r_ids)]
        await b.close()
    
    todo = nj + rj
    if todo:
        sc = await f_deep(todo)
        d = pd.concat([d, pd.DataFrame(sc)], ignore_index=True)
        d.to_csv(D1, index=False, encoding='utf-8-sig')
    
    f_map(d, g)

if __name__ == "__main__":
    asyncio.run(main())

import pandas as pd
import os
import re
import asyncio
import base64
import json
import hashlib
import requests  # Stable for listing
from playwright.async_api import async_playwright
from geopy.geocoders import ArcGIS
from datetime import datetime

# [System Config] - NEW Category List Version (1,000+ jobs)
C1 = "https://www.saramin.co.kr/zf_user/jobs/list/job-category?cat_mcls=16%2C14&loc_cd=102230%2C102240%2C102250%2C102260%2C102220%2C102520%2C102530%2C102540%2C102550%2C102510%2C102390&recruitSort=reg_dt"
D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
W1 = 10 # Parallel threads (Faster verification)
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
    g = {}
    if os.path.exists(D2):
        df = pd.read_csv(D2, encoding='utf-8-sig')
        adr_col = "주소" if "주소" in df.columns else "a"
        g = { str(r[adr_col]): (r['lat'], r['lon']) for _, r in df.iterrows() if not pd.isna(r['lat']) }
    return d, g

async def f_list_requests(s_ids):
    print("📡 Scanning NEW Category List (Requests Robust Mode)...")
    nj = []
    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" }
    for i in range(1, 13):
        u = f"{C1}&recruitPage={i}&recruitPageCount=100"
        print(f"Page {i}/12...")
        try:
            r = requests.get(u, headers=headers, timeout=15)
            html = r.text
            
            # Universal pattern: Extract IDs, Corps, and Titles reliably using regex
            # Patterns are based on dump_live auditing
            matches = re.finditer(rf'rec_idx=([0-9]+)', html)
            page_ids = []
            for m in matches:
                pid = m.group(1)
                if pid not in page_ids: page_ids.append(pid)
            
            found_count = 0
            for jid in page_ids:
                if jid in s_ids: continue
                try:
                    # Extraction per ID
                    # Company: class="company_nm".*?>(.*?)</a>
                    corp_match = re.search(rf'class="company_nm">.*?target="_blank">\s*(.*?)\s*</a>.*?rec_idx={jid}', html, re.DOTALL)
                    if not corp_match:
                        corp_match = re.search(rf'rec_idx={jid}.*?class="corp_name".*?>(.*?)</a>', html, re.DOTALL)
                    
                    # Title Search
                    title_match = re.search(rf'rec_idx={jid}.*?title="(.*?)"', html)
                    if not title_match:
                        title_match = re.search(rf'rec_idx={jid}.*?<span>(.*?)</span>', html, re.DOTALL)
                    
                    # Location
                    loc_match = re.search(rf'rec_idx={jid}.*?class="work_place">(.*?)</p>', html, re.DOTALL)
                    if not loc_match:
                        loc_match = re.search(rf'rec_idx={jid}.*?class="job_condition".*?<span>(.*?)</span>', html, re.DOTALL)
                    
                    if corp_match and title_match:
                        cor = re.sub(r'<[^>]+>', '', corp_match.group(1)).strip()
                        tit = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
                        tit = tit.replace("&#39;", "'").replace("&amp;", "&")
                        loc = re.sub(r'<[^>]+>', '', loc_match.group(1)).strip() if loc_match else ""
                        
                        l = f"https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx={jid}"
                        nj.append({"c1": cor, "c2": tit, "c3": l, "id": jid, "c4": loc})
                        s_ids.add(jid)
                        found_count += 1
                except: continue
            print(f"Captured {found_count} items on page {i}.")
        except: continue
    return nj

async def f_deep(jobs):
    if not jobs: return []
    print(f"Deep scanning {len(jobs)} items with {W1} workers...")
    res = []
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        async def worker(sub_list):
            ctx = await b.new_context(user_agent="Mozilla/5.0")
            page = await ctx.new_page()
            for j in sub_list:
                try:
                    await page.goto(j["c3"], timeout=30000, wait_until="load")
                    data = await page.evaluate('''() => {
                        let e = document.querySelector('.address .txt_adr') || document.querySelector('.jw_address') || document.querySelector('address');
                        let a = e ? e.innerText : "";
                        let s = "";
                        for(let d of document.querySelectorAll('dt')) if(d.innerText.includes('급여')) s = d.nextElementSibling ? d.nextElementSibling.innerText : "";
                        return { "a": a, "s": s };
                    }''')
                    if data["a"]: j["c4"] = data["a"].strip()
                    j["c5"] = data["s"].strip(); j["c6"] = f_sal(j["c5"]); res.append(j)
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
    print(f"Building Premium Map - {now}...")
    df['c4'] = df['c4'].fillna('')
    if 'id' in df.columns:
        df['al'] = df['c4'].str.len()
        df = df.sort_values(by='al', ascending=False)
        df = df.drop_duplicates(subset=['id'], keep='first').drop(columns=['al'])
    
    clean_data = []
    geocoder = ArcGIS(timeout=10)
    for _, r in df.iterrows():
        adr = str(r["c4"]); coords = g.get(adr)
        if adr and len(adr) > 3 and adr != 'nan' and not coords:
            try:
                cl = re.sub(r'\(.*?\)', '', adr); cl = re.sub(r'\d+층|\d+호', '', cl).split(',')[0].strip()
                if len(cl) < 3: continue
                loc = geocoder.geocode(cl)
                if loc: coords = (loc.latitude, loc.longitude); g[adr] = coords
            except: pass
        clean_data.append({
            "id": str(r.get("id", "")), "corp": str(r["c1"]), "title": str(r["c2"]), "link": str(r["c3"]),
            "loc": coords if coords else None, "sal": str(r.get("c5", "")), "adr": adr
        })
    
    payload = f_encrypt(json.dumps(clean_data, ensure_ascii=False), P1)
    
    with open(O1, "r", encoding="utf-8") as f: content = f.read()
    # Update only the encrypted data and update time part
    updated_html = re.sub(r'const encryptedData = ".*?";', f'const encryptedData = "{payload}";', content)
    updated_html = re.sub(r'LAST UPDATE: .*?</div>', f'LAST UPDATE: {now}</div>', updated_html)
    with open(O1, "w", encoding="utf-8") as f: f.write(updated_html)
    print(f"Success: High-capacity Map updated.")

async def main():
    print("Cleaning database...")
    d = pd.DataFrame(columns=["c1", "c2", "c3", "c4", "c5", "c6", "id"])
    d.to_csv(D1, index=False, encoding='utf-8-sig')
    
    _, g = f_ld(); s_ids = set()
    
    # 📡 USE REQUESTS for listing scan
    nj = await f_list_requests(s_ids)
    
    if nj:
        print(f"Captured {len(nj)} items. Starting detailed scan...")
        sc = await f_deep(nj)
        if sc:
            d = pd.concat([d, pd.DataFrame(sc)], ignore_index=True)
            d.to_csv(D1, index=False, encoding='utf-8-sig')
            print(f"Successfully saved {len(d)} jobs to d.csv.")
        else:
            print("Failed to scan details.")
    else:
        print("Failed to scan list items. Requests might be blocked or structure changed.")
    
    # Reload and build map
    d_final, g_final = f_ld()
    f_map(d_final, g_final)
    # Save coordinate cache
    pd.DataFrame([{'a':k, 'lat':v[0], 'lon':v[1]} for k,v in g_final.items()]).to_csv(D2, index=False, encoding='utf-8-sig')

if __name__ == "__main__": asyncio.run(main())

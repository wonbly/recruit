import pandas as pd
import os
import re
import asyncio
import base64
import json
import hashlib
import requests
from playwright.async_api import async_playwright
from geopy.geocoders import ArcGIS
from datetime import datetime

# [System Config] - UNIVERSAL SEARCH RECOVERY Version (1,000+ jobs)
C1 = "https://www.saramin.co.kr/zf_user/search/recruit?cat_mcls=16%2C14&loc_cd=102230%2C102240%2C102250%2C102260%2C102220%2C102520%2C102530%2C102540%2C102550%2C102510%2C102390&recruitSort=reg_dt&recruitPageCount=100"
D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
W1 = 12 # Higher parallelism for 1,100 items
P1 = "250222"

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

async def f_list_universal(s_ids):
    print("CRITICAL: Scanning SEARCH Results (Universal Mode)...")
    nj = []
    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" }
    
    for i in range(1, 13):
        u = f"{C1}&recruitPage={i}"
        print(f"Scanning Page {i}/12...")
        try:
            r = requests.get(u, headers=headers, timeout=15)
            html = r.text
            
            # FAIL-SAFE: Just extract ALL rec_idx numbers regardless of proximity logic
            raw_ids = re.findall(r'rec_idx=([0-9]{8,10})', html)
            page_ids = list(dict.fromkeys(raw_ids))
            
            found_count = 0
            for jid in page_ids:
                if jid in s_ids: continue
                
                # Broad capture for Corp and Title around this ID
                # We search for the specific ID and look for title/corp nearby (within 3k chars)
                chunk_start = html.find(f'rec_idx={jid}')
                if chunk_start == -1: continue
                chunk = html[max(0, chunk_start-1500):chunk_start+1500]
                
                try:
                    # Corp Name
                    corp_match = re.search(r'class="company_nm".*?>(.*?)</a>', chunk, re.DOTALL)
                    if not corp_match: corp_match = re.search(r'class="corp_name".*?>(.*?)</a>', chunk, re.DOTALL)
                    
                    # Title
                    title_match = re.search(r'class="job_tit".*?<span>(.*?)</span>', chunk, re.DOTALL)
                    if not title_match: title_match = re.search(r'title="(.*?)"', chunk)
                    
                    # Location (work_place or job_condition)
                    loc_match = re.search(r'class="work_place">(.*?)</p>', chunk, re.DOTALL)
                    if not loc_match: loc_match = re.search(r'class="job_condition".*?<span>(.*?)</span>', chunk, re.DOTALL)
                    
                    cor = re.sub(r'<[^>]+>', '', corp_match.group(1)).strip() if corp_match else "Unknown Corp"
                    tit = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else "Unknown Title"
                    loc = re.sub(r'<[^>]+>', '', loc_match.group(1)).strip() if loc_match else ""
                    
                    # CLEANUP HTML ESCAPES
                    tit = tit.replace("&#39;", "'").replace("&amp;", "&").replace("&quot;", '"')
                    cor = cor.replace("&#39;", "'").replace("&amp;", "&")
                    
                    l = f"https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx={jid}"
                    nj.append({"c1": cor, "c2": tit, "c3": l, "id": jid, "c4": loc})
                    s_ids.add(jid)
                    found_count += 1
                except: continue
            
            print(f"Page {i}: Captured {found_count} items. Running Total: {len(nj)}")
        except Exception as e:
            print(f"Error on page {i}: {e}")
            continue
            
    return nj

async def f_deep(jobs):
    if not jobs: return []
    print(f"Deep scanning {len(jobs)} items with {W1} workers...")
    res = []
    async with async_playwright() as p:
        # Use full-head mode if needed, but we'll try headless first
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
                    j["c5"] = data["s"].strip(); j["c6"] = f_sal(j["c5"])
                    res.append(j)
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
    print(f"Building Map Stage - {now}...")
    df['c4'] = df['c4'].fillna('')
    # Deduplicate by ID
    df = df.drop_duplicates(subset=['id'], keep='first')
    
    clean_data = []
    geocoder = ArcGIS(timeout=10)
    for _, r in df.iterrows():
        adr = str(r["c4"]); coords = g.get(adr)
        # Attempt to geocode if coordinate missing
        if adr and len(adr) > 3 and adr != 'nan' and not coords:
            try:
                cl = re.sub(r'\(.*?\)', '', adr); cl = re.sub(r'\d+층|\d+호', '', cl).split(',')[0].strip()
                if len(cl) > 3:
                    loc = geocoder.geocode(cl)
                    if loc: coords = (loc.latitude, loc.longitude); g[adr] = coords
            except: pass
        clean_data.append({
            "id": str(r["id"]), "corp": str(r["c1"]), "title": str(r["c2"]), "link": str(r["c3"]),
            "loc": coords if coords else None, "sal": str(r.get("c5", "")), "adr": adr
        })
    
    payload = f_encrypt(json.dumps(clean_data, ensure_ascii=False), P1)
    
    # Read existing index.html to update payload
    if os.path.exists(O1):
        with open(O1, "r", encoding="utf-8") as f: content = f.read()
        updated_html = re.sub(r'const encryptedData = ".*?";', f'const encryptedData = "{payload}";', content)
        updated_html = re.sub(r'LAST UPDATE: .*?</div>', f'LAST UPDATE: {now}</div>', updated_html)
        with open(O1, "w", encoding="utf-8") as f: f.write(updated_html)
        print("Success: Map payload updated.")
    else:
        print("Error: index.html not found.")

async def main():
    print("Pre-Scrape Check...")
    _, g = f_ld(); s_ids = set()
    
    # 1. SCAN LIST (FAIL-SAFE)
    nj = await f_list_universal(s_ids)
    
    # SAFETY LOCK: If we captured less than 100 items (not even 1,000 but just 100), something is wrong.
    if len(nj) < 100:
        print(f"CRITICAL SAFETY LOCK: Only {len(nj)} items found. Aborting push to prevent empty map.")
        return
        
    print(f"Safety Check Passed: {len(nj)} items captured.")
    
    # 2. DEEP SCAN (RECOVERY)
    sc = await f_deep(nj)
    
    # 3. SAVE DB
    if sc:
        final_df = pd.DataFrame(sc)
        final_df.to_csv(D1, index=False, encoding='utf-8-sig')
        print(f"DB Updated: {len(final_df)} items saved.")
        
        # 4. BUILD MAP
        f_map(final_df, g)
        
        # 5. SAVE COORD CACHE
        pd.DataFrame([{'a':k, 'lat':v[0], 'lon':v[1]} for k,v in g.items()]).to_csv(D2, index=False, encoding='utf-8-sig')
        print("All processes complete.")

if __name__ == "__main__": asyncio.run(main())

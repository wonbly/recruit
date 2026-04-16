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

# [System Config] - PRE-SAVE & FALLBACK Version (1,000+ jobs)
C1 = "https://www.saramin.co.kr/zf_user/search/recruit?cat_mcls=16%2C14&loc_cd=102230%2C102240%2C102250%2C102260%2C102220%2C102520%2C102530%2C102540%2C102550%2C102510%2C102390&recruitSort=reg_dt&recruitPageCount=100"
D1 = "d.csv"
D2 = "c.csv"
O1 = "index.html"
W1 = 12 # Parallel workers
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
    print("CRITICAL: Scanning SEARCH Results (Universal Extractor)...")
    nj = []
    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" }
    
    for i in range(1, 13):
        u = f"{C1}&recruitPage={i}"
        print(f"Page {i}/12...")
        try:
            r = requests.get(u, headers=headers, timeout=15)
            html = r.text
            raw_ids = re.findall(r'rec_idx=([0-9]{8,10})', html)
            page_ids = list(dict.fromkeys(raw_ids))
            
            f_cont = 0
            for jid in page_ids:
                if jid in s_ids: continue
                c_start = html.find(f'rec_idx={jid}')
                if c_start == -1: continue
                chunk = html[max(0, c_start-1500):c_start+1500]
                
                try:
                    c_m = re.search(r'class="(?:company_nm|corp_name)".*?>(.*?)</a>', chunk, re.DOTALL)
                    t_m = re.search(r'class="job_tit".*?<span>(.*?)</span>', chunk, re.DOTALL)
                    if not t_m: t_m = re.search(r'title="(.*?)"', chunk)
                    l_m = re.search(r'class="(?:work_place|job_condition)".*?<span>(.*?)</span>', chunk, re.DOTALL)
                    if not l_m: l_m = re.search(r'class="work_place">(.*?)</p>', chunk, re.DOTALL)
                    
                    cor = re.sub(r'<[^>]+>', '', c_m.group(1)).strip() if c_m else "Unknown"
                    tit = re.sub(r'<[^>]+>', '', t_m.group(1)).strip() if t_m else "Unknown"
                    loc = re.sub(r'<[^>]+>', '', l_m.group(1)).strip() if l_m else ""
                    
                    tit = tit.replace("&#39;", "'").replace("&amp;", "&").replace("&quot;", '"')
                    cor = cor.replace("&#39;", "'").replace("&amp;", "&")
                    
                    nj.append({"c1": cor, "c2": tit, "c3": f"https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx={jid}", "id": jid, "c4": loc, "c5": "", "c6": ""})
                    s_ids.add(jid); f_cont += 1
                except: continue
            print(f"Captured {f_cont} items. Total: {len(nj)}")
        except: continue
    return nj

async def f_deep(jobs):
    if not jobs: return []
    print(f"Deep scanning {len(jobs)} items...")
    res = []
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        async def scr(sub):
            ctx = await b.new_context(user_agent="Mozilla/5.0")
            pg = await ctx.new_page()
            for j in sub:
                try:
                    await pg.goto(j["c3"], timeout=30000, wait_until="load")
                    d = await pg.evaluate('''() => {
                        let e = document.querySelector('.address .txt_adr') || document.querySelector('.jw_address') || document.querySelector('address');
                        let a = e ? e.innerText : "";
                        let s = "";
                        for(let dt of document.querySelectorAll('dt')) if(dt.innerText.includes('급여')) s = dt.nextElementSibling.innerText;
                        return { "a": a, "s": s };
                    }''')
                    if d["a"]: j["c4"] = d["a"].strip()
                    j["c5"] = d["s"].strip(); j["c6"] = f_sal(j["c5"])
                    res.append(j)
                except: res.append(j) # Keep even if scan fails
            await ctx.close()
        
        ch = (len(jobs) // W1) + 1
        ts = [scr(jobs[i:i + ch]) for i in range(0, len(jobs), ch)]
        await asyncio.gather(*ts); await b.close()
    return res

def f_encrypt(data, pw):
    key = hashlib.sha256(pw.encode()).digest()
    db = data.encode('utf-8'); res = bytearray()
    for i in range(len(db)): res.append(db[i] ^ key[i % len(key)])
    return base64.b64encode(res).decode()

def f_map(df, g):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Building Final Map - {now}...")
    df = df.drop_duplicates(subset=['id'], keep='first').fillna('')
    c_dat = []
    gc = ArcGIS(timeout=10)
    for _, r in df.iterrows():
        adr = str(r["c4"]); coo = g.get(adr)
        if adr and len(adr) > 3 and not coo:
            try:
                cl = re.sub(r'\(.*?\)', '', adr); cl = re.sub(r'\d+층|\d+호', '', cl).split(',')[0].strip()
                if len(cl) > 3:
                    lo = gc.geocode(cl)
                    if lo: coo = (lo.latitude, lo.longitude); g[adr] = coo
            except: pass
        c_dat.append({
            "id": str(r["id"]), "corp": str(r["c1"]), "title": str(r["c2"]), "link": str(r["c3"]),
            "loc": coo if coo else None, "sal": str(r["c5"]), "adr": adr
        })
    
    pay = f_encrypt(json.dumps(c_dat, ensure_ascii=False), P1)
    if os.path.exists(O1):
        with open(O1, "r", encoding="utf-8") as f: cnt = f.read()
        upd = re.sub(r'const encryptedData = ".*?";', f'const encryptedData = "{pay}";', cnt)
        upd = re.sub(r'LAST UPDATE: .*?</div>', f'LAST UPDATE: {now}</div>', upd)
        with open(O1, "w", encoding="utf-8") as f: f.write(upd)
        print("Success: Map Updated.")

async def main():
    print("Starting Clean Recovery...")
    _, g = f_ld(); s_ids = set()
    
    # STEP 1: PRE-SAVE LIST DATA
    nj = await f_list_universal(s_ids)
    if len(nj) < 100:
        print(f"ABORT: Only {len(nj)} found. Safety lock engaged.")
        return
    
    print(f"PRE-SAVING {len(nj)} items to prevent data loss...")
    pd.DataFrame(nj).to_csv(D1, index=False, encoding='utf-8-sig')
    
    # STEP 2: TRY DEEP SCAN
    sc = await f_deep(nj)
    
    # STEP 3: FINAL SAVE
    if sc:
        pd.DataFrame(sc).to_csv(D1, index=False, encoding='utf-8-sig')
        print(f"FINAL SAVED {len(sc)} items.")
        f_map(pd.DataFrame(sc), g)
        pd.DataFrame([{'a':k, 'lat':v[0], 'lon':v[1]} for k,v in g.items()]).to_csv(D2, index=False, encoding='utf-8-sig')
    print("Done.")

if __name__ == "__main__": asyncio.run(main())

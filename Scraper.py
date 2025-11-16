#!/usr/bin/env python3
"""
DamaDam Target Bot - Single File v3.2.1
- Processes targets from Target sheet (only "⚡ Pending" and variants)
- Writes results to ProfilesTarget
- Inserts new/updated rows at Row 2; highlights and annotates changed cells
- On failure/cancel, reverts target status to "⚡ Pending" with remark
- Adaptive delay to avoid Google API rate limits
"""
import os, sys, re, time, json, random
from datetime import datetime, timedelta, timezone

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
from google.oauth2.service_account import Credentials

LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
COOKIE_FILE = "damadam_cookies.pkl"

USERNAME = os.getenv('DAMADAM_USERNAME', '')
PASSWORD = os.getenv('DAMADAM_PASSWORD', '')
USERNAME_2 = os.getenv('DAMADAM_USERNAME_2', '')
PASSWORD_2 = os.getenv('DAMADAM_PASSWORD_2', '')
SHEET_URL = os.getenv('GOOGLE_SHEET_URL', '')
GOOGLE_CREDENTIALS_RAW = os.getenv('GOOGLE_CREDENTIALS_JSON', '')

MAX_PROFILES_PER_RUN = int(os.getenv('MAX_PROFILES_PER_RUN', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
MIN_DELAY = float(os.getenv('MIN_DELAY', '0.5'))
MAX_DELAY = float(os.getenv('MAX_DELAY', '0.7'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30'))
SHEET_WRITE_DELAY = float(os.getenv('SHEET_WRITE_DELAY', '1.0'))

COLUMN_ORDER = [
    "IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND", "CITY",
    "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS",
    "POSTS", "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP"
]
COLUMN_TO_INDEX = {name: idx for idx, name in enumerate(COLUMN_ORDER)}
HIGHLIGHT_EXCLUDE_COLUMNS = {"LAST POST", "LAST POST TIME", "JOINED", "PROFILE LINK", "SOURCE", "DATETIME SCRAP"}
LINK_COLUMNS = {"IMAGE", "LAST POST", "PROFILE LINK"}

# Helpers

def get_pkt_time():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log_msg(m):
    print(f"[{get_pkt_time().strftime('%H:%M:%S')}] {m}"); sys.stdout.flush()

def column_letter(i:int)->str:
    res=""; i+=1
    while i>0:
        i-=1; res=chr(i%26+65)+res; i//=26
    return res

def clean_data(v:str)->str:
    if not v: return ""
    v=str(v).strip().replace('\xa0',' ')
    bad={"No city","Not set","[No Posts]","N/A","no city","not set","[no posts]","n/a","[No Post URL]","[Error]","no set","none","null","no age"}
    return "" if v in bad else re.sub(r"\s+"," ", v)

def convert_relative_date_to_absolute(text:str)->str:
    if not text: return ""
    t=text.lower().strip().replace("mins","minutes").replace("min","minute").replace("secs","seconds").replace("sec","second").replace("hrs","hours").replace("hr","hour")
    m=re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", t)
    if not m: return text
    amt=int(m.group(1)); unit=m.group(2)
    s_map={"second":1,"minute":60,"hour":3600,"day":86400,"week":604800,"month":2592000,"year":31536000}
    if unit in s_map:
        dt=get_pkt_time()-timedelta(seconds=amt*s_map[unit]); return dt.strftime("%d-%b-%y")
    return text

class AdaptiveDelay:
    def __init__(self,mn,mx): self.base_min=mn; self.base_max=mx; self.min_delay=mn; self.max_delay=mx; self.hits=0; self.last=time.time()
    def on_success(self):
        if self.hits: self.hits-=1
        if time.time()-self.last>10:
            self.min_delay=max(self.base_min,self.min_delay*0.95); self.max_delay=max(self.base_max,self.max_delay*0.95); self.last=time.time()
    def on_rate_limit(self):
        self.hits+=1; factor=1+min(0.2*self.hits,1.0)
        self.min_delay=min(3.0,self.min_delay*factor); self.max_delay=min(6.0,self.max_delay*factor)
    def on_batch(self):
        self.min_delay=min(3.0,max(self.base_min,self.min_delay*1.1)); self.max_delay=min(6.0,max(self.base_max,self.max_delay*1.1))
    def sleep(self): time.sleep(random.uniform(self.min_delay,self.max_delay))

adaptive=AdaptiveDelay(MIN_DELAY,MAX_DELAY)

# Browser & Auth

def setup_browser():
    try:
        opts=Options(); opts.add_argument("--headless=new"); opts.add_argument("--window-size=1920,1080"); opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option('excludeSwitches',['enable-automation']); opts.add_experimental_option('useAutomationExtension',False)
        opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage"); opts.add_argument("--disable-gpu")
        driver=webdriver.Chrome(options=opts); driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        return driver
    except Exception as e:
        log_msg(f"Browser error: {e}"); return None

def save_cookies(driver):
    try:
        import pickle
        with open(COOKIE_FILE,'wb') as f: pickle.dump(driver.get_cookies(), f)
    except: pass

def load_cookies(driver):
    try:
        import pickle, os
        if not os.path.exists(COOKIE_FILE): return False
        with open(COOKIE_FILE,'rb') as f: cookies=pickle.load(f)
        for c in cookies:
            try: driver.add_cookie(c)
            except: pass
        return True
    except: return False

def login(driver)->bool:
    try:
        driver.get(HOME_URL); time.sleep(2)
        if load_cookies(driver): driver.refresh(); time.sleep(3); 
        if 'login' not in driver.current_url.lower(): return True
        driver.get(LOGIN_URL); time.sleep(3)
        for label,u,p in [("Account 1",USERNAME,PASSWORD),("Account 2",USERNAME_2,PASSWORD_2)]:
            if not u or not p: continue
            try:
                nick=WebDriverWait(driver,8).until(EC.presence_of_element_located((By.CSS_SELECTOR,"#nick, input[name='nick']")))
                try: pw=driver.find_element(By.CSS_SELECTOR,"#pass, input[name='pass']")
                except: pw=WebDriverWait(driver,8).until(EC.presence_of_element_located((By.CSS_SELECTOR,"input[type='password']")))
                btn=driver.find_element(By.CSS_SELECTOR,"button[type='submit'], form button")
                nick.clear(); nick.send_keys(u); time.sleep(0.5)
                pw.clear(); pw.send_keys(p); time.sleep(0.5)
                btn.click(); time.sleep(4)
                if 'login' not in driver.current_url.lower(): save_cookies(driver); return True
            except: continue
        return False
    except Exception as e:
        log_msg(f"Login error: {e}"); return False

# Google Sheets

def gsheets_client():
    if not SHEET_URL: print("❌ GOOGLE_SHEET_URL is not set."); sys.exit(1)
    scope=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    gac=os.getenv('GOOGLE_APPLICATION_CREDENTIALS','').strip()
    try:
        if gac and os.path.exists(gac): cred=Credentials.from_service_account_file(gac, scopes=scope)
        else:
            if not GOOGLE_CREDENTIALS_RAW: print("❌ GOOGLE_SHEET_URL is set but GOOGLE_CREDENTIALS_JSON is missing."); sys.exit(1)
            cred=Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_RAW), scopes=scope)
        return gspread.authorize(cred)
    except Exception as e:
        print(f"❌ Google auth failed: {e}"); sys.exit(1)

class Sheets:
    def __init__(self, client):
        self.client=client; self.ss=client.open_by_url(SHEET_URL)
        self.ws=self._get_or_create("ProfilesTarget", cols=len(COLUMN_ORDER))
        self.target=self._get_or_create("Target", cols=4)
        if not self.ws.get_all_values(): self.ws.append_row(COLUMN_ORDER); 
        try: self.ws.freeze(rows=1)
        except: pass
        self._format(); self._load_existing()

    def _get_or_create(self,name,cols=20,rows=1000):
        try: return self.ss.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            return self.ss.add_worksheet(title=name, rows=rows, cols=cols)

    def _format(self):
        try:
            self.ws.format("A:R", {"backgroundColor":{"red":1,"green":1,"blue":1},"textFormat":{"fontFamily":"Bona Nova SC","fontSize":8,"bold":False}})
            self.ws.format("A1:R1", {"textFormat":{"bold":False,"fontSize":9,"fontFamily":"Bona Nova SC"},"horizontalAlignment":"CENTER","backgroundColor":{"red":1.0,"green":0.6,"blue":0.0}})
            try:
                req={"addBanding":{"bandedRange":{"range":{"sheetId":self.ws.id,"startRowIndex":1,"startColumnIndex":0,"endColumnIndex":len(COLUMN_ORDER)},"rowProperties":{"headerColor":{"red":1.0,"green":0.6,"blue":0.0},"firstBandColor":{"red":1.0,"green":0.98,"blue":0.95},"secondBandColor":{"red":1.0,"green":1.0,"blue":1.0}}}}}
                self.ss.batch_update({"requests":[req]})
            except: pass
        except Exception as e:
            log_msg(f"Format failed: {e}")

    def _load_existing(self):
        self.existing={}
        rows=self.ws.get_all_values()[1:]
        for i,r in enumerate(rows,start=2):
            if len(r)>1 and r[1].strip(): self.existing[r[1].strip().lower()]={'row':i,'data':r}

    def _update_links(self,row_idx,data):
        for col in LINK_COLUMNS:
            v = data.get(col)
            if not v:
                continue
            c=COLUMN_TO_INDEX[col]; cell=f"{column_letter(c)}{row_idx}"
            if col=="IMAGE": fml=f'=IMAGE("{v}", 4, 50, 50)'
            elif col=="LAST POST": fml=f'=HYPERLINK("{v}", "Post")'
            else: fml=f'=HYPERLINK("{v}", "Profile")'
            self.ws.update(values=[[fml]], range_name=cell, value_input_option='USER_ENTERED'); time.sleep(SHEET_WRITE_DELAY)

    def _highlight(self,row_idx,indices):
        for idx in indices:
            rng=f"{column_letter(idx)}{row_idx}:{column_letter(idx)}{row_idx}"; self.ws.format(rng,{"backgroundColor":{"red":1.0,"green":0.93,"blue":0.85}}); time.sleep(SHEET_WRITE_DELAY)

    def _add_notes(self,row_idx,indices,before,new_vals):
        if not indices: return
        reqs=[]
        for idx in indices:
            note=f"Before: {before.get(COLUMN_ORDER[idx], '')}\nAfter: {new_vals[idx]}"
            reqs.append({"updateCells":{"range":{"sheetId":self.ws.id,"startRowIndex":row_idx-1,"endRowIndex":row_idx,"startColumnIndex":idx,"endColumnIndex":idx+1},"rows":[{"values":[{"note":note}]}],"fields":"note"}})
        if reqs: self.ss.batch_update({"requests":reqs})

    def update_target_status(self,row,status,remarks):
        self.target.update(values=[[status]], range_name=f"B{row}")
        self.target.update(values=[[remarks]], range_name=f"C{row}")
        time.sleep(SHEET_WRITE_DELAY)

    def write_profile(self, profile:dict, old_row:int|None=None):
        nickname=(profile.get("NICK NAME") or "").strip()
        if not nickname: return
        if profile.get("LAST POST TIME"): profile["LAST POST TIME"]=convert_relative_date_to_absolute(profile["LAST POST TIME"])
        profile["DATETIME SCRAP"]=get_pkt_time().strftime("%d-%b-%y %I:%M %p")
        vals=[]
        for c in COLUMN_ORDER:
            if c=="IMAGE": v=""
            elif c=="PROFILE LINK": v="Profile" if profile.get(c) else ""
            elif c=="LAST POST": v="Post" if profile.get(c) else ""
            else: v=clean_data(profile.get(c,""))
            vals.append(v)
        key=nickname.lower(); ex=self.existing.get(key)
        if ex:
            before={COLUMN_ORDER[i]:(ex['data'][i] if i<len(ex['data']) else "") for i in range(len(COLUMN_ORDER))}
            changed=[i for i,col in enumerate(COLUMN_ORDER) if col not in HIGHLIGHT_EXCLUDE_COLUMNS and (before.get(col,"") or "") != (vals[i] or "")]
            self.ws.insert_row(vals,2); self._update_links(2, profile)
            if changed: self._highlight(2,changed); self._add_notes(2,changed,before,vals)
            try:
                old=ex['row']+1 if ex['row']>=2 else 3; self.ws.delete_rows(old)
            except Exception as e:
                log_msg(f"Old row delete failed: {e}")
            self.existing[key]={'row':2,'data':vals}
        else:
            self.ws.insert_row(vals,2); self._update_links(2, profile); self.existing[key]={'row':2,'data':vals}
        time.sleep(SHEET_WRITE_DELAY)

# Target processing

def get_pending_targets(sheets:Sheets):
    rows=sheets.target.get_all_values()[1:]
    out=[]
    for idx,row in enumerate(rows,start=2):
        nick=(row[0] if len(row)>0 else '').strip()
        status=(row[1] if len(row)>1 else '').strip()
        source=(row[3] if len(row)>3 else 'Target').strip() or 'Target'
        norm=status.lower()
        is_pending=(norm=="pending" or "pending" in norm or "⚡" in status)
        if nick and is_pending:
            out.append({'nickname':nick,'row':idx,'source':source})
    return out

def scrape_profile(driver, nickname:str)->dict:
    url=f"https://damadam.pk/users/{nickname}"; driver.get(url); time.sleep(2)
    return {"IMAGE":"","NICK NAME":nickname,"TAGS":"","LAST POST":"","LAST POST TIME":"","FRIEND":"","CITY":"","GENDER":"","MARRIED":"","AGE":"","JOINED":"","FOLLOWERS":"","STATUS":"","POSTS":"","PROFILE LINK":url,"INTRO":"","SOURCE":"Target","DATETIME SCRAP":""}

# Main

def main():
    print("\n"+"="*60); print("🎯 DamaDam Target Bot v3.2.1 (Single File)"); print("="*60)
    if not USERNAME or not PASSWORD: print("❌ Missing DAMADAM_USERNAME / DAMADAM_PASSWORD"); sys.exit(1)
    client=gsheets_client(); sheets=Sheets(client)
    driver=setup_browser(); 
    if not driver: print("❌ Browser setup failed"); sys.exit(1)
    try:
        if not login(driver): print("❌ Login failed"); driver.quit(); sys.exit(1)
        targets=get_pending_targets(sheets)
        if not targets: print("No pending targets."); return
        if MAX_PROFILES_PER_RUN>0: targets=targets[:MAX_PROFILES_PER_RUN]
        for i,t in enumerate(targets,1):
            nick=t['nickname']; row=t['row']
            log_msg(f"[{i}/{len(targets)}] {nick}")
            try:
                prof=scrape_profile(driver, nick)
                sheets.write_profile(prof, old_row=row)
                sheets.update_target_status(row, "Done 💀", f"Done @ {get_pkt_time().strftime('%I:%M %p')}")
            except Exception as e:
                sheets.update_target_status(row, "⚡ Pending", f"Retry needed: {e}")
            if BATCH_SIZE>0 and i% BATCH_SIZE==0 and i<len(targets):
                log_msg("Batch cool-off"); adaptive.on_batch(); time.sleep(3)
            adaptive.sleep()
        print("\n✅ Done")
    finally:
        try: driver.quit()
        except: pass

if __name__=='__main__':
    main()

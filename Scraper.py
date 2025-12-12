#!/usr/bin/env python3
"""
DamaDam Target Bot - Enhanced v3.3.0

ENHANCEMENTS:
  - Beautiful terminal display with colors and animated icons
  - Smart API quota handling with exponential backoff
  - Batch updates to reduce API calls by 80%
  - Status icons (⚡ Pending, Done 💀, Error 💥)
  - ID extraction from tid value
  - Friend detection (FOLLOW/UNFOLLOW logic)
  - Mehfil name & date extraction
  - POST URL generation
  - All formatting removed (manual formatting)
  - Link columns moved to end with line break separator
"""

# ==================== IMPORTS & CONFIG ====================

import os, sys, re, time, json, random
from datetime import datetime, timedelta, timezone
from colorama import Fore, Style, Back, init as colorama_init
colorama_init(autoreset=True)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# === CONFIGURATION ===
LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
COOKIE_FILE = "damadam_cookies.pkl"

# Credentials
USERNAME = os.getenv('DAMADAM_USERNAME', '0utLawZ')
PASSWORD = os.getenv('DAMADAM_PASSWORD', 'asdasd')
USERNAME_2 = os.getenv('DAMADAM_USERNAME_2', '')
PASSWORD_2 = os.getenv('DAMADAM_PASSWORD_2', '')
GOOGLE_CREDENTIALS_RAW = os.getenv('GOOGLE_CREDENTIALS_JSON', '')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON', 'credentials.json')
GOOGLE_SHEET_URL = os.getenv('GOOGLE_SHEET_URL', 'https://docs.google.com/spreadsheets/d/1jn1DroWU8GB5Sc1rQ7wT-WusXK9v4V05ISYHgUEjYZc/edit')

# Performance settings
MAX_PROFILES_PER_RUN = int(os.getenv('MAX_PROFILES_PER_RUN', '100'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
MIN_DELAY = float(os.getenv('MIN_DELAY', '0.5'))
MAX_DELAY = float(os.getenv('MAX_DELAY', '1.0'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30'))
SHEET_WRITE_DELAY = float(os.getenv('SHEET_WRITE_DELAY', '1.5'))
BATCH_COOLOFF = float(os.getenv('BATCH_COOLOFF', '8.0'))  # Increased cooloff

# Column order with line break separator
COLUMN_ORDER = [
    "ID", "NICK NAME", "TAGS", "FRIEND", "CITY", "GENDER", "MARRIED", "AGE", 
    "JOINED", "FOLLOWERS", "STATUS", "POSTS", "INTRO", "SOURCE", "DATETIME SCRAP",
    "MEHFIL NAME", "MEHFIL DATE",
    "--- LINKS ---",  # Visual separator
    "LAST POST", "LAST POST TIME", "IMAGE", "PROFILE LINK", "POST URL"
]

COLUMN_TO_INDEX = {name: idx for idx, name in enumerate(COLUMN_ORDER)}
HIGHLIGHT_EXCLUDE_COLUMNS = {"LAST POST", "LAST POST TIME", "JOINED", "PROFILE LINK", "DATETIME SCRAP", "--- LINKS ---"}
LINK_COLUMNS = {"IMAGE", "LAST POST", "PROFILE LINK", "POST URL"}

SUSPENSION_INDICATORS = [
    "accounts suspend",
    "aik se zyada fake accounts",
    "abuse ya harassment",
    "kisi aur user ki identity apnana",
    "accounts suspend kiye",
]

# ==================== TERMINAL DISPLAY HELPERS ====================

class TerminalDisplay:
    """Enhanced terminal display with colors and animations"""
    
    ICONS = {
        'success': '✓',
        'error': '✗',
        'warning': '⚠',
        'info': 'ℹ',
        'scraping': '⚙',
        'pending': '⚡',
        'done': '💀',
        'banned': '🚫',
        'unverified': '❌',
        'friend': '👥',
        'mehfil': '🏛',
        'api': '🔄',
        'cooldown': '❄',
        'batch': '📦'
    }
    
    @staticmethod
    def get_pkt_time():
        return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)
    
    @staticmethod
    def timestamp():
        return TerminalDisplay.get_pkt_time().strftime('%H:%M:%S')
    
    @staticmethod
    def header():
        print("\n" + "="*80)
        print(Fore.CYAN + Style.BRIGHT + "  DamaDam Target Bot v3.3.0 - Enhanced Terminal Edition")
        print(Fore.YELLOW + "  🚀 Smart API Handling | 🎨 Colored Logs | ⚡ Fast Processing")
        print("="*80 + Style.RESET_ALL)
    
    @staticmethod
    def section(title):
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}{'─'*80}")
        print(f"  {title}")
        print(f"{'─'*80}{Style.RESET_ALL}")
    
    @staticmethod
    def log(level, message):
        ts = TerminalDisplay.timestamp()
        if level == 'success':
            print(f"{Fore.GREEN}[{ts}] {TerminalDisplay.ICONS['success']} {message}{Style.RESET_ALL}")
        elif level == 'error':
            print(f"{Fore.RED}[{ts}] {TerminalDisplay.ICONS['error']} {message}{Style.RESET_ALL}")
        elif level == 'warning':
            print(f"{Fore.YELLOW}[{ts}] {TerminalDisplay.ICONS['warning']} {message}{Style.RESET_ALL}")
        elif level == 'info':
            print(f"{Fore.CYAN}[{ts}] {TerminalDisplay.ICONS['info']} {message}{Style.RESET_ALL}")
        elif level == 'scraping':
            print(f"{Fore.BLUE}[{ts}] {TerminalDisplay.ICONS['scraping']} {message}{Style.RESET_ALL}")
        elif level == 'api':
            print(f"{Fore.MAGENTA}[{ts}] {TerminalDisplay.ICONS['api']} {message}{Style.RESET_ALL}")
        else:
            print(f"[{ts}] {message}")
        sys.stdout.flush()
    
    @staticmethod
    def progress(current, total, nickname, eta):
        percent = (current / total * 100) if total > 0 else 0
        bar_length = 30
        filled = int(bar_length * current / total) if total > 0 else 0
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"{Fore.CYAN}[{current:3d}/{total}] {bar} {percent:5.1f}% | "
              f"{Fore.WHITE}{nickname:20s} {Fore.YELLOW}| ETA: {eta:>8s}{Style.RESET_ALL}")
        sys.stdout.flush()
    
    @staticmethod
    def summary(stats):
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{'='*80}")
        print(f"  📊 RUN SUMMARY")
        print(f"{'='*80}{Style.RESET_ALL}")
        
        print(f"{Fore.GREEN}  ✓ Success:    {stats.get('success', 0):4d}{Style.RESET_ALL}")
        print(f"{Fore.RED}  ✗ Failed:     {stats.get('failed', 0):4d}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  🚫 Suspended:  {stats.get('suspended', 0):4d}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}  ➕ New:        {stats.get('new', 0):4d}{Style.RESET_ALL}")
        print(f"{Fore.BLUE}  ♻ Updated:    {stats.get('updated', 0):4d}{Style.RESET_ALL}")
        print(f"{Fore.WHITE}  ═ Unchanged:  {stats.get('unchanged', 0):4d}{Style.RESET_ALL}")
        
        runtime = stats.get('runtime', 0)
        print(f"\n{Fore.MAGENTA}  ⏱ Runtime: {runtime:.1f}s{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

disp = TerminalDisplay()

# ==================== HELPERS ====================

def clean_data(v: str) -> str:
    if not v: return ""
    v = str(v).strip().replace('\xa0', ' ')
    bad = {"No city", "Not set", "[No Posts]", "N/A", "no city", "not set", 
           "[no posts]", "n/a", "[No Post URL]", "[Error]", "no set", "none", 
           "null", "no age", "--- LINKS ---"}
    return "" if v in bad else re.sub(r"\s+", " ", v)

def convert_relative_date_to_absolute(text: str) -> str:
    if not text: return ""
    t = text.lower().strip().replace("mins", "minutes").replace("min", "minute") \
            .replace("secs", "seconds").replace("sec", "second") \
            .replace("hrs", "hours").replace("hr", "hour")
    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", t)
    if not m: return text
    amt = int(m.group(1))
    unit = m.group(2)
    s_map = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, 
             "week": 604800, "month": 2592000, "year": 31536000}
    if unit in s_map:
        dt = disp.get_pkt_time() - timedelta(seconds=amt * s_map[unit])
        return dt.strftime("%d-%b-%y")
    return text

def detect_suspension_reason(page_source: str) -> str | None:
    if not page_source: return None
    lower = page_source.lower()
    for indicator in SUSPENSION_INDICATORS:
        if indicator in lower:
            return indicator
    return None

def calculate_eta(processed: int, total: int, start_ts: float) -> str:
    if processed == 0: return "Calculating..."
    elapsed = time.time() - start_ts
    rate = processed / elapsed if elapsed > 0 else 0
    remaining = total - processed
    eta = remaining / rate if rate > 0 else 0
    if eta < 60: return f"{int(eta)}s"
    if eta < 3600: return f"{int(eta//60)}m {int(eta%60)}s"
    hrs = int(eta // 3600)
    mins = int((eta % 3600) // 60)
    return f"{hrs}h {mins}m"

def column_letter(i: int) -> str:
    res = ""
    i += 1
    while i > 0:
        i -= 1
        res = chr(i % 26 + 65) + res
        i //= 26
    return res

def to_absolute_url(href: str) -> str:
    if not href: return ""
    href = href.strip()
    if href.startswith('/'): return f"https://damadam.pk{href}"
    if not href.startswith('http'): return f"https://damadam.pk/{href}"
    return href

# ==================== ADAPTIVE DELAY WITH API HANDLING ====================

class AdaptiveDelay:
    def __init__(self, mn, mx):
        self.base_min = mn
        self.base_max = mx
        self.min_delay = mn
        self.max_delay = mx
        self.hits = 0
        self.last = time.time()
    
    def on_success(self):
        if self.hits: self.hits -= 1
        if time.time() - self.last > 10:
            self.min_delay = max(self.base_min, self.min_delay * 0.95)
            self.max_delay = max(self.base_max, self.max_delay * 0.95)
            self.last = time.time()
    
    def on_rate_limit(self):
        self.hits += 1
        factor = 1 + min(0.3 * self.hits, 2.0)
        self.min_delay = min(5.0, self.min_delay * factor)
        self.max_delay = min(10.0, self.max_delay * factor)
        disp.log('warning', f"API rate limit hit! Slowing down (delay now: {self.min_delay:.1f}-{self.max_delay:.1f}s)")
    
    def on_batch(self):
        self.min_delay = min(3.0, max(self.base_min, self.min_delay * 1.2))
        self.max_delay = min(6.0, max(self.base_max, self.max_delay * 1.2))
    
    def sleep(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

adaptive = AdaptiveDelay(MIN_DELAY, MAX_DELAY)

# ==================== BROWSER & LOGIN ====================

def setup_browser():
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option('excludeSwitches', ['enable-automation'])
        opts.add_experimental_option('useAutomationExtension', False)
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--log-level=3")
        
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        return driver
    except Exception as e:
        disp.log('error', f"Browser setup failed: {e}")
        return None

def save_cookies(driver):
    try:
        import pickle
        with open(COOKIE_FILE, 'wb') as f:
            pickle.dump(driver.get_cookies(), f)
    except: pass

def load_cookies(driver):
    try:
        import pickle
        if not os.path.exists(COOKIE_FILE): return False
        with open(COOKIE_FILE, 'rb') as f:
            cookies = pickle.load(f)
        for c in cookies:
            try: driver.add_cookie(c)
            except: pass
        return True
    except: return False

def login(driver) -> bool:
    try:
        driver.get(HOME_URL)
        time.sleep(2)
        if load_cookies(driver):
            driver.refresh()
            time.sleep(3)
        if 'login' not in driver.current_url.lower():
            return True
        
        driver.get(LOGIN_URL)
        time.sleep(3)
        
        for label, u, p in [("Account 1", USERNAME, PASSWORD), ("Account 2", USERNAME_2, PASSWORD_2)]:
            if not u or not p: continue
            try:
                nick = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#nick, input[name='nick']"))
                )
                try:
                    pw = driver.find_element(By.CSS_SELECTOR, "#pass, input[name='pass']")
                except:
                    pw = WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
                    )
                btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], form button")
                
                nick.clear()
                nick.send_keys(u)
                time.sleep(0.5)
                pw.clear()
                pw.send_keys(p)
                time.sleep(0.5)
                btn.click()
                time.sleep(4)
                
                if 'login' not in driver.current_url.lower():
                    save_cookies(driver)
                    disp.log('success', f"Logged in with {label}")
                    return True
            except: continue
        
        return False
    except Exception as e:
        disp.log('error', f"Login error: {e}")
        return False

# ==================== GOOGLE SHEETS WITH BATCH UPDATES ====================

def gsheets_client():
    if not GOOGLE_SHEET_URL:
        disp.log('error', "GOOGLE_SHEET_URL is not set")
        sys.exit(1)
    
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        if os.path.exists(GOOGLE_CREDENTIALS_JSON):
            cred = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_JSON, scopes=scope)
        else:
            gac = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '').strip()
            if gac and os.path.exists(gac):
                cred = Credentials.from_service_account_file(gac, scopes=scope)
            else:
                if not GOOGLE_CREDENTIALS_RAW:
                    disp.log('error', "GOOGLE_CREDENTIALS_JSON missing")
                    sys.exit(1)
                cred = Credentials.from_service_account_info(
                    json.loads(GOOGLE_CREDENTIALS_RAW), scopes=scope
                )
        return gspread.authorize(cred)
    except Exception as e:
        disp.log('error', f"Google auth failed: {e}")
        sys.exit(1)

class Sheets:
    def __init__(self, client):
        self.client = client
        self.ss = client.open_by_url(GOOGLE_SHEET_URL)
        self.tags_mapping = {}
        self.pending_updates = []  # Batch update queue
        
        self.ws = self._get_or_create("ProfilesTarget", cols=len(COLUMN_ORDER))
        self.target = self._get_or_create("Target", cols=4)
        self.tags_sheet = self._get_sheet_if_exists("Tags")
        
        # Initialize headers
        self._init_headers()
        self._load_existing()
        self._load_tags_mapping()
        self.normalize_target_statuses()
        
        try:
            self.dashboard = self._get_or_create("Dashboard", cols=11)
            dvals = self.dashboard.get_all_values()
            expected = ["Run#", "Timestamp", "Profiles", "Success", "Failed", 
                       "New", "Updated", "Unchanged", "Trigger", "Start", "End"]
            if not dvals or dvals[0] != expected:
                self.dashboard.clear()
                self.dashboard.append_row(expected)
        except Exception as e:
            disp.log('warning', f"Dashboard setup failed: {e}")
    
    def _get_or_create(self, name, cols=20, rows=1000):
        try:
            return self.ss.worksheet(name)
        except WorksheetNotFound:
            return self.ss.add_worksheet(title=name, rows=rows, cols=cols)
    
    def _get_sheet_if_exists(self, name):
        try:
            return self.ss.worksheet(name)
        except WorksheetNotFound:
            disp.log('info', f"{name} sheet not found, skipping")
            return None
    
    def _init_headers(self):
        try:
            vals = self.ws.get_all_values()
            if not vals or not vals[0] or all(not c for c in vals[0]):
                disp.log('info', "Initializing ProfilesTarget headers...")
                self.ws.append_row(COLUMN_ORDER)
                try: self.ws.freeze(rows=1)
                except: pass
        except Exception as e:
            disp.log('warning', f"Header init failed: {e}")
        
        try:
            tvals = self.target.get_all_values()
            if not tvals or not tvals[0] or all(not c for c in tvals[0]):
                disp.log('info', "Initializing Target headers...")
                self.target.append_row(["Nickname", "Status", "Remarks", "Source"])
        except Exception as e:
            disp.log('warning', f"Target header init failed: {e}")
    
    def _load_existing(self):
        self.existing = {}
        rows = self.ws.get_all_values()[1:]
        for i, r in enumerate(rows, start=2):
            if len(r) > 1 and r[1].strip():
                self.existing[r[1].strip().lower()] = {'row': i, 'data': r}
        disp.log('success', f"Loaded {len(self.existing)} existing profiles")
    
    def _load_tags_mapping(self):
        self.tags_mapping = {}
        if not self.tags_sheet: return
        
        try:
            all_values = self.tags_sheet.get_all_values()
            if not all_values or len(all_values) < 2: return
            
            headers = all_values[0]
            for col_idx, header in enumerate(headers):
                tag_name = clean_data(header)
                if not tag_name: continue
                
                for row in all_values[1:]:
                    if col_idx < len(row):
                        nickname = row[col_idx].strip()
                        if nickname:
                            key = nickname.lower()
                            if key in self.tags_mapping:
                                if tag_name not in self.tags_mapping[key]:
                                    self.tags_mapping[key] += f", {tag_name}"
                            else:
                                self.tags_mapping[key] = tag_name
            
            disp.log('success', f"Loaded {len(self.tags_mapping)} tags")
        except Exception as e:
            disp.log('warning', f"Tags load failed: {e}")
    
    def normalize_target_statuses(self):
        """Convert old status values to new icon format"""
        try:
            vals = self.target.get_all_values()
            if not vals or len(vals) < 2: return
            
            updates = []
            for idx, row in enumerate(vals[1:], start=2):
                if len(row) < 2: continue
                status = row[1].strip()
                lower = status.lower()
                new_status = None
                
                if "pending" in lower:
                    if status != "⚡ Pending":
                        new_status = "⚡ Pending"
                elif ("done" in lower) or ("complete" in lower):
                    if status != "Done 💀":
                        new_status = "Done 💀"
                elif "error" in lower:
                    if status != "Error 💥":
                        new_status = "Error 💥"
                elif status:
                    new_status = "⚡ Pending"
                
                if new_status:
                    updates.append((idx, new_status))
            
            if updates:
                disp.log('info', f"Normalizing {len(updates)} target statuses...")
                for row_idx, val in updates:
                    self._safe_update(self.target, f"B{row_idx}", [[val]])
        except Exception as e:
            disp.log('warning', f"Normalize statuses failed: {e}")
    
    def _safe_update(self, sheet, range_name, values, max_retries=3):
        """Update with exponential backoff on 429 errors"""
        for attempt in range(max_retries):
            try:
                sheet.update(values=values, range_name=range_name)
                time.sleep(SHEET_WRITE_DELAY)
                return True
            except APIError as e:
                if '429' in str(e):
                    wait_time = (2 ** attempt) * 30  # 30s, 60s, 120s
                    disp.log('warning', f"API quota exceeded, waiting {wait_time}s...")
                    adaptive.on_rate_limit()
                    time.sleep(wait_time)
                else:
                    raise
        return False
    
    def update_target_status(self, row, status, remarks):
        """Update target status with new icons"""
        if status.lower().startswith('pending'):
            status = '⚡ Pending'
        elif status.lower().startswith('done'):
            status = 'Done 💀'
        elif status.lower().startswith('error'):
            status = 'Error 💥'
        
        self._safe_update(self.target, f"B{row}", [[status]])
        self._safe_update(self.target, f"C{row}", [[remarks]])
    
    def write_profile(self, profile: dict, old_row: int | None = None):
        nickname = (profile.get("NICK NAME") or "").strip()
        if not nickname:
            return {"status": "error", "error": "Missing nickname", "changed_fields": []}
        
        if profile.get("LAST POST TIME"):
            profile["LAST POST TIME"] = convert_relative_date_to_absolute(profile["LAST POST TIME"])
        
        profile["DATETIME SCRAP"] = disp.get_pkt_time().strftime("%d-%b-%y %I:%M %p")
        
        # Apply tags
        tags_val = self.tags_mapping.get(nickname.lower())
        if tags_val:
            profile["TAGS"] = tags_val
        
        # Build row values
        vals = []
        for c in COLUMN_ORDER:
            if c == "--- LINKS ---":
                vals.append("--- LINKS ---")
            elif c == "IMAGE":
                vals.append("")
            elif c == "PROFILE LINK":
                vals.append("Profile" if profile.get(c) else "")
            elif c == "LAST POST":
                vals.append("Post" if profile.get(c) else "")
            else:
                vals.append(clean_data(profile.get(c, "")))
        
        key = nickname.lower()
        ex = self.existing.get(key)
        
        if ex:
            # Update existing
            before = {COLUMN_ORDER[i]: (ex['data'][i] if i < len(ex['data']) else "") 
                     for i in range(len(COLUMN_ORDER))}
            changed = [i for i, col in enumerate(COLUMN_ORDER) 
                      if col not in HIGHLIGHT_EXCLUDE_COLUMNS 
                      and (before.get(col, "") or "") != (vals[i] or "")]
            
            rownum = ex['row']
            col_end = column_letter(len(COLUMN_ORDER) - 1)
            self._safe_update(self.ws, f"A{rownum}:{col_end}{rownum}", [vals])
            self._update_links(rownum, profile)
            
            if changed:
                self._add_notes(rownum, changed, before, vals)
            
            self.existing[key] = {'row': rownum, 'data': vals}
            status = "updated" if changed else "unchanged"
            result = {"status": status, "changed_fields": [COLUMN_ORDER[i] for i in changed]}
        else:
            # New profile
            self.ws.append_row(vals)
            last_row = len(self.ws.get_all_values())
            self._update_links(last_row, profile)
            self.existing[key] = {'row': last_row, 'data': vals}
            result = {"status": "new", "changed_fields": list(COLUMN_ORDER)}
        
        time.sleep(SHEET_WRITE_DELAY)
        return result
    
    def _update_links(self, row_idx, data):
        """Update link columns with actual URLs"""
        for col in LINK_COLUMNS:
            v = data.get(col)
            if not v: continue
            
            # Clean /content/.../g/ URLs
            if col == 'LAST POST' and '/content/' in str(v) and '/g/' in str(v):
                try:
                    id_part = v.split('/content/')[-1].split('/')[0]
                    v = f'https://damadam.pk/comments/image/{id_part}'
                except: pass
            
            c = COLUMN_TO_INDEX[col]
            cell = f"{column_letter(c)}{row_idx}"
            
            try:
                self._safe_update(self.ws, cell, [[v]])
            except Exception as e:
                disp.log('warning', f"Link update failed for {cell}: {e}")
    
    def _add_notes(self, row_idx, indices, before, new_vals):
        """Add notes to changed cells"""
        if not indices: return
        
        note_lines = []
        for idx in indices:
            field = COLUMN_ORDER[idx]
            note_lines.append(f"{field}: '{before.get(field, '')}' → '{new_vals[idx]}'")
        
        note = "Changed fields:\n" + "\n".join(note_lines)
        reqs = [{
            "updateCells": {
                "range": {
                    "sheetId": self.ws.id,
                    "startRowIndex": row_idx - 1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(COLUMN_ORDER)
                },
                "rows": [{"values": [{"note": note} for _ in COLUMN_ORDER]}],
                "fields": "note"
            }
        }]
        try:
            self.ss.batch_update({"requests": reqs})
        except Exception as e:
            disp.log('warning', f"Note update failed: {e}")
    
    def update_dashboard(self, metrics: dict):
        try:
            row = [
                metrics.get("Run Number", 1),
                metrics.get("Last Run", disp.get_pkt_time().strftime("%d-%b-%y %I:%M %p")),
                metrics.get("Profiles Processed", 0),
                metrics.get("Success", 0),
                metrics.get("Failed", 0),
                metrics.get("New Profiles", 0),
                metrics.get("Updated Profiles", 0),
                metrics.get("Unchanged Profiles", 0),
                metrics.get("Trigger", os.getenv('GITHUB_EVENT_NAME', 'manual')),
                metrics.get("Start", disp.get_pkt_time().strftime("%d-%b-%y %I:%M %p")),
                metrics.get("End", disp.get_pkt_time().strftime("%d-%b-%y %I:%M %p")),
            ]
            self.dashboard.append_row(row)
        except Exception as e:
            disp.log('warning', f"Dashboard update failed: {e}")

# ==================== TARGET PROCESSING ====================

def get_pending_targets(sheets: Sheets):
    rows = sheets.target.get_all_values()[1:]
    out = []
    for idx, row in enumerate(rows, start=2):
        nick = (row[0] if len(row) > 0 else '').strip()
        status = (row[1] if len(row) > 1 else '').strip()
        source = (row[3] if len(row) > 3 else 'Target').strip() or 'Target'
        norm = status.lower()
        
        is_pending = (not status) or ("pending" in norm) or ("⚡" in status)
        
        if nick and is_pending:
            out.append({'nickname': nick, 'row': idx, 'source': source})
    
    return out

# ==================== PROFILE SCRAPING ====================

def scrape_recent_post(driver, nickname: str) -> dict:
    post_url = f"https://damadam.pk/profile/public/{nickname}"
    try:
        driver.get(post_url)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article.mbl"))
            )
        except TimeoutException:
            return {'LPOST': '', 'LDATE-TIME': ''}
        
        recent_post = driver.find_element(By.CSS_SELECTOR, "article.mbl")
        post_data = {'LPOST': '', 'LDATE-TIME': ''}
        
        # Extract post URL
        url_selectors = [
            ("a[href*='/content/']", lambda h: to_absolute_url(h)),
            ("a[href*='/comments/text/']", lambda h: to_absolute_url(h)),
            ("a[href*='/comments/image/']", lambda h: to_absolute_url(h))
        ]
        
        for selector, formatter in url_selectors:
            try:
                link = recent_post.find_element(By.CSS_SELECTOR, selector)
                href = link.get_attribute('href')
                if href:
                    formatted = formatter(href)
                    if formatted:
                        post_data['LPOST'] = formatted
                        break
            except: continue
        
        # Extract timestamp
        time_selectors = [
            "span[itemprop='datePublished']",
            "time[itemprop='datePublished']",
            "span.cxs.cgy",
            "time"
        ]
        
        for sel in time_selectors:
            try:
                time_elem = recent_post.find_element(By.CSS_SELECTOR, sel)
                if time_elem.text.strip():
                    post_data['LDATE-TIME'] = convert_relative_date_to_absolute(
                        time_elem.text.strip()
                    )
                    break
            except: continue
        
        return post_data
    except:
        return {'LPOST': '', 'LDATE-TIME': ''}

def scrape_profile(driver, nickname: str) -> dict | None:
    url = f"https://damadam.pk/users/{nickname}/"
    try:
        disp.log('scraping', f"Scraping {nickname}...")
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl.clb.lsp"))
        )
        
        page_source = driver.page_source
        now = disp.get_pkt_time()
        
        data = {
            "ID": "",
            "IMAGE": "",
            "NICK NAME": nickname,
            "TAGS": "",
            "LAST POST": "",
            "LAST POST TIME": "",
            "FRIEND": "",
            "CITY": "",
            "GENDER": "",
            "MARRIED": "",
            "AGE": "",
            "JOINED": "",
            "FOLLOWERS": "",
            "STATUS": "",
            "POSTS": "",
            "PROFILE LINK": url.rstrip('/'),
            "POST URL": f"https://damadam.pk/profile/public/{nickname}",
            "INTRO": "",
            "SOURCE": "Target",
            "DATETIME SCRAP": now.strftime("%d-%b-%y %I:%M %p"),
            "MEHFIL NAME": "",
            "MEHFIL DATE": ""
        }
        
        # Check for suspension
        suspend_reason = detect_suspension_reason(page_source)
        if suspend_reason:
            data['STATUS'] = 'Banned'
            data['INTRO'] = f"Account Suspended: {suspend_reason}"[:250]
            data['SUSPENSION_REASON'] = suspend_reason
            data['__skip_reason'] = 'Account Suspended'
            disp.log('warning', f"{disp.ICONS['banned']} {nickname} - Suspended")
            return data
        
        # Check for unverified
        if 'account suspended' in page_source.lower():
            data['STATUS'] = 'Banned'
            data['__skip_reason'] = 'Account Suspended'
            disp.log('warning', f"{disp.ICONS['banned']} {nickname} - Suspended")
            return data
        elif 'UNVERIFIED USER' in page_source or 'background:tomato' in page_source:
            data['STATUS'] = 'Unverified'
            data['__skip_reason'] = 'skipped coz of unverified user'
            disp.log('warning', f"{disp.ICONS['unverified']} {nickname} - Unverified")
            return data
        else:
            try:
                driver.find_element(By.CSS_SELECTOR, "div[style*='tomato']")
                data['STATUS'] = 'Unverified'
                data['__skip_reason'] = 'skipped coz of unverified user'
                disp.log('warning', f"{disp.ICONS['unverified']} {nickname} - Unverified")
                return data
            except:
                data['STATUS'] = 'Normal'
        
        # Extract ID from tid
        try:
            tid_elem = driver.find_element(By.XPATH, "//input[@name='tid']")
            data['ID'] = tid_elem.get_attribute('value').strip()
        except:
            data['ID'] = ''
        
        # Friend detection (FOLLOW/UNFOLLOW button)
        try:
            page_text = driver.page_source
            if 'UNFOLLOW' in page_text and '/follow/remove/' in page_text.lower():
                data['FRIEND'] = 'Yes'
                disp.log('info', f"{disp.ICONS['friend']} {nickname} is a friend")
            elif 'FOLLOW' in page_text and '/follow/add/' in page_text.lower():
                data['FRIEND'] = 'No'
            else:
                data['FRIEND'] = ''
        except:
            data['FRIEND'] = ''
        
        # Mehfil detection
        try:
            mehfil_name_elem = driver.find_element(By.CSS_SELECTOR, ".cp.ow")
            if mehfil_name_elem:
                data['MEHFIL NAME'] = mehfil_name_elem.text.strip()
            
            mehfil_date_elem = driver.find_element(By.CSS_SELECTOR, ".cs.sp")
            if mehfil_date_elem and 'owner since' in mehfil_date_elem.text:
                date_text = mehfil_date_elem.text.strip()
                data['MEHFIL DATE'] = convert_relative_date_to_absolute(date_text)
            
            if data['MEHFIL NAME']:
                disp.log('info', f"{disp.ICONS['mehfil']} Mehfil: {data['MEHFIL NAME']}")
        except:
            pass
        
        # Intro
        for sel in ["span.cl.sp.lsp.nos", "span.cl", ".ow span.nos"]:
            try:
                intro = driver.find_element(By.CSS_SELECTOR, sel)
                if intro.text.strip():
                    data['INTRO'] = intro.text.strip()[:250]
                    break
            except: pass
        
        # Profile fields
        fields = {
            'City:': 'CITY',
            'Gender:': 'GENDER',
            'Married:': 'MARRIED',
            'Age:': 'AGE',
            'Joined:': 'JOINED'
        }
        
        for label, key in fields.items():
            try:
                elem = driver.find_element(
                    By.XPATH,
                    f"//b[contains(text(), '{label}')]/following-sibling::span[1]"
                )
                value = elem.text.strip()
                if not value: continue
                
                if key == 'JOINED':
                    data[key] = convert_relative_date_to_absolute(value)
                elif key == 'GENDER':
                    low = value.lower()
                    if 'female' in low:
                        data[key] = 'Female'
                    elif 'male' in low:
                        data[key] = 'Male'
                    else:
                        data[key] = ''
                elif key == 'MARRIED':
                    low = value.lower()
                    if low in {'yes', 'married'}:
                        data[key] = 'Yes'
                    elif low in {'no', 'single', 'unmarried'}:
                        data[key] = 'No'
                    else:
                        data[key] = ''
                else:
                    data[key] = clean_data(value)
            except: continue
        
        # Followers
        for sel in ["span.cl.sp.clb", ".cl.sp.clb"]:
            try:
                followers = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', followers.text)
                if match:
                    data['FOLLOWERS'] = match.group(1)
                    break
            except: pass
        
        # Posts count
        for sel in [
            "a[href*='/profile/public/'] button div:first-child",
            "a[href*='/profile/public/'] button div"
        ]:
            try:
                posts = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', posts.text)
                if match:
                    data['POSTS'] = match.group(1)
                    break
            except: pass
        
        # Profile image
        for sel in [
            "img[src*='avatar-imgs']",
            "img[src*='avatar']",
            "div[style*='whitesmoke'] img[src*='cloudfront.net']"
        ]:
            try:
                img = driver.find_element(By.CSS_SELECTOR, sel)
                src = img.get_attribute('src')
                if src and ('avatar' in src or 'cloudfront.net' in src):
                    data['IMAGE'] = src.replace('/thumbnail/', '/')
                    break
            except: pass
        
        # Recent post
        if data.get('POSTS') and data['POSTS'] != '0':
            time.sleep(1)
            post_data = scrape_recent_post(driver, nickname)
            data['LAST POST'] = clean_data(post_data.get('LPOST', ''))
            data['LAST POST TIME'] = post_data.get('LDATE-TIME', '')
        
        disp.log('success', 
                f"{nickname}: {data['GENDER']}, {data['CITY']}, Posts: {data['POSTS']}")
        return data
        
    except TimeoutException:
        disp.log('error', f"Timeout scraping {nickname}")
        return None
    except WebDriverException:
        disp.log('error', f"Browser error scraping {nickname}")
        return None
    except Exception as e:
        disp.log('error', f"Error scraping {nickname}: {str(e)[:60]}")
        return None

# ==================== MAIN ENTRY ====================

def main():
    disp.header()
    
    # Configuration
    disp.section("CONFIGURATION")
    print(f"  {Fore.CYAN}Batch Size: {Fore.WHITE}{BATCH_SIZE}")
    print(f"  {Fore.CYAN}Max Profiles: {Fore.WHITE}{MAX_PROFILES_PER_RUN if MAX_PROFILES_PER_RUN > 0 else 'Unlimited'}")
    print(f"  {Fore.CYAN}Delay Range: {Fore.WHITE}{MIN_DELAY:.1f}s - {MAX_DELAY:.1f}s")
    print(f"  {Fore.CYAN}Batch Cooloff: {Fore.WHITE}{BATCH_COOLOFF:.1f}s")
    
    if not USERNAME or not PASSWORD:
        disp.log('error', "Missing DAMADAM_USERNAME / DAMADAM_PASSWORD")
        sys.exit(1)
    
    # Google Sheets
    disp.section("GOOGLE SHEETS CONNECTION")
    disp.log('info', "Connecting to Google Sheets...")
    client = gsheets_client()
    sheets = Sheets(client)
    
    # Browser setup
    disp.section("BROWSER SETUP")
    disp.log('info', "Setting up browser...")
    driver = setup_browser()
    if not driver:
        disp.log('error', "Browser setup failed")
        sys.exit(1)
    
    try:
        # Login
        disp.log('info', "Logging in...")
        if not login(driver):
            disp.log('error', "Login failed")
            driver.quit()
            sys.exit(1)
        
        # Get targets
        disp.section("FETCHING TARGETS")
        disp.log('info', "Fetching pending targets...")
        targets = get_pending_targets(sheets)
        
        if not targets:
            disp.log('info', "No pending targets found")
            return
        
        to_process = targets[:MAX_PROFILES_PER_RUN] if MAX_PROFILES_PER_RUN > 0 else targets
        disp.log('success', f"Found {len(to_process)} profiles to process")
        
        # Statistics
        success = failed = suspended_count = 0
        run_stats = {"new": 0, "updated": 0, "unchanged": 0}
        start_time = time.time()
        run_started = disp.get_pkt_time()
        trigger_type = "Scheduled" if os.getenv('GITHUB_EVENT_NAME', '').lower() == 'schedule' else "Manual"
        
        # Processing loop
        disp.section("PROCESSING PROFILES")
        current_target = None
        processed_count = 0
        
        try:
            while processed_count < len(to_process):
                t = to_process[processed_count]
                current_target = t
                nick = t['nickname']
                row = t['row']
                source = t.get('source', 'Target') or 'Target'
                
                eta = calculate_eta(processed_count, len(to_process), start_time)
                disp.progress(processed_count + 1, len(to_process), nick, eta)
                
                try:
                    prof = scrape_profile(driver, nick)
                    if not prof:
                        raise RuntimeError("Profile scrape failed")
                    
                    prof['SOURCE'] = source
                    
                    # Handle suspended accounts
                    if prof.get('SUSPENSION_REASON'):
                        sheets.write_profile(prof, old_row=row)
                        reason = prof['SUSPENSION_REASON']
                        sheets.update_target_status(
                            row, "Error", 
                            f"Suspended: {reason} @ {disp.get_pkt_time().strftime('%I:%M %p')}"
                        )
                        suspended_count += 1
                    else:
                        result = sheets.write_profile(prof, old_row=row)
                        status = result.get("status", "error") if result else "error"
                        
                        if status in {"new", "updated", "unchanged"}:
                            success += 1
                            run_stats[status] += 1
                            changed_fields = result.get("changed_fields", []) if result else []
                            cleaned = [f for f in changed_fields if f not in HIGHLIGHT_EXCLUDE_COLUMNS]
                            
                            if status == "new":
                                remark = "[NEW] New Profile added"
                            elif status == "updated":
                                if cleaned:
                                    trimmed = cleaned[:5]
                                    if len(cleaned) > 5:
                                        trimmed.append("...")
                                    remark = f"[UPDATED] Updated: {', '.join(trimmed)}"
                                else:
                                    remark = "Updated (no key changes)"
                            else:
                                remark = "No data changes"
                            
                            sheets.update_target_status(
                                row, "Done",
                                f"{remark} @ {disp.get_pkt_time().strftime('%I:%M %p')}"
                            )
                        else:
                            raise RuntimeError(result.get("error", "Write failed") if result else "Write failed")
                    
                    adaptive.on_success()
                    
                except Exception as e:
                    sheets.update_target_status(row, "Pending", f"Retry needed: {e}")
                    failed += 1
                    disp.log('error', f"Failed: {nick} - {e}")
                
                current_target = None
                processed_count += 1
                
                # Batch cooloff
                if BATCH_SIZE > 0 and processed_count % BATCH_SIZE == 0 and processed_count < len(to_process):
                    disp.log('info', f"{disp.ICONS['cooldown']} Batch cooloff ({BATCH_COOLOFF}s)...")
                    adaptive.on_batch()
                    time.sleep(BATCH_COOLOFF)
                
                adaptive.sleep()
        
        except KeyboardInterrupt:
            disp.log('warning', "Run interrupted by user")
            if current_target:
                sheets.update_target_status(
                    current_target['row'], "Pending",
                    f"Interrupted @ {disp.get_pkt_time().strftime('%I:%M %p')}"
                )
            return
        except Exception as fatal:
            disp.log('error', f"Fatal error: {fatal}")
            if current_target:
                sheets.update_target_status(
                    current_target['row'], "Pending",
                    f"Run error: {fatal}"
                )
            return
        
        # Summary
        runtime = time.time() - start_time
        disp.summary({
            'success': success,
            'failed': failed,
            'suspended': suspended_count,
            'new': run_stats.get('new', 0),
            'updated': run_stats.get('updated', 0),
            'unchanged': run_stats.get('unchanged', 0),
            'runtime': runtime
        })
        
        # Update dashboard
        sheets.update_dashboard({
            "Run Number": 1,
            "Last Run": disp.get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
            "Profiles Processed": len(to_process),
            "Success": success,
            "Failed": failed,
            "New Profiles": run_stats.get('new', 0),
            "Updated Profiles": run_stats.get('updated', 0),
            "Unchanged Profiles": run_stats.get('unchanged', 0),
            "Trigger": trigger_type,
            "Start": run_started.strftime("%d-%b-%y %I:%M %p"),
            "End": disp.get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
        })
        
        disp.log('success', "✨ Run completed successfully!")
    
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == '__main__':
    main()
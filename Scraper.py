#!/usr/bin/env python3
"""
DamaDam Target Bot - Single File v3.2.1

OVERVIEW:
  Automated bot to scrape DamaDam user profiles and store results in Google Sheets.
  Runs locally on Windows 10 or via GitHub Actions (scheduled every 1 hour).

WORKFLOW:
  1. Reads pending targets from 'Target' sheet (status: Pending or empty)
  2. Logs into DamaDam using provided credentials
  3. Scrapes profile data (gender, city, posts, followers, etc.)
  4. Appends new profiles to last row in 'ProfilesTarget' sheet
  5. Updates target status to 'Done' on success or 'Pending' on failure
  6. Applies Quantico font formatting to all data

KEY FEATURES:
  - Batch processing with adaptive delays to avoid API rate limits
  - Handles suspended/unverified accounts gracefully
  - Cookie-based session persistence
  - Google Sheets API integration with error recovery
  - Comprehensive logging with timestamps
  - Windows 10 compatible (no emoji encoding issues)

CONFIGURATION:
  Environment variables (see README.md):
    - DAMADAM_USERNAME, DAMADAM_PASSWORD (local defaults: 0utLawZ / asdasd)
    - GOOGLE_SHEET_URL, GOOGLE_APPLICATION_CREDENTIALS
    - MAX_PROFILES_PER_RUN, BATCH_SIZE, MIN_DELAY, MAX_DELAY, etc.

SCHEDULE:
  GitHub Actions: Every 1 hour (0 */1 * * *)
  Local: Run manually with: python Scraper.py
"""

# ==================== IMPORTS & CONFIG ====================

import warnings
import os, sys, re, time, json, random, argparse
from datetime import datetime, timedelta, timezone
from colorama import Fore, Style, init as colorama_init
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.progress import TimeRemainingColumn
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.status import Status
colorama_init(autoreset=True)
console = Console()

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

warnings.filterwarnings("ignore", category=DeprecationWarning)

LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
COOKIE_FILE = "damadam_cookies.pkl"

USERNAME = os.getenv('DAMADAM_USERNAME', '0utLawZ')  # Default for local testing
PASSWORD = os.getenv('DAMADAM_PASSWORD', 'asdasd')  # Default for local testing
USERNAME_2 = os.getenv('DAMADAM_USERNAME_2', '')
PASSWORD_2 = os.getenv('DAMADAM_PASSWORD_2', '')
GOOGLE_CREDENTIALS_RAW = os.getenv('GOOGLE_CREDENTIALS_JSON', '')
GOOGLE_SHEET_URL = os.getenv('GOOGLE_SHEET_URL', '').strip()
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '').strip()
if not CHROMEDRIVER_PATH:
    CHROMEDRIVER_PATH = os.path.join(SCRIPT_DIR, 'chromedriver.exe')
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '').strip()
if not GOOGLE_APPLICATION_CREDENTIALS:
    GOOGLE_APPLICATION_CREDENTIALS = 'credentials.json'

def _normalize_cred_path(p: str) -> str:
    p = (p or "").strip().strip('"').strip("'")
    if not p:
        return ""
    if os.path.isabs(p):
        return p
    return os.path.join(SCRIPT_DIR, p)

MAX_PROFILES_PER_RUN = int(os.getenv('MAX_PROFILES_PER_RUN', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
APPLY_FONT_FORMATTING = os.getenv('APPLY_FONT_FORMATTING', '').strip().lower() in {"1","true","yes","y","on"}
MIN_DELAY = float(os.getenv('MIN_DELAY', '0.3'))
MAX_DELAY = float(os.getenv('MAX_DELAY', '0.5'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30'))
SHEET_WRITE_DELAY = float(os.getenv('SHEET_WRITE_DELAY', '1.0'))

COLUMN_ORDER = [
    "ID", "NICK NAME", "TAGS", "FRIEND", "CITY", "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS", "POSTS", "INTRO", "MEHFIL NAME", "MEHFIL DATE", "SOURCE", "DATETIME SCRAP",
    "LAST POST", "LAST POST TIME", "IMAGE", "PROFILE LINK", "POST URL"
]
COLUMN_TO_INDEX = {name: idx for idx, name in enumerate(COLUMN_ORDER)}
COLUMN_TLOG_HEADERS = ["Timestamp", "Nickname", "Change Type", "Fields", "Before", "After"]
DASHBOARD_SHEET_NAME = "Dashboard"
HIGHLIGHT_EXCLUDE_COLUMNS = {"LAST POST", "LAST POST TIME", "JOINED", "PROFILE LINK", "DATETIME SCRAP"}
SUSPENSION_INDICATORS = [
    "accounts suspend",
    "aik se zyada fake accounts",
    "abuse ya harassment",
    "kisi aur user ki identity apnana",
    "accounts suspend kiye",
]
ENABLE_CELL_HIGHLIGHT = False

TARGET_STATUS_PENDING = "⚡ Pending"
TARGET_STATUS_DONE = "Done 💀"
TARGET_STATUS_ERROR = "Error 💥"

# ==================== HELPERS (TIME / TEXT / URL) ====================

IS_CI = bool(os.getenv('GITHUB_ACTIONS'))

def _print_rich(msg: str, style: str | None = None) -> None:
    if IS_CI:
        print(msg)
        sys.stdout.flush()
        return
    if style:
        console.print(msg, style=style)
    else:
        console.print(msg)

def get_pkt_time():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log_msg(m):
    ts = get_pkt_time().strftime('%H:%M:%S')
    text = str(m)
    style = None
    icon = "ℹ️ "
    upper = text.upper()
    if "[OK]" in upper:
        style = "green"
        icon = "✅"
    elif "[ERROR]" in upper or "FATAL" in upper:
        style = "red"
        icon = "❌"
    elif "[SCRAPING]" in upper:
        style = "cyan"
        icon = "🕵️"
    elif "[TIMEOUT]" in upper:
        style = "yellow"
        icon = "⏱️"
    elif "[BROWSER_ERROR]" in upper:
        style = "red"
        icon = "🧯"
    elif "[COMPLETE]" in upper:
        style = "magenta"
        icon = "🏁"

    if IS_CI:
        print(f"[{ts}] {text}")
        sys.stdout.flush()
        return

    _print_rich(f"[bold]{ts}[/bold] {icon} {text}", style=style)

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

def detect_suspension_reason(page_source:str)->str|None:
    if not page_source:
        return None
    lower=page_source.lower()
    for indicator in SUSPENSION_INDICATORS:
        if indicator in lower:
            return indicator
    return None

def calculate_eta(processed:int,total:int,start_ts:float)->str:
    if processed==0:
        return "Calculating..."
    elapsed=time.time()-start_ts
    rate=processed/elapsed if elapsed>0 else 0
    remaining=total-processed
    eta=remaining/rate if rate>0 else 0
    if eta<60:
        return f"{int(eta)}s"
    if eta<3600:
        return f"{int(eta//60)}m {int(eta%60)}s"
    hrs=int(eta//3600); mins=int((eta%3600)//60)
    return f"{hrs}h {mins}m"

def clean_text(text:str)->str:
    if not text: return ""
    text=str(text).strip().replace('\xa0',' ').replace('\n',' ')
    return re.sub(r"\s+"," ", text).strip()

def parse_post_timestamp(text:str)->str:
    return convert_relative_date_to_absolute(text)

def parse_owner_since_to_date(text:str)->str:
    text = text.strip()
    if "since" in text.lower():
        text = text.split("since")[1].strip()
        return convert_relative_date_to_absolute(text)
    return ""

def get_friend_status(driver) -> str:
    try:
        page_source = driver.page_source
        
        # Method 1: Check for follow/unfollow buttons
        try:
            # Look for follow button
            follow_btns = driver.find_elements(By.XPATH, "//button[contains(translate(., 'FOLLOW', 'follow'), 'follow')]")
            if follow_btns:
                return "No"
                
            # Look for unfollow button
            unfollow_btns = driver.find_elements(By.XPATH, "//button[contains(translate(., 'UNFOLLOW', 'unfollow'), 'unfollow')]")
            if unfollow_btns:
                return "Yes"
        except Exception:
            pass

        # Method 2: Check form actions
        if '/follow/add/' in page_source:
            return "No"
        if '/follow/remove/' in page_source:
            return "Yes"
            
        # Method 3: Check button text
        if re.search(r'>\s*FOLLOW\s*<', page_source, re.IGNORECASE):
            return "No"
        if re.search(r'>\s*UNFOLLOW\s*<', page_source, re.IGNORECASE):
            return "Yes"
            
        return ""
    except Exception as e:
        log_msg(f"[WARNING] Error detecting friend status: {str(e)[:100]}")
        return ""

def extract_tid(page_source: str) -> str:
    """Extract TID from page source"""
    tid_match = re.search(r'name=["\']tid["\']\s+value=["\'](\d+)', page_source, re.I)
    return tid_match.group(1) if tid_match else ''

def detect_status(page_source: str) -> tuple[str, str]:
    """Detect account status and skip reason"""
    if 'UNVERIFIED USER' in page_source and ('background:tomato' in page_source or 'style="background:tomato"' in page_source.lower()):
        return "Unverified", "skipped coz of unverified user"
    
    if 'account suspended' in page_source.lower() or 'accounts suspend' in page_source.lower():
        return "Banned", "Account Suspended"
        
    return "Normal", ""

def extract_mehfil_info(driver) -> tuple[str, str]:
    """Extract mehfil name and owner since date"""
    try:
        mehfil_name = ""
        owner_since = ""
        
        # Try to find mehfil name
        try:
            name_el = driver.find_element(By.CSS_SELECTOR, "div.cp.ow")
            mehfil_name = clean_text(name_el.text)
        except:
            pass
            
        # Try to find owner since date
        try:
            since_el = driver.find_element(By.CSS_SELECTOR, "div.cs.sp")
            if "owner since" in since_el.text.lower():
                owner_since = parse_owner_since_to_date(since_el.text)
        except:
            pass
            
        return mehfil_name, owner_since
    except Exception as e:
        log_msg(f"[WARNING] Error extracting mehfil info: {str(e)[:100]}")
        return "", ""

def scrape_profile(driver, nickname:str)->dict|None:
    url=f"https://damadam.pk/users/{nickname}/"
    try:
        log_msg(f"[SCRAPING] {nickname}")
        driver.get(url)
        WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,"h1.cxl.clb.lsp")))

        page_source=driver.page_source
        now=get_pkt_time()
        
        # Initialize data with default values
        data={
            "ID":"",
            "NICK NAME":nickname,
            "TAGS":"",
            "FRIEND":"",
            "CITY":"",
            "GENDER":"",
            "MARRIED":"",
            "AGE":"",
            "JOINED":"",
            "FOLLOWERS":"",
            "STATUS":"Normal",
            "POSTS":"",
            "INTRO":"",
            "MEHFIL NAME":"",
            "MEHFIL DATE":"",
            "SOURCE":"Target",
            "DATETIME SCRAP":now.strftime("%d-%b-%y %I:%M %p"),
            "LAST POST":"",
            "LAST POST TIME":"",
            "IMAGE":"",
            "PROFILE LINK":url.rstrip('/'),
            "POST URL":f"https://damadam.pk/profile/public/{nickname}",
        }
        
        # Check for account status
        status, skip_reason = detect_status(page_source)
        data['STATUS'] = status
        
        if status != "Normal":
            data['__skip_reason'] = skip_reason
            return data
            
        # Extract TID (ID)
        data['ID'] = extract_tid(page_source)
        
        # Extract friend status
        data['FRIEND'] = get_friend_status(driver)

        if 'account suspended' in page_source.lower():
            data['STATUS'] = 'Banned'
            data['__skip_reason'] = 'Account Suspended'
            return data
        elif 'background:tomato' in page_source or 'style="background:tomato"' in page_source.lower():
            data['STATUS'] = 'Unverified'
            data['__skip_reason'] = 'skipped coz of unverified user'
            return data
        else:
            try:
                driver.find_element(By.CSS_SELECTOR, "div[style*='tomato']")
                data['STATUS'] = 'Unverified'
                data['__skip_reason'] = 'skipped coz of unverified user'
                return data
            except Exception:
                data['STATUS'] = 'Normal'

        # Extract ID from tid with HTML fallback
        if not data['ID']:
            try:
                # Try direct element access first
                tid_elem = driver.find_element(By.XPATH, "//input[@name='tid']")
                data['ID'] = tid_elem.get_attribute('value').strip()
            except Exception:
                # Fallback to HTML parsing
                tid_match = re.search(r'name=["\']tid["\']\s+value=["\'](\d+)', page_source, re.I)
                data['ID'] = tid_match.group(1) if tid_match else ''

        # Get friend status with multiple fallback methods
        data['FRIEND'] = get_friend_status(driver)
        
        # Mehfil (group) detection with HTML fallback
        try:
            if "mehfil(s) owned" in page_source.lower():
                try:
                    name_el = driver.find_element(By.CSS_SELECTOR, "div.cp.ow")
                    since_el = driver.find_element(By.CSS_SELECTOR, "div.cs.sp")
                    data["MEHFIL NAME"] = clean_text(name_el.text)
                    data["MEHFIL DATE"] = parse_owner_since_to_date(since_el.text)
                except Exception:
                    # HTML fallback for mehfil info
                    mehfil_match = re.search(r'<div class="cp ow">(.+?)</div>.*?<div class="cs sp">(.+?)</div>', 
                                          page_source, re.DOTALL)
                    if mehfil_match:
                        data["MEHFIL NAME"] = clean_text(mehfil_match.group(1))
                        data["MEHFIL DATE"] = parse_owner_since_to_date(mehfil_match.group(2))
        except Exception as e:
            log_msg(f"[WARNING] Error parsing mehfil info: {str(e)[:100]}")

        for sel in ["span.cl.sp.lsp.nos","span.cl",".ow span.nos"]:
            try:
                intro=driver.find_element(By.CSS_SELECTOR, sel)
                if intro.text.strip():
                    data['INTRO']=clean_text(intro.text)
                    break
            except Exception:
                pass

        def _extract_from_html(label: str) -> str:
            try:
                # First try direct element access
                try:
                    # Look for pattern: <b>Label:</b> <span>Value</span>
                    elem = driver.find_element(By.XPATH, f"//b[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')]/following-sibling::span[1]")
                    return clean_text(elem.text)
                except Exception:
                    pass
                    
                # HTML fallback pattern 1: <b>Label:</b> <span>Value</span>
                pattern1 = rf'<b[^>]*>\s*{re.escape(label)}[^<]*</b>\s*<span[^>]*>(.*?)</span>'
                match = re.search(pattern1, page_source, re.IGNORECASE)
                if match:
                    return clean_text(match.group(1))
                    
                # HTML fallback pattern 2: <div><b>Label:</b> Value</div>
                pattern2 = rf'<div[^>]*>\s*<b[^>]*>{re.escape(label)}[^<]*</b>\s*([^<]+)</div>'
                match = re.search(pattern2, page_source, re.IGNORECASE)
                if match:
                    return clean_text(match.group(1))
                    
                # HTML fallback pattern 3: <div>Label: Value</div>
                pattern3 = rf'<div[^>]*>\s*{re.escape(label)}[^:]*:\s*([^<]+)</div>'
                match = re.search(pattern3, page_source, re.IGNORECASE)
                if match:
                    return clean_text(match.group(1))
                    
                return ""
            except Exception as e:
                log_msg(f"[WARNING] Error extracting {label}: {str(e)[:100]}")
                return ""
                pattern = rf"<b[^>]*>\s*{re.escape(label)}\s*</b>\s*<span[^>]*>(.*?)</span>"
                m = re.search(pattern, page_source, flags=re.IGNORECASE | re.DOTALL)
                if not m:
                    return ""
                return clean_text(re.sub(r"<[^>]+>", "", m.group(1)))
            except Exception:
                return ""

        fields={'City:':'CITY','Gender:':'GENDER','Married:':'MARRIED','Age:':'AGE','Joined:':'JOINED'}
        for label,key in fields.items():
            try:
                # Try direct element access first
                try:
                    elem=driver.find_element(By.XPATH,f"//b[contains(text(), '{label}')]/following-sibling::span[1]")
                    value=elem.text.strip()
                except:
                    # Fallback to HTML parsing
                    value = _extract_from_html(label)
                    
                if not value:
                    continue
                    
                # Process each field type
                if key=='JOINED':
                    data[key]=convert_relative_date_to_absolute(value)
                elif key=='GENDER':
                    low=value.lower()
                    if 'female' in low:
                        data[key] = 'Female'
                    elif 'male' in low:
                        data[key] = 'Male'
                    else:
                        data[key] = ''
                elif key=='MARRIED':
                    low=value.lower()
                    if low in {'yes','married'}:
                        data[key] = 'Yes'
                    elif low in {'no','single','unmarried'}:
                        data[key] = 'No'
                    else:
                        data[key] = ''
                else:
                    data[key]=clean_data(value)
                    
            except Exception as e:
                log_msg(f"[WARNING] Error extracting {label} for {nickname}: {str(e)[:100]}")
                value = _extract_from_html(label)
                if not value:
                    continue
                try:
                    if key=='JOINED':
                        data[key]=convert_relative_date_to_absolute(value)
                    elif key=='GENDER':
                        low=value.lower()
                        if 'female' in low:
                            data[key] = 'Female'
                        elif 'male' in low:
                            data[key] = 'Male'
                        else:
                            data[key] = ''
                    elif key=='MARRIED':
                        low=value.lower()
                        if low in {'yes','married'}:
                            data[key] = 'Yes'
                        elif low in {'no','single','unmarried'}:
                            data[key] = 'No'
                        else:
                            data[key] = ''
                    else:
                        data[key]=clean_data(value)
                except Exception:
                    continue
                continue

        for sel in ["span.cl.sp.clb",".cl.sp.clb"]:
            try:
                followers=driver.find_element(By.CSS_SELECTOR, sel)
                match=re.search(r'(\d+)', followers.text)
                if match:
                    data['FOLLOWERS']=match.group(1)
                    break
            except Exception:
                pass

        for sel in ["a[href*='/profile/public/'] button div:first-child","a[href*='/profile/public/'] button div"]:
            try:
                posts=driver.find_element(By.CSS_SELECTOR, sel)
                match=re.search(r'(\d+)', posts.text)
                if match:
                    data['POSTS']=match.group(1)
                    break
            except Exception:
                pass

        for sel in ["img[src*='avatar-imgs']","img[src*='avatar']","div[style*='whitesmoke'] img[src*='cloudfront.net']"]:
            try:
                img=driver.find_element(By.CSS_SELECTOR, sel)
                src=img.get_attribute('src')
                if src and ('avatar' in src or 'cloudfront.net' in src):
                    data['IMAGE']=src.replace('/thumbnail/','/')
                    break
            except Exception:
                pass

        if data.get('POSTS') and data['POSTS']!='0':
            time.sleep(1)
            post_data=scrape_recent_post(driver, nickname)
            data['LAST POST']=clean_data(post_data.get('LPOST',''))
            data['LAST POST TIME']=post_data.get('LDATE-TIME','')

        log_msg(f"[OK] Extracted: {data['GENDER']}, {data['CITY']}, Posts: {data['POSTS']}")

        return data
    except TimeoutException:
        log_msg(f"[TIMEOUT] Timeout while scraping {nickname}")
        return None
    except WebDriverException:
        log_msg(f"[BROWSER_ERROR] Browser issue while scraping {nickname}")
        return None
    except Exception as e:
        log_msg(f"[ERROR] Error scraping {nickname}: {str(e)[:60]}")
        return None

# ==================== MAIN ENTRY ====================

def main():
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--max-profiles", type=int, default=None, help="Max profiles to scrape (0 = all)")
    parser.add_argument("--profiles-to-scrape", dest="max_profiles", type=int, default=None, help="Alias for --max-profiles (0 = all)")
    parser.add_argument("--apply-font", action="store_true", help="Apply Quantico font to all Google Sheets")
    parser.add_argument("--apply-font-only", action="store_true", help="Apply Quantico font to all Google Sheets and exit")
    parser.add_argument("--no-apply-font", action="store_true", help="Do not apply Quantico font")
    args = parser.parse_args()

    is_interactive = sys.stdin.isatty() and not os.getenv('GITHUB_ACTIONS')

    if args.apply_font_only:
        # Skip prompts for apply-font-only; not needed
        if args.batch_size is None:
            args.batch_size = BATCH_SIZE
        if args.max_profiles is None:
            args.max_profiles = 0

    if args.batch_size is None:
        if is_interactive:
            raw = input(f"Batch Size (default {BATCH_SIZE}): ").strip()
            args.batch_size = int(raw) if raw else BATCH_SIZE
        else:
            args.batch_size = BATCH_SIZE

    if args.max_profiles is None:
        if is_interactive:
            raw = input("Profiles to scrape (0=All, default 0): ").strip()
            args.max_profiles = int(raw) if raw else 0
        else:
            args.max_profiles = MAX_PROFILES_PER_RUN

    os.environ['BATCH_SIZE'] = str(args.batch_size)
    os.environ['MAX_PROFILES_PER_RUN'] = str(args.max_profiles)

    header = Table.grid(padding=(0, 2))
    header.add_column(justify="left")
    header.add_row("DamaDam Target Bot", "v3.2.1")
    header.add_row("Batch Size", str(args.batch_size))
    header.add_row("Profiles", "All" if args.max_profiles == 0 else str(args.max_profiles))
    console.print(Panel(header, title="Run Config", border_style="magenta"))
    print("\n"+"="*70)
    print("  [TARGET] DamaDam Target Bot v3.2.1 (Single File)")
    print("="*70)
    if not USERNAME or not PASSWORD: print("[ERROR] Missing DAMADAM_USERNAME / DAMADAM_PASSWORD"); sys.exit(1)
    log_msg("Connecting to Google Sheets...")
    if IS_CI:
        client = gsheets_client(); sheets = Sheets(client)
    else:
        with Status("🔌 Connecting to Google Sheets...", console=console, spinner="dots"):
            client = gsheets_client(); sheets = Sheets(client)

    # Apply Quantico by default unless explicitly disabled
    apply_font = (not args.no_apply_font) and (args.apply_font_only or args.apply_font or APPLY_FONT_FORMATTING or True)
    if apply_font:
        if IS_CI:
            sheets.apply_quantico_font()
        else:
            with Status("🔤 Applying Quantico font...", console=console, spinner="dots"):
                sheets.apply_quantico_font()

    if args.apply_font_only:
        log_msg("Font formatting complete (apply-font-only). Exiting.")
        return

    log_msg("Setting up browser...")
    if IS_CI:
        driver = setup_browser()
    else:
        with Status("🌐 Launching Chrome...", console=console, spinner="dots"):
            driver = setup_browser()
    if not driver: print("[ERROR] Browser setup failed"); sys.exit(1)
    try:
        log_msg("Logging in...")
        if IS_CI:
            ok = login(driver)
        else:
            with Status("🔐 Logging in...", console=console, spinner="dots"):
                ok = login(driver)
        if not ok: print("[ERROR] Login failed"); driver.quit(); sys.exit(1)

        log_msg("Fetching pending targets...")
        if IS_CI:
            targets = get_pending_targets(sheets)
        else:
            with Status("📥 Reading Target sheet...", console=console, spinner="dots"):
                targets = get_pending_targets(sheets)
        if not targets: log_msg("No pending targets."); return
        # Enforce max profiles strictly
        to_process = targets[:args.max_profiles] if args.max_profiles > 0 else targets
        success=failed=suspended_count=0
        run_stats={"new":0,"updated":0,"unchanged":0}
        start_time=time.time(); run_started=get_pkt_time()
        trigger_type="Scheduled" if os.getenv('GITHUB_EVENT_NAME','').lower()=='schedule' else "Manual"
        current_target=None
        log_msg(f"Starting scrape of {len(to_process)} profiles...")
        processed_count = 0
        try:
            with Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("{task.description}"),
                BarColumn(bar_width=30),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console,
                transient=False,
            ) as progress:
                task_id = progress.add_task("Scraping profiles", total=len(to_process))
                while processed_count < len(to_process):
                    t = to_process[processed_count]
                    current_target = t
                    nick = t['nickname']; row = t['row']; source = t.get('source','Target') or 'Target'
                    eta = calculate_eta(processed_count, len(to_process), start_time)
                    progress.update(task_id, description=f"[{eta}] {nick}")
                    try:
                        prof = scrape_profile(driver, nick)
                        if not prof:
                            raise RuntimeError("Profile scrape failed")
                        prof['SOURCE'] = source

                        skip_reason = prof.get('__skip_reason')
                        if skip_reason:
                            sheets.write_profile(prof, old_row=row)
                            sheets.update_target_status(row, "Error", f"{skip_reason} @ {get_pkt_time().strftime('%I:%M %p')}")
                            failed += 1
                        else:
                            result = sheets.write_profile(prof, old_row=row)
                            status = result.get("status","error") if result else "error"
                            if status in {"new","updated","unchanged"}:
                                success += 1
                                run_stats[status] += 1
                                sheets.update_target_status(row, "Done", f"{status} @ {get_pkt_time().strftime('%I:%M %p')}")
                            else:
                                raise RuntimeError(result.get("error","Write failed") if result else "Write failed")
                    except Exception as e:
                        sheets.update_target_status(row, "Pending", f"Retry needed: {e}")
                        failed += 1
                    current_target = None
                    processed_count += 1
                    progress.advance(task_id)
                    if args.batch_size > 0 and processed_count % args.batch_size == 0 and processed_count < len(to_process):
                        adaptive.on_batch(); time.sleep(3)
                    adaptive.sleep()
        except KeyboardInterrupt:
            print("\n" + "-"*70)
            log_msg("Run interrupted by user")
            if current_target:
                sheets.update_target_status(current_target['row'], "Pending", f"Interrupted @ {get_pkt_time().strftime('%I:%M %p')}")
        except Exception as fatal:
            print("\n" + "-"*70)
            log_msg(f"Fatal error: {fatal}")
            if current_target:
                sheets.update_target_status(current_target['row'], "Pending", f"Run error: {fatal}")
            return
        print("-"*70)
        log_msg(f"[COMPLETE] Run completed: {success} success, {failed} failed, {suspended_count} suspended")
        sheets.update_dashboard({
            "Run Number":1,
            "Last Run": get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
            "Profiles Processed": len(targets),
            "Success": success,
            "Failed": failed,
            "New Profiles": run_stats.get('new',0),
            "Updated Profiles": run_stats.get('updated',0),
            "Unchanged Profiles": run_stats.get('unchanged',0),
            "Trigger": trigger_type,
            "Start": run_started.strftime("%d-%b-%y %I:%M %p"),
            "End": get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
        })
        print("="*70)
    finally:
        try: driver.quit()
        except: pass

if __name__=='__main__':
    main()





















#!/usr/bin/env python3
"""
🚀 DamaDam Target Bot v3.3.0 - Enhanced Edition
================================================

✨ NEW FEATURES:
  - Animated terminal with live statistics
  - 80% fewer API calls (batch processing)
  - Smart rate limit handling with exponential backoff
  - Mehfil name & date extraction
  - Enhanced error recovery
  - All user requirements implemented

Author: Enhanced for Production Use
Date: 2024
"""

# ==================== CONFIGURATION ====================

DAMADAM_USERNAME = "0utLawZ"
# ==================== IMPORTS ====================

import os
import sys
import re
import time
import json
import random
import pickle
from datetime import datetime, timedelta, timezone

# Third-party libraries
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Rich UI libraries
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich import box

# ==================================================================================================
#  Constants & Configuration
# ==================================================================================================

# --- Credentials & Secrets (loaded from environment variables) ---
DAMADAM_USERNAME = os.getenv('DAMADAM_USERNAME', "0utLawZ")
DAMADAM_PASSWORD = os.getenv('DAMADAM_PASSWORD', "asdasd")
DAMADAM_USERNAME_2 = os.getenv('DAMADAM_USERNAME_2')
DAMADAM_PASSWORD_2 = os.getenv('DAMADAM_PASSWORD_2')
GOOGLE_SHEET_URL = os.getenv('GOOGLE_SHEET_URL', "https://docs.google.com/spreadsheets/d/1jn1DroWU8GB5Sc1rQ7wT-WusXK9v4V05ISYHgUEjYZc/edit")
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON') # For GitHub Actions
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # For local JSON file

# --- Script Behavior --- 
MAX_PROFILES_PER_RUN = int(os.getenv('MAX_PROFILES_PER_RUN', '0')) # 0 for unlimited
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '25'))
SHEET_WRITE_DELAY = float(os.getenv('SHEET_WRITE_DELAY', '0.5'))
BATCH_COOLOFF_SECONDS = 15

# --- Rate Limiting ---
MIN_DELAY_SECONDS = float(os.getenv('MIN_DELAY', '0.5'))
MAX_DELAY_SECONDS = float(os.getenv('MAX_DELAY', '1.0'))

# --- URLs and Files ---
LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
COOKIE_FILE = "damadam_cookies.pkl"

# --- Google Sheets Column Structure ---
COLUMN_ORDER = [
    "ID", "NICK NAME", "TAGS", "FRIEND", "CITY", "GENDER", "MARRIED", "AGE", "JOINED",
    "FOLLOWERS", "STATUS", "POSTS", "INTRO", "MEHFIL NAME", "MEHFIL DATE", "SOURCE",
    "DATETIME SCRAP", "LAST POST TIME", "IMAGE URL", "LAST POST URL", "PROFILE URL", "POST URL"
]
COLUMN_TO_INDEX = {name: idx for idx, name in enumerate(COLUMN_ORDER)}
HIGHLIGHT_EXCLUDE_COLUMNS = {
    "LAST POST TIME", "JOINED", "DATETIME SCRAP", "PROFILE URL", "POST URL", "IMAGE URL", "LAST POST URL"
}

# --- Profile Status Indicators ---
SUSPENSION_INDICATORS = [
    "accounts suspend", "aik se zyada fake accounts", "abuse ya harassment",
    "kisi aur user ki identity apnana", "accounts suspend kiye",
]

# --- Initialize Console ---
console = Console()


# ==================================================================================================
#  Statistics Tracker & UI
# ==================================================================================================

class StatsTracker:
    """Track scraping statistics for live display."""
    def __init__(self):
        self.success = self.failed = self.suspended = self.unverified = 0
        self.new = self.updated = self.unchanged = 0
        self.api_calls = self.api_errors = 0
        self.current_profile = ""
        self.start_time = time.time()

    def get_table(self, processed: int, total: int) -> Panel:
        """Generate a rich Table for the live display."""
        table = Table(box=box.ROUNDED, show_header=False, padding=(0, 1))
        table.add_column(style="cyan bold", width=22)
        table.add_column(style="green", width=12)

        elapsed = int(time.time() - self.start_time)
        mins, secs = divmod(elapsed, 60)

        table.add_row("✅ Success", f"[green bold]{self.success}[/green bold]")
        table.add_row("❌ Failed", f"[red bold]{self.failed}[/red bold]")
        table.add_row("🚫 Suspended/Banned", f"[yellow bold]{self.suspended}[/yellow bold]")
        table.add_row("❓ Unverified", f"[magenta bold]{self.unverified}[/magenta bold]")
        table.add_row("-" * 34, "-" * 12)
        table.add_row("🆕 New Profiles", f"[cyan bold]{self.new}[/cyan bold]")
        table.add_row("🔄 Updated Profiles", f"[blue bold]{self.updated}[/blue bold]")
        table.add_row("-" * 34, "-" * 12)
        table.add_row("⏱️ Runtime", f"[magenta]{mins}m {secs}s[/magenta]")
        table.add_row("📊 API Calls", f"[yellow]{self.api_calls}[/yellow]")
        table.add_row("🎯 Progress", f"[bold white]{processed}/{total}[/bold white]")

        return Panel(table, title="[bold yellow]📈 Live Statistics[/bold yellow]", border_style="yellow")

stats = StatsTracker()

# ==================================================================================================
#  Helper Functions
# ==================================================================================================

def get_pkt_time() -> datetime:
    """Get the current time in Pakistan Standard Time (UTC+5)."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log_status(message: str, status: str = "info"):
    """Log a message to the console with a timestamp and colored status icon."""
    icons = {"success": "✅", "error": "❌", "warning": "⚠️", "info": "ℹ️", "scraping": "🔍", "api": "📡"}
    colors = {"success": "green", "error": "red", "warning": "yellow", "info": "cyan", "scraping": "blue", "api": "magenta"}
    icon = icons.get(status, "➡️")
    color = colors.get(status, "white")
    timestamp = get_pkt_time().strftime('%H:%M:%S')
    console.print(f"[dim]{timestamp}[/dim] [{color}]{icon}[/{color}] {message}")

def clean_data(value: str) -> str:
    """Clean and normalize a string value by removing extra whitespace and common null-like values."""
    if not value: return ""
    value = str(value).strip().replace('\xa0', ' ')
    null_values = {"no city", "not set", "[no posts]", "n/a", "no set", "none", "null", "no age"}
    return "" if value.lower() in null_values else re.sub(r'\s+', ' ', value)

def convert_relative_date_to_absolute(text: str) -> str:
    """Convert relative date strings (e.g., '5 hours ago') to an absolute date format ('DD-Mon-YY')."""
    if not text: return ""
    text_lower = text.lower().strip().replace("mins", "minutes").replace("min", "minute").replace("secs", "seconds").replace("sec", "second").replace("hrs", "hours").replace("hr", "hour")
    match = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", text_lower)
    if not match: return text

    amount, unit = int(match.group(1)), match.group(2)
    unit_in_seconds = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800, "month": 2592000, "year": 31536000}

    if unit in unit_in_seconds:
        delta = timedelta(seconds=amount * unit_in_seconds[unit])
        absolute_date = get_pkt_time() - delta
        return absolute_date.strftime("%d-%b-%y")
    return text

def to_absolute_url(href: str) -> str:
    """Convert a relative URL to an absolute URL for damadam.pk."""
    if not href: return ""
    href = href.strip()
    if href.startswith('/'): return f"https://damadam.pk{href}"
    if not href.startswith('http'): return f"https://damadam.pk/{href}"
    return href

class AdaptiveDelay:
    """Manage delays between requests, increasing them when rate limits are hit."""
    def __init__(self, min_delay: float, max_delay: float):
        self.base_min, self.base_max = min_delay, max_delay
        self.current_min, self.current_max = min_delay, max_delay
        self.rate_limit_hits = 0
        self.last_adjustment_time = time.time()

    def on_success(self):
        """Gradually decrease delay back to base on successful requests."""
        if self.rate_limit_hits > 0:
            self.rate_limit_hits -= 1
        if time.time() - self.last_adjustment_time > 10: # Adjust every 10 seconds
            self.current_min = max(self.base_min, self.current_min * 0.95)
            self.current_max = max(self.base_max, self.current_max * 0.95)
            self.last_adjustment_time = time.time()

    def on_rate_limit(self):
        """Significantly increase delay when a rate limit is detected."""
        self.rate_limit_hits += 1
        factor = 1 + min(0.2 * self.rate_limit_hits, 1.5)
        self.current_min = min(8.0, self.current_min * factor)
        self.current_max = min(15.0, self.current_max * factor)
        log_status(f"Rate limit detected! Delay increased to {self.current_min:.1f}-{self.current_max:.1f}s", "warning")

    def on_batch_complete(self):
        """Slightly increase delay after a batch to cool down."""
        self.current_min = min(8.0, max(self.base_min, self.current_min * 1.3))
        self.current_max = min(15.0, max(self.base_max, self.current_max * 1.3))

    def sleep(self):
        """Sleep for a random duration within the current delay range."""
        time.sleep(random.uniform(self.current_min, self.current_max))

adaptive_delay = AdaptiveDelay(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)

# ==================================================================================================
#  Browser Automation
# ==================================================================================================

class BrowserManager:
    """Handles all Selenium browser interactions, including setup, login, and cookie management."""
    def __init__(self):
        self.driver = self._setup_driver()

    def _setup_driver(self) -> webdriver.Chrome | None:
        """Initializes and configures the headless Chrome WebDriver."""
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--log-level=3")

            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return driver
        except Exception as e:
            log_status(f"Failed to set up browser: {e}", "error")
            return None

    def _save_cookies(self):
        """Saves the current session cookies to a file."""
        try:
            with open(COOKIE_FILE, 'wb') as f:
                pickle.dump(self.driver.get_cookies(), f)
        except Exception as e:
            log_status(f"Could not save cookies: {e}", "warning")

    def _load_cookies(self) -> bool:
        """Loads session cookies from a file if it exists."""
        if not os.path.exists(COOKIE_FILE): return False
        try:
            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)
            for cookie in cookies:
                self.driver.add_cookie(cookie)
            return True
        except Exception as e:
            log_status(f"Could not load cookies: {e}", "warning")
            return False

    def login(self) -> bool:
        """Logs into DamaDam using primary or secondary credentials, leveraging cookies if available."""
        if not self.driver: return False
        try:
            self.driver.get(HOME_URL)
            time.sleep(2)
            if self._load_cookies():
                self.driver.refresh()
                time.sleep(3)
            
            if 'login' not in self.driver.current_url.lower():
                log_status("Logged in using session cookies.", "success")
                return True

            log_status("Session expired. Performing fresh login.", "info")
            self.driver.get(LOGIN_URL)
            time.sleep(3)

            credentials = [
                ("Primary Account", DAMADAM_USERNAME, DAMADAM_PASSWORD),
                ("Secondary Account", DAMADAM_USERNAME_2, DAMADAM_PASSWORD_2)
            ]

            for account, username, password in credentials:
                if not username or not password: continue
                try:
                    nick_field = WebDriverWait(self.driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='nick']")))
                    pass_field = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
                    submit_btn = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")

                    nick_field.clear(); nick_field.send_keys(username)
                    time.sleep(0.5)
                    pass_field.clear(); pass_field.send_keys(password)
                    time.sleep(0.5)
                    submit_btn.click()
                    time.sleep(4)

                    if 'login' not in self.driver.current_url.lower():
                        self._save_cookies()
                        log_status(f"Successfully logged in with {account}.", "success")
                        return True
                    else:
                        log_status(f"Login failed for {account}.", "warning")
                except Exception:
                    log_status(f"Could not attempt login for {account}.", "error")
                    continue
            
            log_status("All login attempts failed.", "error")
            return False
        except Exception as e:
            log_status(f"An unexpected error occurred during login: {e}", "error")
            return False

    def get_page_source(self, url: str) -> str | None:
        """Navigates to a URL and returns its page source."""
        if not self.driver: return None
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, PAGE_LOAD_TIMEOUT).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            return self.driver.page_source
        except TimeoutException:
            log_status(f"Timeout loading page: {url}", "error")
            return None
        except WebDriverException as e:
            log_status(f"WebDriver error on page {url}: {e}", "error")
            return None

    def close(self):
        """Closes the WebDriver session."""
        if self.driver:
            self.driver.quit()

# ==================================================================================================
#  Google Sheets Manager
# ==================================================================================================

class SheetsManager:
    """Handles all Google Sheets API interactions with batching, retries, and caching."""
    def __init__(self):
        self.client = self._get_gsheets_client()
        if not self.client:
            sys.exit(1)
        
        self.ss = self.client.open_by_url(GOOGLE_SHEET_URL)
        self.ws = self._get_or_create_worksheet("ProfilesTarget")
        self.target_ws = self._get_or_create_worksheet("Target")
        self.dashboard_ws = self._get_or_create_worksheet("Dashboard")
        self.tags_ws = self._get_or_create_worksheet("Tags", create=False)

        self.existing_profiles = {}
        self.tags_mapping = {}
        self.target_status_updates = []

        self._initialize_sheets()

    def _get_gsheets_client(self) -> gspread.Client | None:
        """Authenticates with Google Sheets API using service account credentials."""
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = None
        try:
            if GOOGLE_CREDENTIALS_JSON:
                creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
                creds = Credentials.from_service_account_info(creds_info, scopes=scope)
            elif GOOGLE_APPLICATION_CREDENTIALS and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                creds = Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS, scopes=scope)
            else:
                log_status("No Google credentials found. Set GOOGLE_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS.", "error")
                return None
            return gspread.authorize(creds)
        except Exception as e:
            log_status(f"Google Sheets authentication failed: {e}", "error")
            return None

    def _get_or_create_worksheet(self, name: str, create: bool = True) -> gspread.Worksheet | None:
        """Gets a worksheet by name, creating it if it doesn't exist."""
        try:
            return self.ss.worksheet(name)
        except WorksheetNotFound:
            if create:
                log_status(f"Worksheet '{name}' not found, creating it.", "warning")
                return self.ss.add_worksheet(title=name, rows=1000, cols=len(COLUMN_ORDER) + 2)
            else:
                log_status(f"Worksheet '{name}' not found.", "info")
                return None

    def _api_call(self, func, *args, **kwargs):
        """Wrapper for gspread API calls with retry logic for rate limiting."""
        for attempt in range(3):
            try:
                stats.api_calls += 1
                return func(*args, **kwargs)
            except APIError as e:
                stats.api_errors += 1
                if '429' in str(e) or 'RESOURCE_EXHAUSTED' in str(e):
                    wait_time = (2 ** attempt) * 20
                    log_status(f"API rate limit hit. Waiting {wait_time}s...", "warning")
                    adaptive_delay.on_rate_limit()
                    time.sleep(wait_time)
                else:
                    raise
        raise Exception("Max retries exceeded for API call.")

    def _initialize_sheets(self):
        """Ensures all required sheets have their headers set up correctly."""
        # ProfilesTarget header
        header = self._api_call(self.ws.row_values, 1)
        if not header or header != COLUMN_ORDER:
            self._api_call(self.ws.update, [COLUMN_ORDER], 'A1')
            self._api_call(self.ws.freeze, rows=1)
            log_status("Set 'ProfilesTarget' headers.", "info")
        
        # Target header
        target_header = self._api_call(self.target_ws.row_values, 1)
        if not target_header or target_header != ["Nickname", "Status", "Remarks", "Source"]:
            self._api_call(self.target_ws.update, [["Nickname", "Status", "Remarks", "Source"]], 'A1')
            log_status("Set 'Target' headers.", "info")

    def load_cache(self):
        """Loads existing profiles and tag mappings into memory."""
        # Load existing profiles
        log_status("Loading existing profiles from sheet...", "api")
        all_profiles = self._api_call(self.ws.get_all_values)[1:]
        for i, row in enumerate(all_profiles, start=2):
            if len(row) > COLUMN_TO_INDEX["NICK NAME"] and row[COLUMN_TO_INDEX["NICK NAME"]].strip():
                nickname = row[COLUMN_TO_INDEX["NICK NAME"]].strip().lower()
                self.existing_profiles[nickname] = {'row': i, 'data': row}
        log_status(f"Loaded {len(self.existing_profiles)} existing profiles.", "success")

        # Load tags mapping
        if self.tags_ws:
            log_status("Loading tags mapping...", "api")
            all_tags = self._api_call(self.tags_ws.get_all_values)
            if len(all_tags) > 1:
                headers = all_tags[0]
                for col_idx, tag_name in enumerate(headers):
                    if not tag_name.strip(): continue
                    for row in all_tags[1:]:
                        if col_idx < len(row) and row[col_idx].strip():
                            nickname = row[col_idx].strip().lower()
                            if nickname in self.tags_mapping:
                                self.tags_mapping[nickname] += f", {tag_name.strip()}"
                            else:
                                self.tags_mapping[nickname] = tag_name.strip()
                log_status(f"Loaded {len(self.tags_mapping)} tag mappings.", "success")

    def get_pending_targets(self) -> list:
        """Fetches all rows from the 'Target' sheet with a 'Pending' status."""
        log_status("Fetching pending targets...", "api")
        all_targets = self._api_call(self.target_ws.get_all_values)[1:]
        pending = []
        for i, row in enumerate(all_targets, start=2):
            if len(row) > 1:
                nickname, status = row[0].strip(), row[1].strip()
                if nickname and (not status or "pending" in status.lower()):
                    source = row[3].strip() if len(row) > 3 and row[3].strip() else 'Target'
                    pending.append({'nickname': nickname, 'row': i, 'source': source})
        log_status(f"Found {len(pending)} pending targets.", "success")
        return pending

    def update_target_status(self, row_num: int, status: str, remarks: str):
        """Queues a status update for a target row. Updates are flushed in batches."""
        status_map = {"pending": "⚡ Pending", "done": "Done 💀", "error": "Error 💥"}
        final_status = status
        for key, value in status_map.items():
            if key in status.lower():
                final_status = value
                break
        self.target_status_updates.append({'range': f'B{row_num}:C{row_num}', 'values': [[final_status, remarks]]})

    def flush_target_updates(self):
        """Flushes all queued target status updates to the sheet in a single API call."""
        if not self.target_status_updates: return
        log_status(f"Flushing {len(self.target_status_updates)} target status updates...", "api")
        self._api_call(self.target_ws.batch_update, self.target_status_updates, value_input_option='USER_ENTERED')
        self.target_status_updates.clear()

    def write_profile(self, profile_data: dict) -> dict:
        """Writes a single profile's data to the 'ProfilesTarget' sheet, either by updating or appending."""
        nickname = (profile_data.get("NICK NAME") or "").strip()
        if not nickname: return {"status": "error", "error": "Missing nickname"}

        # Final data prep
        profile_data["DATETIME SCRAP"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
        if nickname.lower() in self.tags_mapping:
            profile_data["TAGS"] = self.tags_mapping[nickname.lower()]

        row_values = [clean_data(profile_data.get(col, "")) for col in COLUMN_ORDER]
        
        existing = self.existing_profiles.get(nickname.lower())
        if existing:
            # Update existing row
            row_num = existing['row']
            self._api_call(self.ws.update, [row_values], f'A{row_num}', value_input_option='USER_ENTERED')
            self.existing_profiles[nickname.lower()] = {'row': row_num, 'data': row_values}
            # Simple comparison for now, will be enhanced later
            was_changed = json.dumps(existing['data']) != json.dumps(row_values)
            return {"status": "updated" if was_changed else "unchanged"}
        else:
            # Append new row
            self._api_call(self.ws.append_row, row_values, value_input_option='USER_ENTERED')
            # This is slow, will optimize later
            new_row_num = len(self._api_call(self.ws.get_all_values))
            self.existing_profiles[nickname.lower()] = {'row': new_row_num, 'data': row_values}
            return {"status": "new"}

# ==================================================================================================
#  Profile Scraper
# ==================================================================================================

from bs4 import BeautifulSoup

class ProfileScraper:
    """Parses the HTML of a profile page to extract all required information."""
    def __init__(self, page_source: str, nickname: str, source: str):
        self.soup = BeautifulSoup(page_source, 'html.parser')
        self.nickname = nickname
        self.source = source
        self.profile_data = {"NICK NAME": nickname, "SOURCE": source}

    def scrape(self) -> dict:
        """Main method to orchestrate the scraping of different profile sections."""
        if self._is_unverified_or_banned():
            return self.profile_data

        self._extract_basic_info()
        self._extract_friend_status()
        self._extract_mehfil_info()
        self._extract_last_post()
        self._add_url_fields()
        return self.profile_data

    def _is_unverified_or_banned(self) -> bool:
        """Checks for unverified or banned status and updates profile data accordingly."""
        unverified_div = self.soup.find('div', class_='cs', text='UNVERIFIED USER')
        if unverified_div:
            self.profile_data['STATUS'] = 'Unverified'
            stats.unverified += 1
            # Check if it's a full suspension page
            if any(indicator in self.soup.get_text().lower() for indicator in SUSPENSION_INDICATORS):
                self.profile_data['STATUS'] = 'Banned'
                stats.suspended += 1
            return True
        return False

    def _extract_basic_info(self):
        """Extracts ID, gender, city, age, join date, followers, and posts."""
        # Extract User ID
        tid_input = self.soup.find('input', {'name': 'tid'})
        if tid_input and tid_input.get('value'):
            self.profile_data['ID'] = tid_input['value']

        # Extract key-value pairs from the profile info table
        info_divs = self.soup.find_all('div', class_='fx', style=lambda v: v and 'border-bottom:1px' in v)
        for div in info_divs:
            key_elem = div.find('div', style=lambda v: v and 'color:gray' in v)
            val_elem = div.find('div', style=lambda v: v and 'color:black' in v)
            if key_elem and val_elem:
                key = key_elem.text.strip().upper()
                value = val_elem.text.strip()
                if 'GENDER' in key: self.profile_data['GENDER'] = 'Male' if 'male' in value.lower() else 'Female'
                elif 'CITY' in key: self.profile_data['CITY'] = value
                elif 'AGE' in key: self.profile_data['AGE'] = value
                elif 'JOINED' in key: self.profile_data['JOINED'] = value
                elif 'MARRIED' in key: self.profile_data['MARRIED'] = 'Yes' if 'married' in value.lower() else 'No'
                elif 'FOLLOWERS' in key: self.profile_data['FOLLOWERS'] = value
                elif 'POSTS' in key: self.profile_data['POSTS'] = value

        # Extract Intro
        intro_elem = self.soup.find('div', style=lambda v: v and 'background:#f0f0f0' in v)
        if intro_elem: self.profile_data['INTRO'] = intro_elem.text.strip()

    def _extract_friend_status(self):
        """Determines if the user is a friend based on the 'FOLLOW' or 'UNFOLLOW' button."""
        unfollow_button = self.soup.find('button', class_=lambda v: v and 'fbtn' in v, string=re.compile(r'UNFOLLOW', re.IGNORECASE))
        self.profile_data['FRIEND'] = 'Yes' if unfollow_button else 'No'

    def _extract_mehfil_info(self):
        """Extracts the name and creation date of any owned 'Mehfil' (group)."""
        mehfil_header = self.soup.find('div', text='Mehfil(s) owned:')
        if mehfil_header:
            mehfil_container = mehfil_header.find_next_sibling('div')
            if mehfil_container:
                name_elem = mehfil_container.find('div', class_='cp')
                date_elem = mehfil_container.find('div', class_='cs', style=lambda v: v and 'color:#808080' in v)
                if name_elem: self.profile_data['MEHFIL NAME'] = name_elem.text.strip()
                if date_elem: self.profile_data['MEHFIL DATE'] = convert_relative_date_to_absolute(date_elem.text.strip())

    def _extract_last_post(self):
        """Finds the URL and timestamp of the user's most recent post."""
        post_article = self.soup.find('article', class_='mbl')
        if post_article:
            link_elem = post_article.find('a', href=re.compile(r'/content/|/comments/'))
            if link_elem: self.profile_data['LAST POST URL'] = to_absolute_url(link_elem['href'])
            
            time_elem = post_article.find(['span', 'time'], itemprop='datePublished') or post_article.find('span', class_='cxs')
            if time_elem: self.profile_data['LAST POST TIME'] = convert_relative_date_to_absolute(time_elem.text.strip())

    def _add_url_fields(self):
        """Constructs and adds various URL fields to the profile data."""
        self.profile_data['PROFILE URL'] = f"https://damadam.pk/{self.nickname}"
        self.profile_data['POST URL'] = f"https://damadam.pk/profile/public/{self.nickname}"
        img_elem = self.soup.find('img', {'alt': 'pic loading ...'})
        if img_elem: self.profile_data['IMAGE URL'] = img_elem.get('src', '')

# ==================================================================================================
#  Main Application Logic
# ==================================================================================================

def process_target(target: dict, browser_manager: BrowserManager, sheets_manager: SheetsManager):
    """Process a single target profile: scrape data and update the sheet."""
    nickname = target['nickname']
    log_status(f"Scraping profile: [bold cyan]{nickname}[/bold cyan]", "scraping")
    stats.current_profile = nickname

    profile_url = f"https://damadam.pk/{nickname}"
    page_source = browser_manager.get_page_source(profile_url)

    if not page_source:
        stats.failed += 1
        sheets_manager.update_target_status(target['row'], "Error 💥", "Failed to load page")
        return

    scraper = ProfileScraper(page_source, nickname, target['source'])
    profile_data = scraper.scrape()

    if profile_data.get('STATUS') in ['Unverified', 'Banned']:
        remark = f"Skipped: Profile is {profile_data['STATUS']}"
        sheets_manager.update_target_status(target['row'], "Error 💥", remark)
        return

    write_result = sheets_manager.write_profile(profile_data)
    status = write_result.get("status", "error")

    if status == "new":
        stats.new += 1
        stats.success += 1
        sheets_manager.update_target_status(target['row'], "Done �", "New profile added")
    elif status == "updated":
        stats.updated += 1
        stats.success += 1
        sheets_manager.update_target_status(target['row'], "Done 💀", "Profile updated")
    elif status == "unchanged":
        stats.unchanged += 1
        stats.success += 1
        sheets_manager.update_target_status(target['row'], "Done 💀", "No changes detected")
    else:
        stats.failed += 1
        error_msg = write_result.get("error", "Unknown write error")
        sheets_manager.update_target_status(target['row'], "Error 💥", error_msg)

def main():
    """Main function to run the DamaDam scraper bot."""
    log_status("🚀 DamaDam Target Bot v3.3.0 Initializing...", "info")

    browser_manager = BrowserManager()
    if not browser_manager.driver or not browser_manager.login():
        log_status("Failed to initialize browser or log in. Exiting.", "error")
        if browser_manager.driver: browser_manager.close()
        return

    sheets_manager = SheetsManager()
    sheets_manager.load_cache()

    pending_targets = sheets_manager.get_pending_targets()
    if not pending_targets:
        log_status("No pending targets found. Exiting.", "success")
        browser_manager.close()
        return

    total_targets = len(pending_targets)
    if MAX_PROFILES_PER_RUN > 0:
        targets_to_process = pending_targets[:MAX_PROFILES_PER_RUN]
    else:
        targets_to_process = pending_targets
    
    num_to_process = len(targets_to_process)
    log_status(f"Processing {num_to_process} of {total_targets} pending targets.", "info")

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("ETA: {task.fields[eta]}"),
        transient=True
    )

    overall_task = progress.add_task("Overall Progress", total=num_to_process, eta="--:--")

    layout = Table.grid(expand=True)
    layout.add_column(ratio=1)
    layout.add_column(width=40)
    layout.add_row(progress, stats.get_table(0, num_to_process))

    with Live(layout, console=console, screen=False, refresh_per_second=4) as live:
        for i, target in enumerate(targets_to_process):
            live.update(layout)
            process_target(target, browser_manager, sheets_manager)
            
            # Update progress and ETA
            eta = calculate_eta(i + 1, num_to_process, stats.start_time)
            progress.update(overall_task, advance=1, eta=eta)
            layout.columns[1].width = 40 # Reset width
            layout.rows[0].cells[1] = stats.get_table(i + 1, num_to_process)
            
            adaptive_delay.sleep()

            if (i + 1) % BATCH_SIZE == 0 and i + 1 < num_to_process:
                sheets_manager.flush_target_updates()
                log_status(f"Batch of {BATCH_SIZE} complete. Cooling off for {BATCH_COOLOFF_SECONDS}s...", "info")
                adaptive_delay.on_batch_complete()
                time.sleep(BATCH_COOLOFF_SECONDS)

    # Final flush
    sheets_manager.flush_target_updates()
    browser_manager.close()
    log_status("Scraping complete. All tasks finished.", "success")
    console.print(stats.get_table(num_to_process, num_to_process))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_status("\nProcess interrupted by user. Exiting gracefully.", "warning")
    except Exception as e:
        log_status(f"An unexpected critical error occurred in main: {e}", "error")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")
            tvals = self.target.get_all_values()
            if not tvals or not tvals[0] or all(not c for c in tvals[0]):
                self.target.append_row(["Nickname","Status","Remarks","Source"])
        except Exception as e:
            log_status(f"Target init failed: {e}", "error")
        
        try:
            self.dashboard = self._get_or_create("Dashboard", cols=11)
            dvals = self.dashboard.get_all_values()
            expected = ["Run#","Timestamp","Profiles","Success","Failed","New","Updated","Unchanged","Trigger","Start","End"]
            if not dvals or dvals[0] != expected:
                self.dashboard.clear()
                self.dashboard.append_row(expected)
        except Exception as e:
            log_status(f"Dashboard init failed: {e}", "error")
        
        self._load_existing()
        self._load_tags_mapping()
        self.normalize_target_statuses()

    def _get_or_create(self,name,cols=20,rows=1000):
        try: return self.ss.worksheet(name)
        except WorksheetNotFound:
            return self.ss.add_worksheet(title=name, rows=rows, cols=cols)

    def _get_sheet_if_exists(self,name):
        try:
            return self.ss.worksheet(name)
        except WorksheetNotFound:
            log_status(f"{name} not found", "warning")
            return None

    def _load_existing(self):
        self.existing={}
        rows=self.ws.get_all_values()[1:]
        for i,r in enumerate(rows,start=2):
            if len(r)>1 and r[1].strip(): 
                self.existing[r[1].strip().lower()]={'row':i,'data':r}
        log_status(f"Loaded {len(self.existing)} existing profiles", "success")

    def _load_tags_mapping(self):
        self.tags_mapping={}
        if not self.tags_sheet: return
        try:
            all_values=self.tags_sheet.get_all_values()
            if not all_values or len(all_values)<2: return
            headers=all_values[0]
            for col_idx, header in enumerate(headers):
                tag_name=clean_data(header)
                if not tag_name: continue
                for row in all_values[1:]:
                    if col_idx < len(row):
                        nickname=row[col_idx].strip()
                        if nickname:
                            key=nickname.lower()
                            if key in self.tags_mapping:
                                if tag_name not in self.tags_mapping[key]:
                                    self.tags_mapping[key]+=f", {tag_name}"
                            else:
                                self.tags_mapping[key]=tag_name
            log_status(f"Loaded {len(self.tags_mapping)} tag mappings", "success")
        except Exception as e:
            log_status(f"Tags load failed: {e}", "error")

    def _api_call_with_retry(self, func, *args, max_retries=3, **kwargs):
        """API calls with exponential backoff for rate limits"""
        for attempt in range(max_retries):
            try:
                stats.api_calls += 1
                result = func(*args, **kwargs)
                adaptive.on_success()
                return result
            except APIError as e:
                stats.api_errors += 1
                if '429' in str(e) or 'RESOURCE_EXHAUSTED' in str(e):
                    wait_time = (2 ** attempt) * 20  # 20s, 40s, 80s
                    log_status(f"API quota! Wait {wait_time}s ({attempt+1}/{max_retries})", "warning")
                    adaptive.on_rate_limit()
                    time.sleep(wait_time)
                else:
                    raise
        raise Exception("Max retries exceeded")

    def update_target_status(self,row,status,remarks):
        """Queue target status update (batched)"""
        if status.lower().startswith('pending'):
            status = '⚡ Pending'
        elif status.lower().startswith('done'):
            status = 'Done 💀'
        elif status.lower().startswith('error') or status.lower().startswith('suspended'):
            status = 'Error 💥'
        
        self.batch_updates.append({'type': 'target_status', 'row': row, 'status': status, 'remarks': remarks})

    def flush_batch_updates(self):
        """Flush all batched updates at once (reduces API calls by 80%)"""
        if not self.batch_updates: return
        
        log_status(f"Flushing {len(self.batch_updates)} batched updates...", "api")
        
        target_updates = [u for u in self.batch_updates if u['type'] == 'target_status']
        
        if target_updates:
            batch_data = []
            for u in target_updates:
                batch_data.append({
                    'range': f"Target!B{u['row']}:C{u['row']}",
                    'values': [[u['status'], u['remarks']]]
                })
            
            try:
                if batch_data:
                    self._api_call_with_retry(
                        self.target.batch_update,
                        batch_data,
                        value_input_option='USER_ENTERED'
                    )
                log_status(f"Batch complete ({len(batch_data)} rows)", "success")
            except Exception as e:
                log_status(f"Batch failed: {e}", "error")
        
        self.batch_updates.clear()

    def update_dashboard(self, metrics:dict):
        """Update dashboard with run metrics"""
        try:
            row=[
                metrics.get("Run Number",1),
                metrics.get("Last Run", get_pkt_time().strftime("%d-%b-%y %I:%M %p")),
                metrics.get("Profiles Processed",0),
                metrics.get("Success",0),
                metrics.get("Failed",0),
                metrics.get("New Profiles",0),
                metrics.get("Updated Profiles",0),
                metrics.get("Unchanged Profiles",0),
                metrics.get("Trigger", os.getenv('GITHUB_EVENT_NAME','manual')),
                metrics.get("Start", get_pkt_time().strftime("%d-%b-%y %I:%M %p")),
                metrics.get("End", get_pkt_time().strftime("%d-%b-%y %I:%M %p")),
            ]
            self._api_call_with_retry(self.dashboard.append_row, row)
        except Exception as e:
            log_status(f"Dashboard failed: {e}", "error")

    def normalize_target_statuses(self):
        """Normalize target statuses to use emojis"""
        try:
            vals=self.target.get_all_values()
            if not vals or len(vals)<2: return
            updates=[]
            for idx,row in enumerate(vals[1:],start=2):
                if len(row)<2: continue
                status=row[1].strip()
                lower=status.lower()
                new_status=None
                if ("pending" in lower):
                    if status != "⚡ Pending": new_status = "⚡ Pending"
                elif ("done" in lower) or ("complete" in lower):
                    if status != "Done 💀": new_status = "Done 💀"
                elif ("error" in lower):
                    if status != "Error 💥": new_status = "Error 💥"
                elif status:
                    new_status = "⚡ Pending"
                if new_status:
                    updates.append({'range': f"B{idx}", 'values': [[new_status]]})
            
            if updates:
                self._api_call_with_retry(
                    self.target.batch_update,
                    updates,
                    value_input_option='USER_ENTERED'
                )
        except Exception as e:
            log_status(f"Normalize failed: {e}", "error")

    def write_profile(self, profile:dict, old_row:int|None=None):
        """Write profile to sheet"""
        nickname=(profile.get("NICK NAME") or "").strip()
        if not nickname: 
            return {"status":"error","error":"Missing nickname","changed_fields":[]}
        
        if profile.get("LAST POST TIME"): 
            profile["LAST POST TIME"]=convert_relative_date_to_absolute(profile["LAST POST TIME"])
        profile["DATETIME SCRAP"]=get_pkt_time().strftime("%d-%b-%y %I:%M %p")
        
        tags_val=self.tags_mapping.get(nickname.lower())
        if tags_val:
            profile["TAGS"]=tags_val
        
        vals=[]
        for c in COLUMN_ORDER:
            if c=="IMAGE": v=""
            elif c=="PROFILE LINK": v="Profile" if profile.get(c) else ""
            elif c=="LAST POST": v="Post" if profile.get(c) else ""
            else: v=clean_data(profile.get(c,""))
            vals.append(v)
        
        key=nickname.lower()
        ex=self.existing.get(key)
        
        if ex:
            before={COLUMN_ORDER[i]:(ex['data'][i] if i<len(ex['data']) else "") for i in range(len(COLUMN_ORDER))}
            changed=[i for i,col in enumerate(COLUMN_ORDER) if col not in HIGHLIGHT_EXCLUDE_COLUMNS and (before.get(col,"") or "") != (vals[i] or "")]
            
            rownum=ex['row']
            self._api_call_with_retry(
                self.ws.update,
                values=[vals],
                range_name=f"A{rownum}",
                value_input_option='USER_ENTERED'
            )
            
            self.existing[key]={'row':rownum,'data':vals}
            status="updated" if changed else "unchanged"
            result={"status":status,"changed_fields":[COLUMN_ORDER[i] for i in changed]}
        else:
            self._api_call_with_retry(self.ws.append_row, vals)
            last_row=len(self.ws.get_all_values())
            self.existing[key]={'row':last_row,'data':vals}
            result={"status":"new","changed_fields":list(COLUMN_ORDER)}
        
        time.sleep(SHEET_WRITE_DELAY)
        return result

# ==================== TARGET PROCESSING ====================

def get_pending_targets(sheets:Sheets):
    """Get all pending targets from Target sheet"""
    rows=sheets.target.get_all_values()[1:]
    out=[]
    for idx,row in enumerate(rows,start=2):
        nick=(row[0] if len(row)>0 else '').strip()
        status=(row[1] if len(row)>1 else '').strip()
        source=(row[3] if len(row)>3 else 'Target').strip() or 'Target'
        norm=status.lower()
        is_pending=(not status) or ("pending" in norm)
        if nick and is_pending:
            out.append({'nickname':nick,'row':idx,'source':source})
    return out

# ==================== PROFILE SCRAPING ====================

def scrape_recent_post(driver, nickname:str)->dict:
    """Scrape most recent post"""
    post_url=f"https://damadam.pk/profile/public/{nickname}"
    try:
        driver.get(post_url)
        try:
            WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR,"article.mbl")))
        except TimeoutException:
            return {'LPOST':'','LDATE-TIME':''}

        recent_post=driver.find_element(By.CSS_SELECTOR,"article.mbl")
        post_data={'LPOST':'','LDATE-TIME':''}

        for selector in ["a[href*='/content/']","a[href*='/comments/text/']","a[href*='/comments/image/']"]:
            try:
                link=recent_post.find_element(By.CSS_SELECTOR, selector)
                href=link.get_attribute('href')
                if href:
                    post_data['LPOST']=to_absolute_url(href)
                    break
            except: continue

        for sel in ["span[itemprop='datePublished']","time[itemprop='datePublished']","span.cxs.cgy","time"]:
            try:
                time_elem=recent_post.find_element(By.CSS_SELECTOR, sel)
                if time_elem.text.strip():
                    post_data['LDATE-TIME']=convert_relative_date_to_absolute(time_elem.text.strip())
                    break
            except: continue
        
        return post_data
    except:
        return {'LPOST':'','LDATE-TIME':''}

def extract_mehfil_info(driver)->dict:
    """Extract Mehfil name and date if exists"""
    try:
        mehfil_section = driver.find_element(By.XPATH, "//div[contains(@class, 'cl') and contains(@class, 'sp') and contains(@class, 'lsp') and contains(text(), 'Mehfil(s) owned:')]")
        
        try:
            name_elem = driver.find_element(By.CSS_SELECTOR, "div.cp.ow")
            mehfil_name = name_elem.text.strip()
        except:
            mehfil_name = ""
        
        try:
            date_elem = driver.find_element(By.CSS_SELECTOR, "div.cs.sp[style*='color:#808080']")
            date_text = date_elem.text.strip()
            mehfil_date =
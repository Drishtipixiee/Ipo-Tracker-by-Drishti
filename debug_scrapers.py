import sys
import os
from datetime import datetime
import logging
import json
import requests
from bs4 import BeautifulSoup
import re
import threading
import queue # Needed for MessageAnnouncer dummy
import time # For delays if needed

# Add parent directory to sys.path to ensure module imports work if this script is run standalone
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- MockConfig for Debugging ---
# This class provides all necessary configuration values for the debug script.
# It overrides the actual config.py to ensure isolated testing without affecting the main application.
class MockConfig:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "debug_data") # Use a separate data dir for debugging
    os.makedirs(DATA_DIR, exist_ok=True) # Ensure it exists

    HISTORY_FILE = os.path.join(DATA_DIR, "debug_history.json")
    LOCATION_CACHE = os.path.join(DATA_DIR, "debug_location_cache.json")

    # --- UPDATED USER_AGENT for better website access ---
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"
    REQUEST_TIMEOUT = 10 # seconds for web requests
    SCRAPE_INTERVAL_SECONDS = 3600 # 1 hour (3600 seconds) - not used in debug but kept for completeness

    # --- UPDATED IPO Source URLs (MATCHING YOUR config.py) ---
    MONEYCONTROL_IPO_URL = "https://www.moneycontrol.com/stocks/ipo/upcoming_ipo.php"
    CHITTORGARH_MAIN_URL = "https://www.chittorgarh.com/ipo/ipo_dashboard.asp"
    CHITTORGARH_SME_URL = "https://www.chittorgarh.com/report/ipo-in-india-list-main-board-sme/82/all/?year=2025"
    CHITTORGARH_GMP_URL = "https://www.chittorgarh.com/book-chapter/ipo-grey-market-gmp/28"
    IPOCENTRAL_SUBSCRIPTION_URL = "https://www.ipocentral.in/ipo-subscription-status-live/"
    
    # --- YOU MUST FIND AND UPDATE THIS CLASS NAME FOR IPOCENTRAL ---
    # Go to https://www.ipocentral.in/ipo-subscription-status-live/,
    # right-click the main table, select "Inspect", and find the 'class' attribute of the <table> tag.
    # Example: If you see <table class="some-new-table-class">, then use "some-new-table-class"
    IPOCENTRAL_TABLE_CLASS = "YOUR_IPOCENTRAL_TABLE_CLASS_HERE" # <--- UPDATE THIS LINE!

    # --- RSS Feed URLs ---
    INC42_FEED_URL = "https://inc42.com/feed/"
    YOURSTORY_FEED_URL = "https://yourstory.com/feed/"

    # --- News Keywords for Filtering and Type Classification ---
    NEWS_KEYWORDS = [
        "funding", "fundraise", "investment", "acquires", "acquisition", "merger",
        "buys", "seed round", "series a", "series b", "series c", "series d",
        "raises", "closes round", "secures", "bags investment", "ipo", "public offer"
    ]

    IPO_DATE_FORMATS = [
        "%b %d, %Y", # Jul 24, 2025 (Moneycontrol, Chittorgarh)
        "%d %b %Y", # 24 Jul 2025
        "%d %b",     # 24 Jul (NEW)
        "%b %d",     # Jul 24 (NEW)
        "%Y-%m-%d"   # Standard ISO format if converted
    
    ]

# Override the config module with our debug MockConfig
config = MockConfig

# --- Mock the announcer and file_lock for standalone testing ---
class MockAnnouncer:
    def announce(self, msg):
        logging.info(f"MOCK SSE Announce: {msg[:100]}...") # Just log it

announcer = MockAnnouncer()
file_lock = threading.Lock() # Still use a real lock if functions expect it

# Reconfigure logging for debug script to show more detail
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- START OF CORE FUNCTIONS (Copied from ipo_tracker.py/app.py) ---
def load_json_file(filepath, default_value=None):
    """Loads JSON data from a file, with robust error handling."""
    if default_value is None:
        default_value = {} # Default for history and cache is an empty dictionary

    with file_lock:
        if not os.path.exists(filepath):
            logging.info(f"File not found: {filepath}. Returning default value.")
            return default_value
        if os.path.getsize(filepath) == 0:
            logging.warning(f"File is empty: {filepath}. Returning default value.")
            return default_value
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                logging.debug(f"Successfully loaded data from {filepath}")
                return data
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {filepath}: {e}. File might be corrupted. Returning default value.", exc_info=True)
            return default_value
        except IOError as e:
            logging.error(f"IOError loading JSON from {filepath}: {e}. Returning default value.", exc_info=True)
            return default_value

def save_json_file(filepath, data):
    """Saves data to a JSON file."""
    with file_lock:
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.debug(f"Successfully saved data to {filepath}")
        except IOError as e:
            logging.error(f"Error saving JSON to {filepath}: {e}", exc_info=True)

def load_location_cache():
    return load_json_file(config.LOCATION_CACHE)

def save_location_cache(cache):
    save_json_file(config.LOCATION_CACHE, cache)

def update_history(event_date_str, event_type, entry_data):
    """
    Updates the history file with structured data, preventing duplicates,
    and also announces it via SSE. Ensures the history structure is robust.
    """
    history = load_json_file(config.HISTORY_FILE)

    # Validate and ensure event_date_str is in YYYY-MM-DD format
    try:
        parsed_date = parse_date_flexible(event_date_str)
        if parsed_date:
            event_date_str = parsed_date.strftime("%Y-%m-%d")
        else:
            logging.warning(f"Invalid date string '{event_date_str}' provided for history update. Using today's date.")
            event_date_str = datetime.now().strftime("%Y-%m-%d")
    except Exception as e:
        logging.error(f"Error parsing event_date_str '{event_date_str}': {e}. Using today's date.", exc_info=True)
        event_date_str = datetime.now().strftime("%Y-%m-%d")

    if event_date_str not in history or not isinstance(history[event_date_str], dict):
        if event_date_str in history:
            logging.warning(f"History entry for date '{event_date_str}' was malformed (not a dictionary). Re-initializing it.")
        history[event_date_str] = {"ipos": [], "news": []}
    
    if "ipos" not in history[event_date_str]:
        history[event_date_str]["ipos"] = []
    if "news" not in history[event_date_str]:
        history[event_date_str]["news"] = []

    target_list = history[event_date_str].get(event_type, [])
    
    is_duplicate = False
    if event_type == "ipos":
        for existing_entry in target_list:
            if (existing_entry.get("company", "").lower() == entry_data.get("company", "").lower() and
                existing_entry.get("open_date", "") == entry_data.get("open_date", "")):
                existing_entry.update(entry_data)
                is_duplicate = True
                logging.debug(f"Updated existing IPO entry for {entry_data.get('company')}")
                break
    elif event_type == "news":
        for existing_entry in target_list:
            if (existing_entry.get("title", "").lower() == entry_data.get("title", "").lower() and
                existing_entry.get("link", "") == entry_data.get("link", "")):
                is_duplicate = True
                logging.debug(f"Duplicate news entry found for {entry_data.get('title')}")
                break

    if not is_duplicate:
        target_list.append(entry_data)
        logging.info(f"Added new {event_type} entry for {event_date_str}: {entry_data.get('company') or entry_data.get('title')}")
    
    history[event_date_str][event_type] = target_list
    save_json_file(config.HISTORY_FILE, history)
    
    announcer.announce(json.dumps({"date": event_date_str, "type": event_type, "data": entry_data}))

def parse_date_flexible(date_str):
    if not date_str or date_str.upper() in ['N.A.', 'TBD', 'YET TO BE ANNOUNCED']:
        return None
    for fmt in config.IPO_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    logging.warning(f"Could not parse date string: '{date_str}' with any known formats.")
    return None

def get_company_location(company):
    cache = load_location_cache()
    if company in cache:
        logging.debug(f"Location for '{company}' found in cache: {cache[company]}")
        return cache[company]

    logging.info(f"Attempting to find location for '{company}' via DuckDuckGo...")
    try:
        query = f"{company} headquarters location"
        url = f"https://duckduckgo.com/html/?q={requests.utils.quote(query)}"
        
        session = requests.Session()
        retry_strategy = requests.packages.urllib3.util.retry.Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods={"HEAD", "GET", "OPTIONS"}
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        res = session.get(url, headers={"User-Agent": config.USER_AGENT}, timeout=config.REQUEST_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get('content'):
            match = re.search(r'(?:in|based in|from)\s+([A-Z][a-z]+(?:[\s,-]*[A-Z][a-z]+)*)', meta_desc['content'])
            if match:
                location = match.group(1).strip()
                cache[company] = location
                save_location_cache(cache)
                logging.info(f"Found location for '{company}' (meta): {location}")
                return location

        snippet = soup.find("a", class_="result__snippet")
        if snippet:
            match = re.search(r'(?:in|based in|from)\s+([A-Z][a-z]+(?:[\s,-]*[A-Z][a-z]+)*)', snippet.text)
            if match:
                location = match.group(1).strip()
                cache[company] = location
                save_location_cache(cache)
                logging.info(f"Found location for '{company}' (snippet): {location}")
                return location
            
        body_text = soup.get_text()
        location_keywords = ["headquartered in", "based in", "located in"]
        for keyword in location_keywords:
            match = re.search(f'{keyword}\\s+([A-Z][a-z]+(?:[\\s,\\-]*[A-Z][a-z]+|\\s*\\([A-Z]+\\))*)(?:\\s|\\.|,|$)', body_text, re.IGNORECASE)
            if match:
                location = match.group(1).strip()
                location = re.sub(r'[.,)\s]+$', '', location)
                cache[company] = location
                save_location_cache(cache)
                logging.info(f"Found location for '{company}' (body text): {location}")
                return location

    except requests.exceptions.Timeout:
        logging.warning(f"Timeout occurred getting location for {company}.")
    except requests.exceptions.RequestException as e:
        logging.warning(f"Network error getting location for {company}: {e}")
    except Exception as e:
        logging.warning(f"Error parsing location for {company}: {e}", exc_info=True)

    cache[company] = "Location Unknown"
    save_location_cache(cache)
    logging.info(f"Could not determine location for '{company}'. Setting to 'Location Unknown'.")
    return "Location Unknown"

def get_ipo_status(open_date_str, close_date_str, listing_date_str):
    """Determines the current status of an IPO based on parsed dates."""
    today = datetime.now().date()
    
    open_date = parse_date_flexible(open_date_str)
    close_date = parse_date_flexible(close_date_str)
    listing_date = parse_date_flexible(listing_date_str)

    if listing_date and today >= listing_date:
        return "Listed"
    elif open_date and close_date and open_date <= today <= close_date:
        return "Live"
    elif open_date and today < open_date:
        return "Upcoming"
    elif close_date and today > close_date:
        return "Closed"
    else:
        return "Unknown"

def get_moneycontrol_ipos():
    logging.info("Starting IPO scrape from Moneycontrol...")
    try:
        headers = {"User-Agent": config.USER_AGENT}
        session = requests.Session()
        retry_strategy = requests.packages.urllib3.util.retry.Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        response = session.get(config.MONEYCONTROL_IPO_URL, headers=headers, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "tblporhd"})

        if not table:
            logging.warning("IPO table not found on Moneycontrol page. Check HTML structure or URL.")
            return

        rows = table.find_all("tr")[1:]
        for row_num, row in enumerate(rows):
            cols = row.find_all("td")
            if len(cols) >= 5:
                company = cols[0].get_text(strip=True)
                open_date_str = cols[1].get_text(strip=True)
                close_date_str = cols[2].get_text(strip=True)
                price_band = cols[3].get_text(strip=True)
                listing_date_str = cols[4].get_text(strip=True)
                
                event_date_for_history = (parse_date_flexible(listing_date_str) or 
                                          parse_date_flexible(close_date_str) or 
                                          parse_date_flexible(open_date_str))

                if not event_date_for_history:
                    logging.warning(f"Could not parse any valid date for Moneycontrol IPO: {company}. Skipping.")
                    continue

                location = get_company_location(company)
                status = get_ipo_status(open_date_str, close_date_str, listing_date_str)

                ipo_data = {
                    "company": company,
                    "location": location,
                    "open_date": open_date_str,
                    "close_date": close_date_str,
                    "listing_date": listing_date_str,
                    "price_band": price_band,
                    "lot_size": "N.A.",
                    "gmp": "N.A.",
                    "subscription": "N.A.",
                    "status": status,
                    "source": "Moneycontrol"
                }
                update_history(event_date_for_history.strftime("%Y-%m-%d"), "ipos", ipo_data)
            else:
                logging.debug(f"Skipping malformed Moneycontrol IPO row (not enough columns): {row.get_text(strip=True)[:50]}...")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network or HTTP error fetching Moneycontrol IPOs: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Moneycontrol IPO scraping: {e}", exc_info=True)

def get_chittorgarh_ipos_data():
    logging.info("Starting IPO scrape from Chittorgarh (Mainboard & SME)...")
    urls = {
        "mainboard": config.CHITTORGARH_MAIN_URL,
        "sme": config.CHITTORGARH_SME_URL
    }
    
    for ipo_type, url in urls.items():
        try:
            headers = {"User-Agent": config.USER_AGENT}
            session = requests.Session()
            retry_strategy = requests.packages.urllib3.util.retry.Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            response = session.get(url, headers=headers, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Updated regex for table class names for broader matching
            tables = soup.find_all("table", class_=re.compile(r'(current_ipos|upcoming_ipos|listed_ipos|table_data|table-striped|table-bordered|table-responsive|responsive-table)'))
            
            if not tables:
                logging.warning(f"No IPO tables found on Chittorgarh {ipo_type} page: {url}. Check HTML structure or URL.")
                continue

            for table in tables:
                rows = table.find_all("tr")
                if not rows or len(rows) < 2:
                    logging.debug(f"Table on {url} has no data rows. Skipping.")
                    continue

                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text(strip=True).lower() for h in header_cells]
                
                col_map = {
                    'company': -1, 'open date': -1, 'close date': -1,
                    'listing date': -1, 'price band': -1, 'lot size': -1
                }
                for i, header in enumerate(headers):
                    if 'company' in header or 'ipo name' in header: col_map['company'] = i
                    elif 'open date' in header or 'open' in header: col_map['open date'] = i
                    elif 'close date' in header or 'close' in header: col_map['close date'] = i
                    elif 'listing date' in header or 'listing' in header: col_map['listing date'] = i
                    elif 'price band' in header or 'price' in header: col_map['price band'] = i
                    elif 'lot size' in header or 'lot' in header: col_map['lot size'] = i
                
                if col_map['company'] == -1:
                    logging.warning(f"Could not find 'Company' column in a table on {url}. Skipping table.")
                    continue

                for row_num, row in enumerate(rows[1:]):
                    cols = row.find_all("td")
                    # Check if enough columns exist for the mapped indices
                    if not all(idx < len(cols) for idx in col_map.values() if idx != -1):
                        logging.debug(f"Skipping incomplete row on {url}: {row.get_text(strip=True)[:50]}...")
                        continue

                    company = cols[col_map['company']].get_text(strip=True) if col_map['company'] != -1 else "N.A."
                    open_date_str = cols[col_map['open date']].get_text(strip=True) if col_map['open date'] != -1 else "N.A."
                    close_date_str = cols[col_map['close date']].get_text(strip=True) if col_map['close date'] != -1 else "N.A."
                    listing_date_str = cols[col_map['listing date']].get_text(strip=True) if col_map['listing date'] != -1 else "N.A."
                    price_band = cols[col_map['price band']].get_text(strip=True) if col_map['price band'] != -1 else "N.A."
                    lot_size = cols[col_map['lot size']].get_text(strip=True) if col_map['lot size'] != -1 else "N.A."
                    
                    event_date_for_history = (parse_date_flexible(listing_date_str) or 
                                              parse_date_flexible(close_date_str) or 
                                              parse_date_flexible(open_date_str))
                                              
                    if not event_date_for_history:
                        logging.debug(f"Could not parse any valid date for Chittorgarh {ipo_type} IPO: {company}. Skipping update for this entry.")
                        continue

                    location = get_company_location(company)
                    status = get_ipo_status(open_date_str, close_date_str, listing_date_str)

                    ipo_data = {
                        "company": company,
                        "location": location,
                        "open_date": open_date_str,
                        "close_date": close_date_str,
                        "listing_date": listing_date_str,
                        "price_band": price_band,
                        "lot_size": lot_size,
                        "gmp": "N.A.",
                        "subscription": "N.A.",
                        "status": status,
                        "source": f"Chittorgarh {ipo_type.capitalize()}"
                    }
                    update_history(event_date_for_history.strftime("%Y-%m-%d"), "ipos", ipo_data)

        except requests.exceptions.RequestException as e:
            logging.error(f"Network or HTTP error fetching Chittorgarh {ipo_type} IPOs from {url}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred during Chittorgarh {ipo_type} IPO scraping: {e}", exc_info=True)

def get_chittorgarh_gmp():
    logging.info("Starting GMP scrape from Chittorgarh...")
    try:
        headers = {"User-Agent": config.USER_AGENT}
        session = requests.Session()
        retry_strategy = requests.packages.urllib3.util.retry.Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        response = session.get(config.CHITTORGARH_GMP_URL, headers=headers, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # More robust regex for GMP table class names
        gmp_table = soup.find("table", class_=re.compile(r'table-bordered|table-striped|responsive-table|gmp-table')) 
        if not gmp_table:
            logging.warning("GMP table not found on Chittorgarh GMP page. Check HTML structure.")
            return

        rows = gmp_table.find_all("tr")
        if not rows or len(rows) < 2:
            logging.warning("GMP table has no data rows. Skipping.")
            return

        header_cells = rows[0].find_all(['th', 'td'])
        headers = [h.get_text(strip=True).lower() for h in header_cells]
        
        company_col_idx = -1
        gmp_col_idx = -1
        
        for i, header in enumerate(headers):
            if 'company' in header or 'ipo name' in header: company_col_idx = i
            elif 'gmp' in header: gmp_col_idx = i
        
        if company_col_idx == -1 or gmp_col_idx == -1:
            logging.warning("Could not find 'Company' or 'GMP' columns in Chittorgarh GMP table. Skipping.")
            return

        history = load_json_file(config.HISTORY_FILE)
        history_updated_flag = False

        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) > max(company_col_idx, gmp_col_idx): 
                company_name = cols[company_col_idx].get_text(strip=True)
                gmp_value = cols[gmp_col_idx].get_text(strip=True)
                
                found_and_updated = False
                for date_str, date_data in history.items():
                    if "ipos" in date_data:
                        for ipo in date_data["ipos"]:
                            if ipo.get("company", "").lower() == company_name.lower() or \
                               company_name.lower() in ipo.get("company", "").lower(): # Check if GMP company name is substring of IPO company name
                                
                                if ipo.get("gmp") != gmp_value:
                                    ipo["gmp"] = gmp_value
                                    # Since update_history handles duplicates, we can just call it
                                    # This re-saves the entire history, but for debug it's fine.
                                    update_history(date_str, "ipos", ipo)
                                    logging.debug(f"Updated GMP for {ipo['company']} to {gmp_value}")
                                    history_updated_flag = True
                                    found_and_updated = True
                                    break # Found and updated for this GMP entry
                        if found_and_updated:
                            break # Break from date_str loop too

                if not found_and_updated:
                    logging.debug(f"Could not find IPO '{company_name}' in history to update GMP.")
            else:
                logging.debug(f"Skipping incomplete GMP row: {row.get_text(strip=True)[:50]}")
        
        if history_updated_flag:
            logging.info("GMP scrape from Chittorgarh complete. History updated.")
        else:
            logging.info("GMP scrape from Chittorgarh complete. No significant changes detected in existing IPOs.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network or HTTP error fetching Chittorgarh GMPs: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Chittorgarh GMP scraping: {e}", exc_info=True)

def get_ipocentral_subscription():
    logging.info("Starting IPO subscription scrape from IPOCentral...")
    try:
        headers = {"User-Agent": config.USER_AGENT}
        session = requests.Session()
        retry_strategy = requests.packages.urllib3.util.retry.Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        response = session.get(config.IPOCENTRAL_SUBSCRIPTION_URL, headers=headers, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # --- THIS LINE USES THE UPDATED CLASS NAME FROM MOCKCONFIG ---
        subscription_table = soup.find("table", class_=config.IPOCENTRAL_TABLE_CLASS)
        if not subscription_table:
            logging.warning(f"Subscription table not found on IPOCentral page with class '{config.IPOCENTRAL_TABLE_CLASS}'. Check HTML structure.")
            return

        rows = subscription_table.find_all("tr")
        if not rows or len(rows) < 2:
            logging.warning("Subscription table has no data rows. Skipping.")
            return

        header_cells = rows[0].find_all(['th', 'td'])
        headers = [h.get_text(strip=True).lower() for h in header_cells]
        
        company_col_idx = -1
        total_sub_col_idx = -1
        
        for i, header in enumerate(headers):
            if 'company' in header or 'ipo name' in header: company_col_idx = i
            elif 'total' in header or 'overall' in header: total_sub_col_idx = i
        
        if company_col_idx == -1 or total_sub_col_idx == -1:
            logging.warning("Could not find 'Company' or 'Total/Overall' subscription columns in IPOCentral table. Skipping.")
            return

        history = load_json_file(config.HISTORY_FILE)
        history_updated_flag = False

        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) > max(company_col_idx, total_sub_col_idx):
                company_name = cols[company_col_idx].get_text(strip=True)
                total_subscription = cols[total_sub_col_idx].get_text(strip=True)
                
                found_and_updated = False
                for date_str, date_data in history.items():
                    if "ipos" in date_data:
                        for ipo in date_data["ipos"]:
                            if ipo.get("company", "").lower() == company_name.lower() or \
                               company_name.lower() in ipo.get("company", "").lower():
                                
                                if ipo.get("subscription") != total_subscription:
                                    ipo["subscription"] = total_subscription
                                    update_history(date_str, "ipos", ipo)
                                    logging.debug(f"Updated subscription for {ipo['company']} to {total_subscription}")
                                    history_updated_flag = True
                                    found_and_updated = True
                                    break
                        if found_and_updated:
                            break
                
                if not found_and_updated:
                    logging.debug(f"Could not find IPO '{company_name}' in history to update subscription.")
            else:
                logging.debug(f"Skipping incomplete subscription row: {row.get_text(strip=True)[:50]}")
        
        if history_updated_flag:
            logging.info("Subscription scrape from IPOCentral complete. History updated.")
        else:
            logging.info("Subscription scrape from IPOCentral complete. No significant changes detected in existing IPOs.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network or HTTP error fetching IPOCentral subscriptions: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during IPOCentral subscription scraping: {e}", exc_info=True)


# --- END OF CORE FUNCTIONS ---


# --- Main Execution Block for Debugging ---
if __name__ == "__main__":
    logging.info("Running IPO scrapers in debug mode...")
    
    # Clean up previous debug data to ensure a fresh run
    if os.path.exists(config.HISTORY_FILE):
        os.remove(config.HISTORY_FILE)
        logging.info(f"Removed old debug history: {config.HISTORY_FILE}")
    if os.path.exists(config.LOCATION_CACHE):
        os.remove(config.LOCATION_CACHE)
        logging.info(f"Removed old location cache: {config.LOCATION_CACHE}")
    
    # Run the scraper functions
    get_moneycontrol_ipos()
    time.sleep(1) # Small delay
    get_chittorgarh_ipos_data()
    time.sleep(1) # Small delay
    get_chittorgarh_gmp()
    time.sleep(1) # Small delay
    get_ipocentral_subscription()

    logging.info("Debug scraping complete. Check debug_data/debug_history.json")

    # Print the final history content for immediate review
    final_history = load_json_file(config.HISTORY_FILE)
    print("\n--- Final Debug History ---")
    print(json.dumps(final_history, indent=2, ensure_ascii=False))
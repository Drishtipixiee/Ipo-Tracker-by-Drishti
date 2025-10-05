from flask import Flask, render_template_string, request, Response
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import feedparser
import json
import os
import re
import logging
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import queue

import config # Ensure config.py is in the same directory

app = Flask(__name__)

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Set requests logging to WARNING to reduce verbosity
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- Global Lock for File Operations ---
file_lock = threading.Lock()

# --- SSE Message Queue and Announcer ---
class MessageAnnouncer:
    def __init__(self):
        self.listeners = []

    def listen(self):
        q = queue.Queue(maxsize=20) # Increased max size for robustness
        self.listeners.append(q)
        return q

    def announce(self, msg):
        # Remove unresponsive listeners first
        self.listeners = [q for q in self.listeners if not q.full()] # Prune full queues
        for q in self.listeners:
            try:
                q.put_nowait(msg)
            except queue.Full:
                # This should ideally not happen often due to pruning, but good to have
                logging.warning(f"Message queue full for a listener. Message: {msg[:50]}...")
            except Exception as e:
                logging.error(f"Error putting message in queue: {e}. Listener might be dead.", exc_info=True)

announcer = MessageAnnouncer()

def format_sse(data: str, event=None, id=None) -> str:
    """Formats a message for Server-Sent Events."""
    msg = f'data: {data}\n\n'
    if event is not None:
        msg = f'event: {event}\n{msg}'
    if id is not None:
        msg = f'id: {id}\n{msg}'
    return msg

# --- File Operations ---
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
            # Optionally, back up the corrupted file before returning default
            # os.rename(filepath, f"{filepath}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}")
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

# Load or initialize company location cache
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
        # Try parsing and reformatting to ensure consistency
        parsed_date = parse_date_flexible(event_date_str)
        if parsed_date:
            event_date_str = parsed_date.strftime("%Y-%m-%d")
        else:
            logging.warning(f"Invalid date string '{event_date_str}' provided for history update. Using today's date.")
            event_date_str = datetime.now().strftime("%Y-%m-%d")
    except Exception as e:
        logging.error(f"Error parsing event_date_str '{event_date_str}': {e}. Using today's date.", exc_info=True)
        event_date_str = datetime.now().strftime("%Y-%m-%d")


    # Ensure the entry for the event_date exists and is a dictionary
    if event_date_str not in history or not isinstance(history[event_date_str], dict):
        if event_date_str in history: # Log only if it existed but was malformed
            logging.warning(f"History entry for date '{event_date_str}' was malformed (not a dictionary). Re-initializing it.")
        history[event_date_str] = {"ipos": [], "news": []}
    
    # Ensure ipos and news keys exist within the date's dictionary
    if "ipos" not in history[event_date_str]:
        history[event_date_str]["ipos"] = []
    if "news" not in history[event_date_str]:
        history[event_date_str]["news"] = []

    target_list = history[event_date_str].get(event_type, [])
    
    is_duplicate = False
    if event_type == "ipos":
        # For IPOs, check by company name and open_date (primary key for unique IPOs)
        for existing_entry in target_list:
            if (existing_entry.get("company", "").lower() == entry_data.get("company", "").lower() and
                existing_entry.get("open_date", "") == entry_data.get("open_date", "")):
                # If it's a duplicate, update existing entry (e.g., subscription, GMP)
                existing_entry.update(entry_data)
                is_duplicate = True
                logging.debug(f"Updated existing IPO entry for {entry_data.get('company')}")
                break
    elif event_type == "news":
        # For News, check by title and link (primary key for unique news)
        for existing_entry in target_list:
            if (existing_entry.get("title", "").lower() == entry_data.get("title", "").lower() and
                existing_entry.get("link", "") == entry_data.get("link", "")):
                is_duplicate = True
                logging.debug(f"Duplicate news entry found for {entry_data.get('title')}")
                break

    if not is_duplicate:
        target_list.append(entry_data)
        logging.info(f"Added new {event_type} entry for {event_date_str}: {entry_data.get('company') or entry_data.get('title')}")
    
    # Update the history object with the potentially modified target_list
    history[event_date_str][event_type] = target_list
    save_json_file(config.HISTORY_FILE, history)
    
    # Announce the update to SSE clients
    announcer.announce(format_sse(json.dumps({"date": event_date_str, "type": event_type, "data": entry_data}), event="update_item"))

# --- Helper for Date Parsing ---
def parse_date_flexible(date_str):
    """
    Tries to parse a date string using multiple known formats.
    Returns a datetime.date object or None if parsing fails.
    """
    if not date_str or date_str.upper() in ['N.A.', 'TBD', 'YET TO BE ANNOUNCED']:
        return None
    for fmt in config.IPO_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    logging.warning(f"Could not parse date string: '{date_str}' with any known formats.")
    return None

# --- Location Finding ---
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
        
        # Prioritize meta description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get('content'):
            # Look for patterns like "based in [City, State, Country]"
            match = re.search(r'(?:in|based in|from)\s+([A-Z][a-z]+(?:[\s,-]*[A-Z][a-z]+)*)', meta_desc['content'])
            if match:
                location = match.group(1).strip()
                cache[company] = location
                save_location_cache(cache)
                logging.info(f"Found location for '{company}' (meta): {location}")
                return location

        # Fallback to snippet text
        snippet = soup.find("a", class_="result__snippet")
        if snippet:
            match = re.search(r'(?:in|based in|from)\s+([A-Z][a-z]+(?:[\s,-]*[A-Z][a-z]+)*)', snippet.text)
            if match:
                location = match.group(1).strip()
                cache[company] = location
                save_location_cache(cache)
                logging.info(f"Found location for '{company}' (snippet): {location}")
                return location
            
        # Fallback to general body text
        body_text = soup.get_text()
        location_keywords = ["headquartered in", "based in", "located in"]
        for keyword in location_keywords:
            match = re.search(f'{keyword}\\s+([A-Z][a-z]+(?:[\\s,\\-]*[A-Z][a-z]+|\\s*\\([A-Z]+\\))*)(?:\\s|\\.|,|$)', body_text, re.IGNORECASE)
            if match:
                location = match.group(1).strip()
                location = re.sub(r'[.,)\s]+$', '', location) # Clean trailing punctuation
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

# --- IPO Specific Scrapers ---

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
    elif close_date and today > close_date: # If closed but not yet listed
        return "Closed"
    else:
        return "Unknown" # Fallback for edge cases or missing dates


def get_moneycontrol_ipos():
    """Scrapes Indian IPO data from Moneycontrol (primary source for basic info)."""
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

        rows = table.find_all("tr")[1:] # Skip header row
        for row_num, row in enumerate(rows):
            cols = row.find_all("td")
            if len(cols) >= 5: # Expecting at least Company, Open, Close, Price, Listing
                company = cols[0].get_text(strip=True)
                open_date_str = cols[1].get_text(strip=True)
                close_date_str = cols[2].get_text(strip=True)
                price_band = cols[3].get_text(strip=True)
                listing_date_str = cols[4].get_text(strip=True)
                
                # Determine primary event date for history categorization
                # Use listing date if available, otherwise close date, otherwise open date
                # Convert to YYYY-MM-DD for storage key
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
                    "lot_size": "N.A.", # Moneycontrol doesn't usually have this
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
    """
    Scrapes IPO details from Chittorgarh (mainboard and SME) for comprehensive data
    including tentative dates, price band, lot size.
    """
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

            # Look for tables containing IPO data (these classes can change)
            # Inspect Chittorgarh.com to find the exact table structure.
            # Example: current_ipos, upcoming_ipos, listed_ipos tables
            tables = soup.find_all("table", class_=re.compile(r'(current_ipos|upcoming_ipos|listed_ipos|table_data|table-striped|table-bordered)'))
            
            if not tables:
                logging.warning(f"No IPO tables found on Chittorgarh {ipo_type} page: {url}. Check HTML structure.")
                continue

            for table in tables:
                rows = table.find_all("tr")
                if not rows or len(rows) < 2: # Need at least header + 1 data row
                    logging.debug(f"Table on {url} has no data rows. Skipping.")
                    continue

                # Determine header columns to map data reliably
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text(strip=True).lower() for h in header_cells]
                
                # Initialize column indices with -1 (not found)
                col_map = {
                    'company': -1, 'open date': -1, 'close date': -1,
                    'listing date': -1, 'price band': -1, 'lot size': -1
                }
                for i, header in enumerate(headers):
                    for key in col_map:
                        if key in header:
                            col_map[key] = i
                            break # Found a match for this key

                if col_map['company'] == -1: # Company name is essential
                    logging.warning(f"Could not find 'Company' column in a table on {url}. Skipping table.")
                    continue

                for row_num, row in enumerate(rows[1:]): # Skip header row
                    cols = row.find_all("td")
                    # Check if enough columns exist to prevent IndexError for common columns
                    if len(cols) <= max(col_map.values()):
                        logging.debug(f"Skipping incomplete row on {url}: {row.get_text(strip=True)[:50]}...")
                        continue

                    company = cols[col_map['company']].get_text(strip=True) if col_map['company'] != -1 else "N.A."
                    open_date_str = cols[col_map['open date']].get_text(strip=True) if col_map['open date'] != -1 else "N.A."
                    close_date_str = cols[col_map['close date']].get_text(strip=True) if col_map['close date'] != -1 else "N.A."
                    listing_date_str = cols[col_map['listing date']].get_text(strip=True) if col_map['listing date'] != -1 else "N.A."
                    price_band = cols[col_map['price band']].get_text(strip=True) if col_map['price band'] != -1 else "N.A."
                    lot_size = cols[col_map['lot size']].get_text(strip=True) if col_map['lot size'] != -1 else "N.A."
                    
                    # Determine primary event date for history categorization
                    event_date_for_history = (parse_date_flexible(listing_date_str) or 
                                              parse_date_flexible(close_date_str) or 
                                              parse_date_flexible(open_date_str))
                                              
                    if not event_date_for_history:
                        logging.debug(f"Could not parse any valid date for Chittorgarh {ipo_type} IPO: {company}. Skipping update for this entry.")
                        continue # Don't add if no valid date

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
                        "gmp": "N.A.", # Will be filled by separate GMP scraper
                        "subscription": "N.A.", # Will be filled by separate subscription scraper
                        "status": status,
                        "source": f"Chittorgarh {ipo_type.capitalize()}"
                    }
                    update_history(event_date_for_history.strftime("%Y-%m-%d"), "ipos", ipo_data)

        except requests.exceptions.RequestException as e:
            logging.error(f"Network or HTTP error fetching Chittorgarh {ipo_type} IPOs from {url}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred during Chittorgarh {ipo_type} IPO scraping: {e}", exc_info=True)


def get_chittorgarh_gmp():
    """Scrapes Grey Market Premium (GMP) from Chittorgarh.com and updates existing IPOs."""
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

        # Find the table containing GMP data - this is highly subject to change
        # Common classes for Chittorgarh tables: table table-bordered table-striped
        gmp_table = soup.find("table", class_=re.compile(r'table-bordered|table-striped')) 
        if not gmp_table:
            logging.warning("GMP table not found on Chittorgarh GMP page. Check HTML structure.")
            return

        rows = gmp_table.find_all("tr")
        if not rows or len(rows) < 2:
            logging.warning("GMP table has no data rows. Skipping.")
            return

        # Dynamically find column indices
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

        history = load_json_file(config.HISTORY_FILE) # Load history once before iterating
        history_updated_flag = False

        for row in rows[1:]: # Skip header
            cols = row.find_all("td")
            if len(cols) > max(company_col_idx, gmp_col_idx): 
                company_name = cols[company_col_idx].get_text(strip=True)
                gmp_value = cols[gmp_col_idx].get_text(strip=True)
                
                found_and_updated = False
                # Iterate through history to find the IPO and update its GMP
                for date_str, date_data in history.items():
                    if "ipos" in date_data:
                        for ipo in date_data["ipos"]:
                            # Attempt to match IPOs, possibly using fuzzy matching for better results
                            # For now, keeping simple check, but be aware of variations
                            if ipo.get("company", "").lower() == company_name.lower() or \
                               company_name.lower() in ipo.get("company", "").lower():
                                
                                if ipo.get("gmp") != gmp_value: # Only update if GMP changed
                                    ipo["gmp"] = gmp_value
                                    # Use update_history to ensure SSE announcement and file save
                                    # Pass the entire modified ipo object back
                                    update_history(date_str, "ipos", ipo)
                                    logging.debug(f"Updated GMP for {ipo['company']} to {gmp_value}")
                                    history_updated_flag = True
                                    found_and_updated = True
                                    break # Found and updated, move to next GMP row
                        if found_and_updated:
                            break # Break outer loop if company found in this date

                if not found_and_updated:
                    logging.debug(f"Could not find IPO '{company_name}' in history to update GMP.")
            else:
                logging.debug(f"Skipping incomplete GMP row: {row.get_text(strip=True)[:50]}")
        
        # No need for a final save_json_file(history) here, as update_history saves each time.
        if history_updated_flag:
            logging.info("GMP scrape from Chittorgarh complete. History updated.")
        else:
            logging.info("GMP scrape from Chittorgarh complete. No significant changes detected in existing IPOs.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network or HTTP error fetching Chittorgarh GMPs: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Chittorgarh GMP scraping: {e}", exc_info=True)


def get_ipocentral_subscription():
    """Scrapes IPO subscription data from IPOCentral.in and updates existing IPOs."""
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

        # Find the table containing subscription data - inspect site for current class
        subscription_table = soup.find("table", class_="ipostatus-table") # Common class example for IPOCentral
        if not subscription_table:
            logging.warning("Subscription table not found on IPOCentral page. Check HTML structure.")
            return

        rows = subscription_table.find_all("tr")
        if not rows or len(rows) < 2:
            logging.warning("Subscription table has no data rows. Skipping.")
            return

        # Dynamically find column indices
        header_cells = rows[0].find_all(['th', 'td'])
        headers = [h.get_text(strip=True).lower() for h in header_cells]
        
        company_col_idx = -1
        total_sub_col_idx = -1 # Assuming this is the 'Total' or 'Overall' subscription
        
        for i, header in enumerate(headers):
            if 'company' in header or 'ipo name' in header: company_col_idx = i
            elif 'total' in header or 'overall' in header: total_sub_col_idx = i
        
        if company_col_idx == -1 or total_sub_col_idx == -1:
            logging.warning("Could not find 'Company' or 'Total/Overall' subscription columns in IPOCentral table. Skipping.")
            return

        history = load_json_file(config.HISTORY_FILE) # Load history once
        history_updated_flag = False

        for row in rows[1:]: # Skip header
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
                                
                                if ipo.get("subscription") != total_subscription: # Only update if changed
                                    ipo["subscription"] = total_subscription
                                    # Use update_history to ensure SSE announcement and file save
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


# --- RSS Feed Scrapers ---
def parse_rss_feed(feed_url, label):
    logging.info(f"Parsing RSS feed: {label} from {feed_url}")
    try:
        feed = feedparser.parse(feed_url)
        if feed.bozo:
            logging.warning(f"RSS feed parsing error for {label}: {feed.bozo_exception}")

        for entry in feed.entries:
            title = entry.title.strip()
            link = entry.link
            
            # Use published_parsed if available, otherwise current time
            event_date_struct = getattr(entry, 'published_parsed', None)
            if event_date_struct:
                published_date = datetime(*event_date_struct[:6]).strftime("%Y-%m-%d")
            else:
                published_date = datetime.now().strftime("%Y-%m-%d")

            lower_title = title.lower()
            event_type = "News" # Default type

            is_relevant_news = False
            for keyword in config.NEWS_KEYWORDS:
                if keyword in lower_title:
                    is_relevant_news = True
                    break

            if is_relevant_news:
                # Refined company name extraction
                company = "Unknown Company"
                # Pattern 1: "[Company Name] raises/secures/bags/gets/closes [amount]"
                match_raises = re.search(r'([\w\s&\.\-\']+\s+)(?:raises|secures|bags|gets|closes)\s+(?:a)?\s*(?:[\$\₹€£][\d\.]+b?m?)', title, re.IGNORECASE)
                if match_raises:
                    company = match_raises.group(1).strip()
                else:
                    # Pattern 2: "[amount] funding/investment for [Company Name]" or "[Company Name] acquires/merges with [Company Name]"
                    match_for_in = re.search(r'(?:funding|acquisition|investment|merger)\s+(?:for|in|of)\s+([\w\s&\.\-\']+)', title, re.IGNORECASE)
                    if match_for_in:
                        company = match_for_in.group(1).strip()
                    else:
                        match_acquires = re.search(r'([\w\s&\.\-\']+)\s+(?:acquires|merges with|buys)\s+([\w\s&\.\-\']+)', title, re.IGNORECASE)
                        if match_acquires:
                            company = f"{match_acquires.group(1).strip()} acquires {match_acquires.group(2).strip()}"
                        else:
                            # Fallback: Extract first few capitalized words, heuristic
                            potential_company_words = []
                            for word in title.split():
                                clean_word = re.sub(r'[^a-zA-Z0-9]', '', word) # remove punctuation
                                if clean_word and (clean_word[0].isupper() or clean_word.replace('.', '').isalnum()):
                                    potential_company_words.append(word)
                                else:
                                    # Stop at first non-capitalized word unless it's a conjunction
                                    if word.lower() not in ["and", "the", "a", "an", "of", "in", "for"]:
                                        break
                            if potential_company_words:
                                company = " ".join(potential_company_words[:min(5, len(potential_company_words))]).strip()
                                # Remove common corporate suffixes if they are at the end
                                company = re.sub(r'\s+(Ltd|Pvt|Inc|Corp|Group|Media|Capital|Tech|Labs|Solutions|Enterprises|India|Co|Holdings)$', '', company, flags=re.IGNORECASE).strip()
                                # Remove common prefixes
                                company = re.sub(r'^(the|a|an)\s+', '', company, flags=re.IGNORECASE).strip()
                                company = re.sub(r'[\'"]', '', company).strip()
                                # Clean up if it ends up being just a number or very short common word
                                if not company or len(company.split()) < 2 and company.lower() in ["india", "startup", "tech", "digital", "news"]:
                                    company = "Unknown Company"
                            else:
                                company = "Unknown Company"

                # Classify event type more specifically
                if "funding" in lower_title or "fundraise" in lower_title or "investment" in lower_title or "raises" in lower_title:
                    event_type = "Fundraise"
                elif "acquisition" in lower_title or "acquires" in lower_title or "merger" in lower_title or "buys" in lower_title:
                    event_type = "Acquisition"
                elif "ipo" in lower_title or "public offer" in lower_title:
                    event_type = "IPO News"

                location = get_company_location(company)
                
                news_data = {
                    "title": title,
                    "type": event_type,
                    "company": company,
                    "location": location,
                    "link": link,
                    "published_date": published_date,
                    "source": label
                }
                update_history(published_date, "news", news_data)
                logging.info(f"Added news: {title} (from {label})")
            else:
                logging.debug(f"Skipping non-relevant news item from {label}: {title[:70]}...")

    except Exception as e:
        logging.error(f"Error parsing RSS feed {feed_url} ({label}): {e}", exc_info=True)

# --- Background Scrape Task ---
def run_scrapers_in_background():
    """
    Runs all scraping functions in parallel using a ThreadPoolExecutor
    and then sleeps for a defined interval.
    """
    while True:
        logging.info("-" * 50)
        logging.info(f"Starting scheduled background data refresh at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
        
        with ThreadPoolExecutor(max_workers=5) as executor: # Increased max workers for more scrapers
            futures = []
            futures.append(executor.submit(get_moneycontrol_ipos))
            futures.append(executor.submit(get_chittorgarh_ipos_data)) # Scrape detailed IPO data
            futures.append(executor.submit(get_chittorgarh_gmp))       # Scrape GMP
            futures.append(executor.submit(get_ipocentral_subscription)) # Scrape Subscription
            futures.append(executor.submit(parse_rss_feed, config.INC42_FEED_URL, "Inc42"))
            futures.append(executor.submit(parse_rss_feed, config.YOURSTORY_FEED_URL, "YourStory"))

            # Optionally, add a timeout for all scraping to prevent indefinite hangs
            # for f in concurrent.futures.as_completed(futures, timeout=config.REQUEST_TIMEOUT * 5):
            #     try:
            #         f.result() # Get result to raise any exceptions that occurred in the thread
            #     except Exception as exc:
            #         logging.error(f"Scraper generated an exception: {exc}")
            
            # Await all futures to ensure completion or error reporting
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"One of the scraper tasks failed: {exc}", exc_info=True)

        logging.info(f"Background data refresh complete. Next refresh in {config.SCRAPE_INTERVAL_SECONDS} seconds.")
        logging.info("-" * 50)
        time.sleep(config.SCRAPE_INTERVAL_SECONDS)

# --- Flask Routes ---
@app.route('/')
def index():
    """Renders the main dashboard page."""
    # A basic HTML template for display. This would typically be in a separate .html file.
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>IPO & Startup News Tracker</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f4f4f4; color: #333; }
            h1 { color: #0056b3; text-align: center; margin-bottom: 30px; }
            .container { max-width: 1200px; margin: auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .section { margin-bottom: 30px; padding: 15px; border: 1px solid #ddd; border-radius: 5px; background-color: #fcfcfc; }
            .section h2 { margin-top: 0; color: #007bff; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-bottom: 20px; }
            .event-list { list-style: none; padding: 0; }
            .event-item { margin-bottom: 15px; padding: 10px; border-left: 5px solid #007bff; background-color: #e9f7ff; border-radius: 4px; }
            .event-item.news { border-left-color: #28a745; background-color: #e6ffe6; }
            .event-item.ipo { border-left-color: #ffc107; background-color: #fff8e6; }
            .event-item h3 { margin: 0 0 5px 0; color: #0056b3; font-size: 1.2em; }
            .event-item p { margin: 0; font-size: 0.9em; line-height: 1.4; }
            .event-item p strong { color: #555; }
            .event-item a { color: #007bff; text-decoration: none; }
            .event-item a:hover { text-decoration: underline; }
            .refresh-button { display: block; width: 200px; margin: 20px auto; padding: 10px 15px; background-color: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; text-align: center; font-size: 1em; }
            .refresh-button:hover { background-color: #0056b3; }
            .last-updated { text-align: center; font-size: 0.8em; color: #777; margin-top: 20px; }
            .loading-indicator { display: none; text-align: center; margin-top: 20px; font-weight: bold; }
            .date-group { margin-bottom: 25px; border-bottom: 1px dashed #ccc; padding-bottom: 15px; }
            .date-group:last-child { border-bottom: none; }
            .date-group h3 { background-color: #e0e0e0; padding: 8px 15px; border-radius: 5px; margin-top: 0; margin-bottom: 15px; color: #444; }
            .filter-controls { text-align: center; margin-bottom: 20px; }
            .filter-controls button { padding: 8px 15px; margin: 0 5px; background-color: #6c757d; color: white; border: none; border-radius: 5px; cursor: pointer; }
            .filter-controls button.active { background-color: #007bff; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Indian IPO & Startup News Tracker</h1>
            <p class="last-updated">Last Updated: <span id="last-updated-time">Loading...</span></p>
            <div class="filter-controls">
                <button class="filter-btn active" data-filter="all">All Events</button>
                <button class="filter-btn" data-filter="ipos">IPOs</button>
                <button class="filter-btn" data-filter="news">News</button>
                <button class="filter-btn" data-filter="upcoming">Upcoming IPOs</button>
                <button class="filter-btn" data-filter="live">Live IPOs</button>
                <button class="filter-btn" data-filter="listed">Listed IPOs</button>
                <button class="filter-btn" data-filter="fundraise">Fundraise News</button>
                <button class="filter-btn" data-filter="acquisition">Acquisition News</button>
            </div>
            <div id="events-container">
                <p class="loading-indicator" style="display: block;">Loading data...</p>
            </div>
        </div>

        <script>
            const eventsContainer = document.getElementById('events-container');
            const lastUpdatedTime = document.getElementById('last-updated-time');
            let allEventsData = {}; // Store all fetched data
            let currentFilter = 'all';

            function renderEvents(filter = 'all') {
                eventsContainer.innerHTML = ''; // Clear existing content
                lastUpdatedTime.textContent = new Date().toLocaleString();

                const sortedDates = Object.keys(allEventsData).sort().reverse(); // Newest dates first

                if (sortedDates.length === 0) {
                    eventsContainer.innerHTML = '<p style="text-align: center;">No data available yet. Please wait for the first scrape to complete.</p>';
                    return;
                }

                sortedDates.forEach(date => {
                    const dateData = allEventsData[date];
                    let hasVisibleEvents = false;
                    const dateGroup = document.createElement('div');
                    dateGroup.className = 'date-group';
                    dateGroup.innerHTML = `<h3>${date}</h3><ul class="event-list"></ul>`;
                    const ul = dateGroup.querySelector('ul');

                    // Render IPOs
                    if (dateData.ipos && dateData.ipos.length > 0) {
                        dateData.ipos.forEach(ipo => {
                            const isUpcoming = ipo.status === 'Upcoming';
                            const isLive = ipo.status === 'Live';
                            const isListed = ipo.status === 'Listed';

                            let showIpo = false;
                            if (filter === 'all' || filter === 'ipos') {
                                showIpo = true;
                            } else if (filter === 'upcoming' && isUpcoming) {
                                showIpo = true;
                            } else if (filter === 'live' && isLive) {
                                showIpo = true;
                            } else if (filter === 'listed' && isListed) {
                                showIpo = true;
                            }

                            if (showIpo) {
                                hasVisibleEvents = true;
                                const li = document.createElement('li');
                                li.className = 'event-item ipo';
                                li.innerHTML = `
                                    <h3>${ipo.company} <span style="font-size: 0.8em; color: gray;">(${ipo.status})</span></h3>
                                    <p><strong>Open:</strong> ${ipo.open_date} | <strong>Close:</strong> ${ipo.close_date}</p>
                                    <p><strong>Listing:</strong> ${ipo.listing_date || 'N.A.'}</p>
                                    <p><strong>Price Band:</strong> ${ipo.price_band} | <strong>Lot Size:</strong> ${ipo.lot_size}</p>
                                    <p><strong>GMP:</strong> ${ipo.gmp} | <strong>Subscription:</strong> ${ipo.subscription}</p>
                                    <p><strong>Location:</strong> ${ipo.location} | <strong>Source:</strong> ${ipo.source}</p>
                                `;
                                ul.appendChild(li);
                            }
                        });
                    }

                    // Render News
                    if (dateData.news && dateData.news.length > 0) {
                        dateData.news.forEach(news => {
                            let showNews = false;
                            if (filter === 'all' || filter === 'news') {
                                showNews = true;
                            } else if (filter === 'fundraise' && news.type === 'Fundraise') {
                                showNews = true;
                            } else if (filter === 'acquisition' && news.type === 'Acquisition') {
                                showNews = true;
                            }

                            if (showNews) {
                                hasVisibleEvents = true;
                                const li = document.createElement('li');
                                li.className = 'event-item news';
                                li.innerHTML = `
                                    <h3>${news.title} <span style="font-size: 0.8em; color: gray;">(${news.type})</span></h3>
                                    <p><strong>Company:</strong> ${news.company} | <strong>Location:</strong> ${news.location}</p>
                                    <p><strong>Source:</strong> ${news.source} | <a href="${news.link}" target="_blank">Read More</a></p>
                                `;
                                ul.appendChild(li);
                            }
                        });
                    }

                    if (hasVisibleEvents) {
                        eventsContainer.appendChild(dateGroup);
                    }
                });

                if (!eventsContainer.hasChildNodes()) {
                    eventsContainer.innerHTML = '<p style="text-align: center;">No events matching the current filter.</p>';
                }
            }

            // Fetch initial data
            fetch('/history')
                .then(response => response.json())
                .then(data => {
                    allEventsData = data;
                    renderEvents(currentFilter);
                })
                .catch(error => {
                    console.error('Error fetching initial history:', error);
                    eventsContainer.innerHTML = '<p style="text-align: center; color: red;">Failed to load data.</p>';
                });

            // SSE Event Listener
            const eventSource = new EventSource('/stream');
            eventSource.onmessage = function(event) {
                const eventData = JSON.parse(event.data);
                console.log('SSE update:', eventData);

                const date = eventData.date;
                const type = eventData.type; // 'ipos' or 'news'
                const item = eventData.data;

                // Ensure the date entry exists as a dict
                if (!allEventsData[date] || typeof allEventsData[date] !== 'object') {
                    allEventsData[date] = { ipos: [], news: [] };
                }
                if (!allEventsData[date][type]) {
                     allEventsData[date][type] = [];
                }

                let updated = false;
                // Check if item already exists and update it
                if (type === 'ipos') {
                    for (let i = 0; i < allEventsData[date].ipos.length; i++) {
                        // Match IPOs by company and open_date for uniqueness
                        if (allEventsData[date].ipos[i].company === item.company && 
                            allEventsData[date].ipos[i].open_date === item.open_date) {
                            allEventsData[date].ipos[i] = item; // Replace with updated item
                            updated = true;
                            break;
                        }
                    }
                } else if (type === 'news') {
                    for (let i = 0; i < allEventsData[date].news.length; i++) {
                        // Match News by title and link for uniqueness
                        if (allEventsData[date].news[i].title === item.title && 
                            allEventsData[date].news[i].link === item.link) {
                            allEventsData[date].news[i] = item; // Replace with updated item
                            updated = true;
                            break;
                        }
                    }
                }

                if (!updated) {
                    allEventsData[date][type].push(item); // Add new item if not found
                }
                
                renderEvents(currentFilter); // Re-render the display with updated data
            };

            eventSource.onerror = function(err) {
                console.error('EventSource failed:', err);
                eventSource.close();
            };

            // Filter button logic
            document.querySelectorAll('.filter-btn').forEach(button => {
                button.addEventListener('click', function() {
                    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
                    this.classList.add('active');
                    currentFilter = this.dataset.filter;
                    renderEvents(currentFilter);
                });
            });

        </script>
    </body>
    </html>
    """
    return render_template_string(html_template)

@app.route('/history')
def get_history():
    """Endpoint to serve the full history data as JSON."""
    history = load_json_file(config.HISTORY_FILE)
    return Response(json.dumps(history, indent=2, ensure_ascii=False), mimetype='application/json')

@app.route('/stream')
def stream():
    """SSE endpoint for real-time updates."""
    def event_stream():
        messages = announcer.listen()  # Registers a listener
        while True:
            msg = messages.get()  # Blocks until a message is available
            yield msg

    return Response(event_stream(), mimetype="text/event-stream")

# --- Initialize Background Scraper Thread ---
# Use a flag to ensure the scraper runs only once with before_request
scraper_initialized = threading.Event() # Use a threading.Event for thread-safe flag

@app.before_request
def start_background_scraper_once():
    if not scraper_initialized.is_set():
        logging.info("Starting background scraper thread...")
        scraper_thread = threading.Thread(target=run_scrapers_in_background, daemon=True)
        scraper_thread.start()
        scraper_initialized.set() # Set the flag to true after starting

# --- Main Run Block ---
if __name__ == '__main__':
    # For development, you might want to run with debug=True, but disable in production
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True) # debug=True can cause issues with threading reloads
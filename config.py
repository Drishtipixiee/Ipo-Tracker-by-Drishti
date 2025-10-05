import os

# --- File Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the data directory path
DATA_DIR = os.path.join(BASE_DIR, "data")

# Ensure the data directory exists before defining file paths within it
os.makedirs(DATA_DIR, exist_ok=True)

HISTORY_FILE = os.path.join(DATA_DIR, "history.json")
LOCATION_CACHE = os.path.join(DATA_DIR, "location_cache.json")

# --- Scraper Settings ---
# Updated USER_AGENT for better spoofing
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"
REQUEST_TIMEOUT = 10 # seconds for web requests
SCRAPE_INTERVAL_SECONDS = 3600 # 1 hour (3600 seconds)

# --- IPO Source URLs (UPDATED WITH YOUR FINDINGS) ---
MONEYCONTROL_IPO_URL = "https://www.moneycontrol.com/stocks/ipo/upcoming_ipo.php"
CHITTORGARH_MAIN_URL = "https://www.chittorgarh.com/ipo/ipo_dashboard.asp"
CHITTORGARH_SME_URL = "https://www.chittorgarh.com/report/ipo-in-india-list-main-board-sme/82/all/?year=2025"
CHITTORGARH_GMP_URL = "https://www.chittorgarh.com/book-chapter/ipo-grey-market-gmp/28"
IPOCENTRAL_SUBSCRIPTION_URL = "https://www.ipocentral.in/ipo-subscription-status-live/"

# --- RSS Feed URLs ---
INC42_FEED_URL = "https://inc42.com/feed/"
YOURSTORY_FEED_URL = "https://yourstory.com/feed/"

# --- News Keywords for Filtering and Type Classification ---
NEWS_KEYWORDS = [
    "funding", "fundraise", "investment", "acquires", "acquisition", "merger",
    "buys", "seed round", "series a", "series b", "series c", "series d",
    "raises", "closes round", "secures", "bags investment", "ipo", "public offer"
]

# --- Date Formats ---
# Common IPO date formats encountered on scraping sites
IPO_DATE_FORMATS = [
    "%b %d, %Y", # Jul 24, 2025 (Moneycontrol, Chittorgarh)
    "%d %b %Y", # 24 Jul 2025
    "%Y-%m-%d"   # Standard ISO format if converted
]
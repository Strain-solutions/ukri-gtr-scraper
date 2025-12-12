import os
import json
import time
import random
import requests
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION ---
OUTPUT_FILE = "nihr_rag_dataset.jsonl"
CURSOR_FILE = "scraper_cursor.txt"
LOG_FILE = "scraper.log"

# API Settings
API_BASE_URL = "https://nihr.opendatasoft.com/api/records/1.0/search/"
DATASET_NAME = "infonihr-open-dataset"
BATCH_SIZE = 10  # Process 10 items per API call to keep memory low

# Rate Limiting (Seconds)
MIN_SLEEP = 5
MAX_SLEEP = 12

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


def setup_driver():
    """Starts the Headless Chrome Driver"""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # Add a user agent so we look like a normal browser, not a bot
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=opts)


def get_cursor():
    """Reads the starting offset from a file."""
    if os.path.exists(CURSOR_FILE):
        with open(CURSOR_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0


def save_cursor(offset):
    """Saves the current offset to a file."""
    with open(CURSOR_FILE, "w") as f:
        f.write(str(offset))


def fetch_api_batch(start_offset):
    """Fetches a page of results from the NIHR API."""
    params = {
        "dataset": DATASET_NAME,
        "q": "*",  # Wildcard to get everything
        "rows": BATCH_SIZE,
        "start": start_offset,
        "sort": "project_id"  # Sort ensures consistent order for pagination
    }
    try:
        resp = requests.get(API_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("records", []), data.get("nhits", 0)
    except Exception as e:
        logging.error(f"API Error at offset {start_offset}: {e}")
        return [], 0


def check_for_protocol(driver, project_url):
    """
    Scrapes the specific award page to find if a protocol PDF exists.
    Returns: (bool, url_string, title_string)
    """
    if not project_url:
        return False, None, None

    try:
        driver.get(project_url)
        # Wait for the table rows to load (max 5 seconds)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".thread-row"))
            )
        except:
            # If timeout, page might just be empty or different format, but we continue
            pass

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Look for any link containing "protocol"
        for row in soup.select(".thread-row"):
            link = row.select_one("a.thread-link[href]")
            if link:
                text = link.get_text(strip=True).lower()
                href = link.get("href", "")

                if "protocol" in text and href.endswith(".pdf"):
                    # Found one!
                    return True, href, link.get_text(strip=True)

    except Exception as e:
        logging.warning(f"Selenium scrape error for {project_url}: {e}")

    return False, None, None


def save_record(record):
    """Appends a valid record to the JSONL file."""
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# --- MAIN LOOP ---
def main():
    driver = setup_driver()
    current_offset = get_cursor()

    logging.info(f"🚀 Starting Scraper. Resuming from item {current_offset}...")

    try:
        while True:
            # 1. Get a batch of projects from the API
            records, total_hits = fetch_api_batch(current_offset)

            if not records:
                logging.info("No more records found. Job Complete!")
                break

            logging.info(
                f"Processing batch: {current_offset} to {current_offset + len(records)} (Total in DB: {total_hits})")

            # 2. Iterate through the batch
            for item in records:
                fields = item.get("fields", {})
                project_id = fields.get("project_id", "UNKNOWN")
                project_url = fields.get(
                    "funding_and_awards_link") or f"https://fundingawards.nihr.ac.uk/award/{project_id}"

                logging.info(f"Checking {project_id}...")

                # 3. Check for Protocol (The Selenium Part)
                has_protocol, protocol_url, protocol_title = check_for_protocol(driver, project_url)

                if has_protocol:
                    logging.info(f"✅ FOUND PROTOCOL: {project_id}")

                    # 4. Construct the RAG Data Object
                    # We save the REMOTE URL now. Stage 2 will download it later.
                    rag_entry = {
                        "id": project_id,
                        "title": fields.get("project_title"),
                        "abstract_scientific": fields.get("scientific_abstract"),
                        "abstract_plain": fields.get("plain_english_abstract"),
                        "amount": fields.get("award_amount"),
                        "start_date": fields.get("start_date"),
                        "status": fields.get("status"),
                        "program": fields.get("programme"),
                        "url_meta": project_url,
                        "protocol_url": protocol_url,
                        "protocol_filename": f"{project_id}_protocol.pdf",  # Calculated for Stage 2
                        "scraped_at": datetime.now().isoformat()
                    }

                    save_record(rag_entry)
                else:
                    # Optional: Log that we checked it but found nothing
                    # logging.info(f"   No protocol for {project_id}")
                    pass

                # Polite sleep between web requests
                time.sleep(random.randint(MIN_SLEEP, MAX_SLEEP))

            # 5. Update Cursor after finishing the batch
            current_offset += len(records)
            save_cursor(current_offset)

            # Additional safety sleep between API batches
            time.sleep(2)

    except KeyboardInterrupt:
        logging.info("🛑 Paused by user.")
    except Exception as e:
        logging.error(f"💥 CRITICAL ERROR: {e}")
    finally:
        driver.quit()
        logging.info(f"Driver closed. Last position saved: {current_offset}")


if __name__ == "__main__":
    main()

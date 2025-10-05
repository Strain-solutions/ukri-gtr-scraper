import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


# ---------- Utilities ----------

def load_classification_label():
    """Read classification label from classification.txt if present."""
    default = "University of the West of Scotland – INTERNAL"
    try:
        with open("classification.txt", "r", encoding="utf-8") as f:
            txt = f.read().strip()
            return txt or default
    except FileNotFoundError:
        return default


def setup_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    return webdriver.Chrome(options=opts)


def parse_month_year(text):
    """Parse 'Aug 2023' or 'August 2023' → datetime(2023,8,1)."""
    text = (text or "").strip()
    for fmt in ("%b %Y", "%B %Y"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    return None


def choose_best_date(fields):
    """
    Prefer start_date, then award_date, then end_date, then record_timestamp.
    Returns datetime or None.
    """
    candidates = [
        fields.get("start_date"),
        fields.get("award_date"),
        fields.get("end_date"),
        fields.get("record_timestamp"),
    ]
    for c in candidates:
        if not c:
            continue
        # Try ISO date
        try:
            return datetime.strptime(c[:10], "%Y-%m-%d")
        except Exception:
            pass
        # Try full ISO with timezone
        try:
            return datetime.fromisoformat(c.replace("Z", "+00:00"))
        except Exception:
            pass
    return None


def within_range(dt, start_dt, end_dt):
    """Inclusive date range check; returns False if dt is None."""
    if dt is None:
        return False
    return start_dt <= dt <= end_dt


def get_protocol_links_for_award(driver, project_url):
    """
    Load award page; return a list of dicts with protocol links:
    [{'title': str, 'url': str, 'date': datetime or None}, ...]
    Newest first.
    """
    driver.get(project_url)
    # Wait for timeline rows to appear, then fall back to short sleep
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".thread-row"))
        )
    except Exception:
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    items = []
    for row in soup.select(".thread-row"):
        date_div = row.select_one(".thread-date-col")
        date_text = date_div.get_text(strip=True) if date_div else None
        a = row.select_one("a.thread-link[href]")
        if not a:
            continue
        link_text = a.get_text(strip=True)
        href = a.get("href", "")
        if "protocol" in link_text.lower():
            items.append({
                "title": link_text,
                "url": href,
                "date": parse_month_year(date_text)
            })

    items.sort(key=lambda d: (d["date"] or datetime(1900, 1, 1)), reverse=True)
    return items


import concurrent.futures
import threading

def scrape_protocol_info_multithreaded(simplified_records, max_rows, num_threads=4):
    """
    Scrapes protocol information using multiple concurrent Selenium instances.
    Each thread handles its own webdriver and record subset.
    """
    lock = threading.Lock()
    protocol_rows = []
    total_records = len(simplified_records)
    chunk_size = (total_records + num_threads - 1) // num_threads

    if total_records == 0:
        print('No records to return')
        return []

    def worker(subset, worker_id):
        driver = setup_driver()
        local_results = []
        checked = 0
        for row in subset:
            if len(protocol_rows) >= max_rows:
                break
            checked += 1
            aid = row["Award ID"]
            print(f"🧵 Worker {worker_id}: ({checked}/{len(subset)}) Checking {aid}…")
            if not aid or not row["Project URL"]:
                continue
            try:
                protos = get_protocol_links_for_award(driver, row["Project URL"])
            except Exception as e:
                print(f"⚠️ Worker {worker_id} error on {aid}: {e}")
                continue
            if protos:
                newest = protos[0]
                local_results.append({
                    "Award ID": row["Award ID"],
                    "Project Title": row["Project Title"],
                    "Funding Stream": row["Funding Stream"],
                    "Project URL": row["Project URL"],
                    "Protocol Count": len(protos),
                    "Most Recent Protocol URL": newest["url"],
                    "Most Recent Protocol Title": newest["title"],
                    "Most Recent Protocol Date": newest["date"].strftime("%Y-%m") if newest["date"] else ""
                })
        driver.quit()
        with lock:
            protocol_rows.extend(local_results)

    # Split work across threads
    chunks = [simplified_records[i:i + chunk_size] for i in range(0, total_records, chunk_size)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, chunks[i], i+1) for i in range(len(chunks))]
        concurrent.futures.wait(futures)

    print(f"✅ Multi-thread scrape complete. Total protocols found: {len(protocol_rows)}")
    return protocol_rows




def fetch_all_hits(query, page_size=100):
    """Fetch all records for a query from NIHR OpenData (paginated)."""
    base_url = "https://nihr.opendatasoft.com/api/records/1.0/search/"
    dataset = "infonihr-open-dataset"

    # First call to get total hits
    head = requests.get(base_url, params={"dataset": dataset, "q": query, "rows": 0})
    head.raise_for_status()
    total = head.json().get("nhits", 0)

    records, start = [], 0
    while start < total:
        params = {"dataset": dataset, "q": query, "rows": page_size, "start": start}
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()
        chunk = resp.json().get("records", [])
        if not chunk:
            break
        records.extend(chunk)
        start += page_size
    return records, total


# ---------- New Sub-Functions for run_search_to_excel ----------

def get_filtered_api_records(search_term, start_date, end_date):
    """Fetches all records and filters them by the specified date range."""
    # Parse cutoff dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    print(f"🔍 Query: {search_term}")
    print(f"📅 Date window: {start_date} → {end_date} (inclusive)")
    all_records, nhits_total = fetch_all_hits(search_term)
    print(f"   API returned {nhits_total} total hits (before date filtering)")

    simplified = []
    for rec in all_records:
        f = rec.get("fields", {})
        award_id = f.get("project_id") or f.get("project_reference") or ""
        title = f.get("project_title", "")
        stream = f.get("funding_stream") or f.get("programme") or f.get("programme_stream") or ""
        project_url = f.get("funding_and_awards_link") or f"https://fundingawards.nihr.ac.uk/award/{quote(award_id)}"
        best = choose_best_date(f)
        if within_range(best, start_dt, end_dt):
            simplified.append({
                "Award ID": award_id,
                "Project Title": title,
                "Funding Stream": stream,
                "Project URL": project_url,
                "_sort_date": best
            })

    print(f"   After date filter: {len(simplified)} records")
    simplified.sort(key=lambda r: r["_sort_date"], reverse=True)
    return simplified


def scrape_protocol_info(simplified_records, max_rows):
    """Scrapes protocol information for a limited number of records."""
    driver = setup_driver()
    protocol_rows = []
    checked = 0

    for row in simplified_records:
        if len(protocol_rows) >= max_rows:
            break
        checked += 1
        aid = row["Award ID"]
        print(f"   ({checked}/{len(simplified_records)}) Checking protocols for {aid}…")
        if not aid or not row["Project URL"]:
            continue

        try:
            protos = get_protocol_links_for_award(driver, row["Project URL"])
        except Exception as e:
            print(f"      ⚠️ Error scraping {aid}: {e}")
            continue

        if protos:
            newest = protos[0]
            protocol_rows.append({
                "Award ID": row["Award ID"],
                "Project Title": row["Project Title"],
                "Funding Stream": row["Funding Stream"],
                "Project URL": row["Project URL"],
                "Protocol Count": len(protos),
                "Most Recent Protocol URL": newest["url"],
                "Most Recent Protocol Title": newest["title"],
                "Most Recent Protocol Date": newest["date"].strftime("%Y-%m") if newest["date"] else ""
            })

    driver.quit()
    return protocol_rows


def scrape_protocol_info_driver_pool(simplified_records, max_rows, num_drivers=4):
    task_queue = Queue()
    results = []
    results_lock = Lock()

    # Populate queue
    for rec in simplified_records:
        task_queue.put(rec)

    def worker(worker_id):
        driver = setup_driver()
        local_results = []
        while not task_queue.empty() and len(results) < max_rows:
            try:
                row = task_queue.get_nowait()
            except Exception:
                break

            aid = row["Award ID"]
            print(f"🧵 Worker {worker_id} scraping {aid}")
            try:
                protos = get_protocol_links_for_award(driver, row["Project URL"])
                if protos:
                    newest = protos[0]
                    local_results.append({
                        "Award ID": row["Award ID"],
                        "Project Title": row["Project Title"],
                        "Funding Stream": row["Funding Stream"],
                        "Project URL": row["Project URL"],
                        "Protocol Count": len(protos),
                        "Most Recent Protocol URL": newest["url"],
                        "Most Recent Protocol Title": newest["title"],
                        "Most Recent Protocol Date": newest["date"].strftime("%Y-%m") if newest["date"] else ""
                    })
            except Exception as e:
                print(f"⚠️ Worker {worker_id} error on {aid}: {e}")
            finally:
                task_queue.task_done()

        with results_lock:
            results.extend(local_results)
        driver.quit()

    # Spin up driver pool
    threads = []
    for i in range(num_drivers):
        t = Thread(target=worker, args=(i+1,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print(f"✅ Completed scraping with driver pool. {len(results)} protocols found.")
    return results


def write_excel_files(all_records, enriched_records, search_term, max_rows):
    """
    Writes 4 sheets, fully matching the original single-threaded version:
      1. All Checked
      2. Has Protocol Attachments
      3. PI and Co-I Summary
      4. Investigator Counts
    """

    from collections import Counter

    # --- Sheet 1: All Checked (ALL filtered hits, regardless of protocols) ---
    df_all = pd.DataFrame([
        {
            "Award ID": r.get("Award ID", ""),
            "Project Title": r.get("Project Title", ""),
            "Funding Stream": r.get("Funding Stream", ""),
            "Start Date": r.get("Start Date", ""),
            "End Date": r.get("End Date", ""),
            "Project URL": f'=HYPERLINK("{r.get("Project URL", "")}", "Open")',
            "Protocol Count": r.get("Protocol Count", 0),
            "Most Recent Protocol": (
                f'=HYPERLINK("{r["Most Recent Protocol URL"]}", "Protocol")'
                if r.get("Most Recent Protocol URL") else ""
            ),
            "Most Recent Protocol Title": r.get("Most Recent Protocol Title", ""),
            "Most Recent Protocol Date": r.get("Most Recent Protocol Date", ""),
            "Sort Date": (
                r["_sort_date"].strftime("%Y-%m-%d")
                if isinstance(r.get("_sort_date"), datetime) else ""
            )
        }
        for r in enriched_records or all_records  # fallback if no enrichment
    ])

    # --- Sheet 2: Has Protocol Attachments (subset with ≥1 protocol) ---
    with_protocol = [r for r in enriched_records if r.get("Protocol Count", 0) > 0]
    df_with = pd.DataFrame([
        {
            "Award ID": r.get("Award ID", ""),
            "Project Title": r.get("Project Title", ""),
            "Funding Stream": r.get("Funding Stream", ""),
            "Start Date": r.get("Start Date", ""),
            "End Date": r.get("End Date", ""),
            "Project URL": f'=HYPERLINK("{r.get("Project URL", "")}", "Open")',
            "Protocol Count": r.get("Protocol Count", 0),
            "Most Recent Protocol": (
                f'=HYPERLINK("{r["Most Recent Protocol URL"]}", "Protocol")'
                if r.get("Most Recent Protocol URL") else ""
            ),
            "Most Recent Protocol Title": r.get("Most Recent Protocol Title", ""),
            "Most Recent Protocol Date": r.get("Most Recent Protocol Date", "")
        }
        for r in with_protocol[:max_rows]
    ])

    # --- Sheet 3: PI and Co-I Summary (for ALL records) ---
    df_people = pd.DataFrame([
        {
            "Award ID": r.get("Award ID", ""),
            "Project Title": r.get("Project Title", ""),
            "Funding Stream": r.get("Funding Stream", ""),
            "Project URL": f'=HYPERLINK("{r.get("Project URL", "")}", "Open")',
            "Chief Investigators": r.get("Chief Investigators", ""),
            "No. of PIs": r.get("No. of PIs", 0),
            "Co-Investigators": r.get("Co-Investigators", ""),
            "No. of Co-Is": r.get("No. of Co-Is", 0),
        }
        for r in enriched_records
    ])

    # --- Sheet 4: Investigator Counts (name frequency) ---
    all_names = []
    for r in enriched_records:
        if r.get("Chief Investigators"):
            all_names.extend([n.strip() for n in r["Chief Investigators"].split(";") if n.strip()])
        if r.get("Co-Investigators"):
            all_names.extend([n.strip() for n in r["Co-Investigators"].split(";") if n.strip()])

    from collections import Counter
    counts = Counter(all_names)
    df_counts = pd.DataFrame(
        sorted(counts.items(), key=lambda x: x[1], reverse=True),
        columns=["Investigator Name", "Total Count"]
    )

    # --- Save file ---
    safe_term = re.sub(r"[^A-Za-z0-9_]+", "_", search_term).strip("_")
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"nihr_protocol_search_{safe_term}_{today}.xlsx"
    classification = load_classification_label()

    # --- Write Excel ---
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        df_all.to_excel(writer, index=False, sheet_name="All Checked")
        df_with.to_excel(writer, index=False, sheet_name="Has Protocol Attachments")
        df_people.to_excel(writer, index=False, sheet_name="PI and Co-I Summary")
        df_counts.to_excel(writer, index=False, sheet_name="Investigator Counts")

        wb = writer.book

        # Apply header/footer + column autosizing
        for sheet_name, df_curr in [
            ("All Checked", df_all),
            ("Has Protocol Attachments", df_with),
            ("PI and Co-I Summary", df_people),
            ("Investigator Counts", df_counts),
        ]:
            ws = writer.sheets[sheet_name]
            ws.set_header('&C' + classification)
            ws.set_footer('&L' + classification + ' &R&P of &N')

            # Autosize columns safely
            for i, col in enumerate(df_curr.columns):
                col_vals = df_curr[col].tolist()
                # Convert everything to string before measuring length
                max_len = max([len(str(col))] + [len(str(v)) for v in col_vals]) + 2
                ws.set_column(i, i, min(max_len, 80))

    print(f"✅ Excel complete.")
    print(f"🧾 Sheets written:")
    print(f"   • All Checked: {len(df_all)} records")
    print(f"   • Has Protocol Attachments: {len(df_with)} records")
    print(f"   • PI and Co-I Summary: {len(df_people)} records")
    print(f"   • Investigator Counts: {len(df_counts)} investigators")
    print(f"💾 Saved as: {outfile}")


# ---------- Main ----------


def run_search_to_excel(search_term, start_date, end_date, max_rows):
    """
    Hard date cut-offs; two-sheet Excel:
      - 'All Checked': ALL filtered hits (no protocol requirement)
      - 'Has Protocol Attachments': up to max_rows that have ≥1 protocol
    Stops when either max_rows reached OR filtered list exhausted.
    Saves to current directory.
    """
    # Step 1: Fetch and filter API records
    all_records = get_filtered_api_records(search_term, start_date, end_date)

    # Step 2: Scrape for protocol information
    # protocol_records = scrape_protocol_info(all_records, max_rows)

    # with multi threading
    protocol_records = scrape_protocol_info_multithreaded(all_records, max_rows)

    # --- Merge protocol info back into full record list ---
    enriched_records = []
    protocol_index = {r["Award ID"]: r for r in protocol_records}

    for rec in all_records:
        aid = rec.get("Award ID")
        p = protocol_index.get(aid, {})
        enriched_records.append({
            **rec,
            "Protocol Count": p.get("Protocol Count", 0),
            "Most Recent Protocol URL": p.get("Most Recent Protocol URL", ""),
            "Most Recent Protocol Title": p.get("Most Recent Protocol Title",
                                                ""),
            "Most Recent Protocol Date": p.get("Most Recent Protocol Date", ""),
            "Chief Investigators": p.get("Chief Investigators", ""),
            "No. of PIs": p.get("No. of PIs", 0),
            "Co-Investigators": p.get("Co-Investigators", ""),
            "No. of Co-Is": p.get("No. of Co-Is", 0),
        })

    write_excel_files(all_records, enriched_records, search_term, max_rows)


# ---------- Example usage ----------
if __name__ == "__main__":
    search_term = ' "older adults" physical activity '
    start_date = '2022-01-01'
    end_date = '2025-10-01'
    max_rows = 20

    run_search_to_excel(
        search_term=search_term,
        start_date=start_date,
        end_date=end_date,
        max_rows=max_rows
    )
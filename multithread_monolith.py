import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


def load_classification_label():
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
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=opts)


def parse_month_year(text):
    if not text:
        return None
    for fmt in ("%b %Y", "%B %Y"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except Exception:
            pass
    return None


def choose_best_date(fields):
    for key in ["start_date", "award_date", "end_date", "record_timestamp"]:
        val = fields.get(key)
        if not val:
            continue
        try:
            return datetime.strptime(val[:10], "%Y-%m-%d")
        except Exception:
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except Exception:
                pass
    return None


def within_range(dt, start_dt, end_dt):
    return dt and start_dt <= dt <= end_dt


def fetch_all_hits(query, page_size=100):
    base_url = "https://nihr.opendatasoft.com/api/records/1.0/search/"
    dataset = "infonihr-open-dataset"
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


def scrape_award_page(driver, project_url):
    driver.get(project_url)
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".thread-row"))
        )
    except Exception:
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Protocol links
    protocols = []
    for row in soup.select(".thread-row"):
        date_div = row.select_one(".thread-date-col")
        date_text = date_div.get_text(strip=True) if date_div else None
        a = row.select_one("a.thread-link[href]")
        if not a:
            continue
        if "protocol" in a.get_text(strip=True).lower():
            protocols.append({
                "title": a.get_text(strip=True),
                "url": a.get("href", ""),
                "date": parse_month_year(date_text)
            })
    protocols.sort(key=lambda d: (d["date"] or datetime(1900, 1, 1)), reverse=True)

    # Investigator names
    def pick_names(label):
        names = []
        for comp in soup.select(".icon-component, .wide-icon-component-details, .icon-component-details"):
            lbl = comp.select_one(".icon-component-label, .form-label")
            if lbl and label.lower() in lbl.get_text(strip=True).lower():
                for a in comp.select("a.std-link"):
                    nm = a.get_text(strip=True)
                    if nm and nm not in names:
                        names.append(nm)
        return names

    pi_names = pick_names("Chief Investigator")
    coi_names = pick_names("Co-investigators")
    return protocols, pi_names, coi_names


# ---------------------------------------------------------------------
# MAIN FUNCTION - only this changes to multithreading
# ---------------------------------------------------------------------
def run_search_to_excel(search_term, start_date, end_date, max_rows):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    print(f"🔍 Query: {search_term}")
    print(f"📅 Date window: {start_date} → {end_date}")
    all_records, total_hits = fetch_all_hits(search_term)
    print(f"   API returned {total_hits} total hits")

    candidates = []
    for rec in all_records:
        f = rec.get("fields", {})
        best = choose_best_date(f)
        if within_range(best, start_dt, end_dt):
            award_id = f.get("project_id") or f.get("project_reference") or ""
            candidates.append({
                "Award ID": award_id,
                "Project Title": f.get("project_title", ""),
                "Funding Stream": f.get("funding_stream") or f.get("programme") or f.get("programme_stream") or "",
                "Start Date": f.get("start_date"),
                "End Date": f.get("end_date"),
                "Project URL": f.get("funding_and_awards_link") or f"https://fundingawards.nihr.ac.uk/award/{quote(award_id)}",
                "_sort_date": best
            })
    candidates.sort(key=lambda r: r["_sort_date"], reverse=True)
    print(f"   After date filter: {len(candidates)} records")

    # ------------------------------
    # Multithreaded scraping section
    # ------------------------------
    def scrape_batch(batch, wid):
        driver = setup_driver()
        results = []
        for i, row in enumerate(batch, start=1):
            aid = row["Award ID"]
            print(f"🧵 Worker {wid}: [{i}/{len(batch)}] {aid}")
            try:
                protocols, pis, cois = scrape_award_page(driver, row["Project URL"])
            except Exception as e:
                print(f"⚠️ Worker {wid} error {aid}: {e}")
                protocols, pis, cois = [], [], []
            results.append({
                **row,
                "Protocol Count": len(protocols),
                "Most Recent Protocol URL": protocols[0]["url"] if protocols else "",
                "Most Recent Protocol Title": protocols[0]["title"] if protocols else "",
                "Most Recent Protocol Date": (
                    protocols[0]["date"].strftime("%Y-%m") if (protocols and protocols[0]["date"]) else ""
                ),
                "Chief Investigators": "; ".join(pis),
                "No. of PIs": len(pis),
                "Co-Investigators": "; ".join(cois),
                "No. of Co-Is": len(cois)
            })
        driver.quit()
        return results

    n_threads = min(4, max(1, len(candidates)))
    chunk_size = (len(candidates) + n_threads - 1) // n_threads
    chunks = [candidates[i:i + chunk_size] for i in range(0, len(candidates), chunk_size)]

    enriched, lock = [], threading.Lock()
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = {pool.submit(scrape_batch, chunk, i + 1): i for i, chunk in enumerate(chunks)}
        for f in as_completed(futures):
            res = f.result()
            with lock:
                enriched.extend(res)
    print(f"✅ Completed scraping {len(enriched)} records using {n_threads} threads")

    # ------------------------------
    # Excel writing (unchanged)
    # ------------------------------
    with_protocol = [r for r in enriched if r["Protocol Count"] > 0]

    df_all = pd.DataFrame([
        {
            "Award ID": r["Award ID"],
            "Project Title": r["Project Title"],
            "Funding Stream": r["Funding Stream"],
            "Start Date": r["Start Date"],
            "End Date": r["End Date"],
            "Project URL": f'=HYPERLINK("{r["Project URL"]}", "Open")',
            "Protocol Count": r["Protocol Count"],
            "Most Recent Protocol": (
                f'=HYPERLINK("{r["Most Recent Protocol URL"]}", "Protocol")' if r["Most Recent Protocol URL"] else ""
            ),
            "Most Recent Protocol Title": r["Most Recent Protocol Title"],
            "Most Recent Protocol Date": r["Most Recent Protocol Date"],
            "Sort Date": r["_sort_date"].strftime("%Y-%m-%d")
        }
        for r in enriched
    ])

    df_with = pd.DataFrame([
        {
            "Award ID": r["Award ID"],
            "Project Title": r["Project Title"],
            "Funding Stream": r["Funding Stream"],
            "Start Date": r["Start Date"],
            "End Date": r["End Date"],
            "Project URL": f'=HYPERLINK("{r["Project URL"]}", "Open")',
            "Protocol Count": r["Protocol Count"],
            "Most Recent Protocol": f'=HYPERLINK("{r["Most Recent Protocol URL"]}", "Protocol")',
            "Most Recent Protocol Title": r["Most Recent Protocol Title"],
            "Most Recent Protocol Date": r["Most Recent Protocol Date"],
        }
        for r in with_protocol[:max_rows]
    ])

    df_people = pd.DataFrame([
        {
            "Award ID": r["Award ID"],
            "Project Title": r["Project Title"],
            "Funding Stream": r["Funding Stream"],
            "Project URL": f'=HYPERLINK("{r["Project URL"]}", "Open")',
            "Chief Investigators": r["Chief Investigators"],
            "No. of PIs": r["No. of PIs"],
            "Co-Investigators": r["Co-Investigators"],
            "No. of Co-Is": r["No. of Co-Is"],
        }
        for r in enriched
    ])

    from collections import Counter
    all_names = []
    for r in enriched:
        for name_group in ["Chief Investigators", "Co-Investigators"]:
            if r[name_group]:
                all_names.extend([n.strip() for n in r[name_group].split(";") if n.strip()])
    df_counts = pd.DataFrame(
        sorted(Counter(all_names).items(), key=lambda x: x[1], reverse=True),
        columns=["Investigator Name", "Total Count"]
    )

    safe_term = re.sub(r"[^A-Za-z0-9_]+", "_", search_term).strip("_")
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"nihr_protocol_search_{safe_term}_{today}.xlsx"
    classification = load_classification_label()

    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        df_all.to_excel(writer, index=False, sheet_name="All Checked")
        df_with.to_excel(writer, index=False, sheet_name="Has Protocol Attachments")
        df_people.to_excel(writer, index=False, sheet_name="PI and Co-I Summary")
        df_counts.to_excel(writer, index=False, sheet_name="Investigator Counts")

        wb = writer.book
        for sheet_name, df_curr in [
            ("All Checked", df_all),
            ("Has Protocol Attachments", df_with),
            ("PI and Co-I Summary", df_people),
            ("Investigator Counts", df_counts)
        ]:
            ws = writer.sheets[sheet_name]
            ws.set_header('&C' + classification)
            ws.set_footer('&L' + classification + ' &R&P of &N')
            for i, col in enumerate(df_curr.columns):
                max_len = max(len(col), *(len(str(v)) for v in df_curr[col])) + 2
                ws.set_column(i, i, min(max_len, 80))

    print(f"✅ Done. Wrote {len(df_all)} projects to 'All Checked', "
          f"{len(df_with)} to 'Has Protocol Attachments', and "
          f"{len(df_people)} to 'PI and Co-I Summary'.")
    print(f"💾 File saved: {outfile}")


if __name__ == "__main__":

    search_term = '"essential tremor"'
    start_date = '2016-01-01'
    end_date = '2025-10-01'
    max_rows = 20


    run_search_to_excel(
        search_term=search_term,
        start_date=start_date,
        end_date=end_date,
        max_rows=20
    )

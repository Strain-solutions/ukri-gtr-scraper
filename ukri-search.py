#
# import os
# import re
# import time
# import requests
# import pandas as pd
# from bs4 import BeautifulSoup
# from datetime import datetime
# from urllib.parse import quote
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
#
#
# # ---------- Utilities ----------
#
# def load_classification_label():
#     """Read classification label from classification.txt if present."""
#     default = "University of the West of Scotland – INTERNAL"
#     try:
#         with open("classification.txt", "r", encoding="utf-8") as f:
#             txt = f.read().strip()
#             return txt or default
#     except FileNotFoundError:
#         return default
#
#
# def setup_driver():
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--disable-gpu")
#     opts.add_argument("--no-sandbox")
#     return webdriver.Chrome(options=opts)
#
#
# def parse_month_year(text):
#     """Parse 'Aug 2023' or 'August 2023' → datetime(2023,8,1)."""
#     text = (text or "").strip()
#     for fmt in ("%b %Y", "%B %Y"):
#         try:
#             return datetime.strptime(text, fmt)
#         except Exception:
#             pass
#     return None
#
#
# def choose_best_date(fields):
#     """
#     Prefer start_date, then award_date, then end_date, then record_timestamp.
#     Returns datetime or None.
#     """
#     candidates = [
#         fields.get("start_date"),
#         fields.get("award_date"),
#         fields.get("end_date"),
#         fields.get("record_timestamp"),
#     ]
#     for c in candidates:
#         if not c:
#             continue
#         # Try ISO date
#         try:
#             return datetime.strptime(c[:10], "%Y-%m-%d")
#         except Exception:
#             pass
#         # Try full ISO with timezone
#         try:
#             return datetime.fromisoformat(c.replace("Z", "+00:00"))
#         except Exception:
#             pass
#     return None
#
#
# def within_range(dt, start_dt, end_dt):
#     """Inclusive date range check; returns False if dt is None."""
#     if dt is None:
#         return False
#     return start_dt <= dt <= end_dt
#
#
# def get_protocol_links_for_award(driver, project_url):
#     """
#     Load award page; return a list of dicts with protocol links:
#     [{'title': str, 'url': str, 'date': datetime or None}, ...]
#     Newest first.
#     """
#     driver.get(project_url)
#     # Wait for timeline rows to appear, then fall back to short sleep
#     try:
#         WebDriverWait(driver, 6).until(
#             EC.presence_of_element_located((By.CSS_SELECTOR, ".thread-row"))
#         )
#     except Exception:
#         time.sleep(2)
#
#     soup = BeautifulSoup(driver.page_source, "html.parser")
#     items = []
#     for row in soup.select(".thread-row"):
#         date_div = row.select_one(".thread-date-col")
#         date_text = date_div.get_text(strip=True) if date_div else None
#         a = row.select_one("a.thread-link[href]")
#         if not a:
#             continue
#         link_text = a.get_text(strip=True)
#         href = a.get("href", "")
#         if "protocol" in link_text.lower():
#             items.append({
#                 "title": link_text,
#                 "url": href,
#                 "date": parse_month_year(date_text)
#             })
#
#     items.sort(key=lambda d: (d["date"] or datetime(1900, 1, 1)), reverse=True)
#     return items
#
#
# def fetch_all_hits(query, page_size=100):
#     """Fetch all records for a query from NIHR OpenData (paginated)."""
#     base_url = "https://nihr.opendatasoft.com/api/records/1.0/search/"
#     dataset = "infonihr-open-dataset"
#
#     # First call to get total hits
#     head = requests.get(base_url, params={"dataset": dataset, "q": query, "rows": 0})
#     head.raise_for_status()
#     total = head.json().get("nhits", 0)
#
#     records, start = [], 0
#     while start < total:
#         params = {"dataset": dataset, "q": query, "rows": page_size, "start": start}
#         resp = requests.get(base_url, params=params)
#         resp.raise_for_status()
#         chunk = resp.json().get("records", [])
#         if not chunk:
#             break
#         records.extend(chunk)
#         start += page_size
#     return records, total
#
#
# # ---------- Main ----------
#
# def run_search_to_excel(search_term, start_date, end_date, max_rows):
#     """
#     Hard date cut-offs; two-sheet Excel:
#       - 'All Checked': ALL filtered hits (no protocol requirement)
#       - 'Has Protocol Attachments': up to max_rows that have ≥1 protocol
#     Stops when either max_rows reached OR filtered list exhausted.
#     Saves to current directory.
#     """
#     # Parse cutoff dates
#     start_dt = datetime.strptime(start_date, "%Y-%m-%d")
#     end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")
#
#     print(f"🔍 Query: {search_term}")
#     print(f"📅 Date window: {start_date} → {end_date} (inclusive)")
#     all_records, nhits_total = fetch_all_hits(search_term)
#     print(f"   API returned {nhits_total} total hits (before date filtering)")
#
#     # Build simplified rows and apply HARD date filter
#     simplified = []
#     for rec in all_records:
#         f = rec.get("fields", {})
#         award_id = f.get("project_id") or f.get("project_reference") or ""
#         title = f.get("project_title", "")
#         stream = f.get("funding_stream") or f.get("programme") or f.get("programme_stream") or ""
#         project_url = f.get("funding_and_awards_link") or f"https://fundingawards.nihr.ac.uk/award/{quote(award_id)}"
#         best = choose_best_date(f)
#         if within_range(best, start_dt, end_dt):
#             simplified.append({
#                 "Award ID": award_id,
#                 "Project Title": title,
#                 "Funding Stream": stream,
#                 "Project URL": project_url,
#                 "_sort_date": best
#             })
#
#     print(f"   After date filter: {len(simplified)} records")
#
#     # Sort newest-first
#     simplified.sort(key=lambda r: r["_sort_date"], reverse=True)
#
#     # Prepare scraping
#     driver = setup_driver()
#     protocol_rows = []
#     checked = 0
#
#     for row in simplified:
#         if len(protocol_rows) >= max_rows:
#             break
#         checked += 1
#         aid = row["Award ID"]
#         print(f"   ({checked}/{len(simplified)}) Checking protocols for {aid}…")
#         if not aid or not row["Project URL"]:
#             continue
#
#         try:
#             protos = get_protocol_links_for_award(driver, row["Project URL"])
#         except Exception as e:
#             print(f"      ⚠️ Error scraping {aid}: {e}")
#             continue
#
#         if protos:
#             newest = protos[0]
#             protocol_rows.append({
#                 "Award ID": row["Award ID"],
#                 "Project Title": row["Project Title"],
#                 "Funding Stream": row["Funding Stream"],
#                 "Project URL": row["Project URL"],
#                 "Protocol Count": len(protos),
#                 "Most Recent Protocol URL": newest["url"],
#                 "Most Recent Protocol Title": newest["title"],
#                 "Most Recent Protocol Date": newest["date"].strftime("%Y-%m") if newest["date"] else ""
#             })
#
#     driver.quit()
#
#     # --- Build DataFrames for Excel ---
#     df_all = pd.DataFrame([
#         {
#             "Award ID": r["Award ID"],
#             "Project Title": r["Project Title"],
#             "Funding Stream": r["Funding Stream"],
#             "Project URL": f'=HYPERLINK("{r["Project URL"]}", "Open")',
#             "Sort Date": r["_sort_date"].strftime("%Y-%m-%d") if isinstance(r["_sort_date"], datetime) else ""
#         }
#         for r in simplified
#     ])
#
#     df_with = pd.DataFrame([
#         {
#             "Award ID": r["Award ID"],
#             "Project Title": r["Project Title"],
#             "Funding Stream": r["Funding Stream"],
#             "Project URL": f'=HYPERLINK("{r["Project URL"]}", "Open")',
#             "Protocol Count": r["Protocol Count"],
#             "Most Recent Protocol": f'=HYPERLINK("{r["Most Recent Protocol URL"]}", "Protocol")',
#             "Most Recent Protocol Title": r["Most Recent Protocol Title"],
#             "Most Recent Protocol Date": r["Most Recent Protocol Date"]
#         }
#         for r in protocol_rows
#     ])
#
#     # Output path (current dir)
#     safe_name = re.sub(r"[^A-Za-z0-9_]+", "_", search_term).strip("_")
#     outfile = f"nihr_search_{safe_name}.xlsx"
#     classification = load_classification_label()
#
#     # --- Write Excel with two sheets ---
#     with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
#         df_all.to_excel(writer, index=False, sheet_name="All Checked")
#         df_with.to_excel(writer, index=False, sheet_name="Has Protocol Attachments")
#
#         wb = writer.book
#         for sheet_name in ["All Checked", "Has Protocol Attachments"]:
#             ws = writer.sheets[sheet_name]
#             ws.set_header('&C' + classification)
#             ws.set_footer('&L' + classification + ' &R&P of &N')
#
#             df_curr = df_all if sheet_name == "All Checked" else df_with
#             for i, col in enumerate(df_curr.columns):
#                 # sensible width cap
#                 max_len = max(len(col), *(len(str(v)) for v in df_curr[col])) + 2
#                 ws.set_column(i, i, min(max_len, 80))
#
#     print(f"✅ Done. Wrote {len(df_all)} filtered hits to 'All Checked', "
#           f"{len(df_with)} with protocols (max {max_rows}) to 'Has Protocol Attachments'.")
#     print(f"💾 File saved: {outfile}")
#
#
# # ---------- Example usage ----------
# if __name__ == "__main__":
#     search_term = 'Physical activity AND older adults'
#     start_date = '2019-01-01'
#     end_date   = '2025-10-01'
#     max_rows   = 20
#
#     run_search_to_excel(
#         search_term=search_term,
#         start_date=start_date,
#         end_date=end_date,
#         max_rows=max_rows
#     )

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


# ----------------------------
# Helpers
# ----------------------------

def load_classification_label() -> str:
    """Read classification label from classification.txt if available."""
    default = "University of the West of Scotland – INTERNAL"
    try:
        with open("classification.txt", "r", encoding="utf-8") as f:
            txt = f.read().strip()
            return txt or default
    except FileNotFoundError:
        return default


def setup_driver():
    """Create a single headless Chrome driver for the whole run."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    # Tame resource usage a bit
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=opts)


def parse_month_year(text):
    """Parse 'Aug 2023' or 'August 2023' → datetime(2023,8,1)."""
    if not text:
        return None
    text = text.strip()
    for fmt in ("%b %Y", "%B %Y"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    return None


def choose_best_date(fields: dict):
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
        # Try YYYY-MM-DD
        try:
            return datetime.strptime(c[:10], "%Y-%m-%d")
        except Exception:
            pass
        # Try full ISO
        try:
            return datetime.fromisoformat(c.replace("Z", "+00:00"))
        except Exception:
            pass
    return None


def within_range(dt: datetime, start_dt: datetime, end_dt: datetime) -> bool:
    """Inclusive hard cut-off range check."""
    if dt is None:
        return False
    return start_dt <= dt <= end_dt


def fetch_all_hits(query: str, page_size: int = 100):
    """Fetch all records for a query from NIHR OpenData (paginated)."""
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


def scrape_award_page(driver, project_url: str):
    """
    Visit an award page and extract:
      - protocol entries (list of {title, url, date})
      - PI names (list)
      - Co-I names (list)
    """
    driver.get(project_url)
    # Wait for the timeline; if slow, fallback to a short sleep
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".thread-row"))
        )
    except Exception:
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # --- Protocols (timeline “thread-row” items with Protocol link text)
    protocols = []
    for row in soup.select(".thread-row"):
        date_div = row.select_one(".thread-date-col")
        date_text = date_div.get_text(strip=True) if date_div else None
        a = row.select_one("a.thread-link[href]")
        if not a:
            continue
        link_text = a.get_text(strip=True)
        href = a.get("href", "")
        if "protocol" in link_text.lower():
            protocols.append({
                "title": link_text,
                "url": href,
                "date": parse_month_year(date_text)
            })
    protocols.sort(key=lambda d: (d["date"] or datetime(1900, 1, 1)), reverse=True)

    # --- PI & Co-I names (from the icon-component blocks)
    # Robust selection: find label divs then sibling value area
    def pick_names(label_contains: str):
        names = []
        for comp in soup.select(".icon-component, .wide-icon-component-details, .icon-component-details"):
            label = comp.select_one(".icon-component-label, .form-label")
            if not label:
                continue
            if label_contains.lower() in label.get_text(strip=True).lower():
                val = comp.find_next(class_="icon-component-value") or comp.find_next(class_="wide-icon-component-details") or comp
                if val:
                    for a in val.select("a.std-link"):
                        nm = a.get_text(strip=True)
                        if nm:
                            names.append(nm)
        # Deduplicate while preserving order
        seen = set()
        uniq = []
        for n in names:
            if n not in seen:
                seen.add(n)
                uniq.append(n)
        return uniq

    pi_names = pick_names("Chief Investigator")
    coi_names = pick_names("Co-investigators")

    return protocols, pi_names, coi_names


# ----------------------------
# Main entry
# ----------------------------

def run_search_to_excel(search_term: str, start_date: str, end_date: str, max_rows: int):
    """
    - Hard date cut-offs (inclusive).
    - Scrape ALL filtered records (so we can populate PI/Co-I and protocol counts).
    - Create 3 sheets:
        1) All Checked (all filtered projects)
        2) Has Protocol Attachments (subset; up to max_rows by newest)
        3) PI and Co-I Summary (ALL projects)
    - Save to current directory using xlsxwriter with hyperlinks + classification header/footer.
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    print(f"🔎 Query: {search_term}")
    print(f"📅 Date window: {start_date} → {end_date} (inclusive)")

    all_records, total_hits = fetch_all_hits(search_term)
    print(f"   API hits before date filter: {total_hits}")

    # Build candidate list with best date + basics
    candidates = []
    for rec in all_records:
        f = rec.get("fields", {})
        award_id = f.get("project_id") or f.get("project_reference") or ""
        title = f.get("project_title", "")
        stream = f.get("funding_stream") or f.get("programme") or f.get("programme_stream") or ""
        start = f.get("start_date")
        end   = f.get("end_date")
        project_url = f.get("funding_and_awards_link") or f"https://fundingawards.nihr.ac.uk/award/{quote(award_id)}"
        best = choose_best_date(f)
        if within_range(best, start_dt, end_dt):
            candidates.append({
                "Award ID": award_id,
                "Project Title": title,
                "Funding Stream": stream,
                "Start Date": start,
                "End Date": end,
                "Project URL": project_url,
                "_sort_date": best
            })

    print(f"   After date filter: {len(candidates)} records")

    # Sort newest first
    candidates.sort(key=lambda r: r["_sort_date"], reverse=True)

    # Scrape all filtered records (for protocol + PI/Co-I info)
    driver = setup_driver()
    enriched = []
    total = len(candidates)
    for i, row in enumerate(candidates, start=1):
        aid = row["Award ID"]
        url = row["Project URL"]
        print(f"   [{i}/{total}] Scraping {aid} …")
        try:
            protocols, pi_names, coi_names = scrape_award_page(driver, url)
        except Exception as e:
            print(f"      ⚠️ Error scraping {aid}: {e}")
            protocols, pi_names, coi_names = [], [], []

        protocol_count = len(protocols)
        most_recent_protocol_url = protocols[0]["url"] if protocol_count else None
        most_recent_protocol_title = protocols[0]["title"] if protocol_count else ""
        most_recent_protocol_date = (
            protocols[0]["date"].strftime("%Y-%m") if (protocol_count and protocols[0]["date"]) else ""
        )

        enriched.append({
            **row,
            "Protocol Count": protocol_count,
            "Most Recent Protocol URL": most_recent_protocol_url,
            "Most Recent Protocol Title": most_recent_protocol_title,
            "Most Recent Protocol Date": most_recent_protocol_date,
            "Chief Investigators": "; ".join(pi_names) if pi_names else "",
            "No. of PIs": len(pi_names),
            "Co-Investigators": "; ".join(coi_names) if coi_names else "",
            "No. of Co-Is": len(coi_names),
        })

    driver.quit()

    # Build DataFrames
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
            "Sort Date": r["_sort_date"].strftime("%Y-%m-%d") if isinstance(r["_sort_date"], datetime) else ""
        }
        for r in enriched
    ])

    # Subset with protocols; limit to max_rows by newest (already sorted)
    with_protocol = [r for r in enriched if r["Protocol Count"] > 0]
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

    # PI/Co-I Summary for ALL projects
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

    # --- Investigator Counts (new sheet) ---
    from collections import Counter

    all_names = []
    for r in enriched:
        if r["Chief Investigators"]:
            all_names.extend([n.strip() for n in r["Chief Investigators"].split(";") if n.strip()])
        if r["Co-Investigators"]:
            all_names.extend([n.strip() for n in r["Co-Investigators"].split(";") if n.strip()])

    counts = Counter(all_names)
    df_counts = pd.DataFrame(
        sorted(counts.items(), key=lambda x: x[1], reverse=True),
        columns=["Investigator Name", "Total Count"]
    )

    # Output path with date-stamped filename
    safe_term = re.sub(r"[^A-Za-z0-9_]+", "_", search_term).strip("_")
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"nihr_protocol_search_{safe_term}_{today}.xlsx"

    classification = load_classification_label()

    # Write Excel (xlsxwriter)
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        df_all.to_excel(writer, index=False, sheet_name="All Checked")
        df_with.to_excel(writer, index=False, sheet_name="Has Protocol Attachments")
        df_people.to_excel(writer, index=False, sheet_name="PI and Co-I Summary")
        df_counts.to_excel(writer, index=False,
                           sheet_name="Investigator Counts")

        wb = writer.book
        for sheet_name, df_curr in [
            ("All Checked", df_all),
            ("Has Protocol Attachments", df_with),
            ("PI and Co-I Summary", df_people),
        ]:
            ws = writer.sheets[sheet_name]
            # Header/footer classification
            ws.set_header('&C' + classification)
            ws.set_footer('&L' + classification + ' &R&P of &N')
            # Autosize columns (cap width at 80)
            for col_idx, col_name in enumerate(df_curr.columns):
                vals = df_curr[col_name].astype(str).tolist()
                max_len = max([len(col_name)] + [len(v) for v in vals]) + 2
                ws.set_column(col_idx, col_idx, min(max_len, 80))

    print(f"✅ Done. Wrote {len(df_all)} projects to 'All Checked', "
          f"{len(df_with)} to 'Has Protocol Attachments', and "
          f"{len(df_people)} to 'PI and Co-I Summary'.")
    print(f"💾 File saved: {outfile}")


# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    # Adjust these as needed, or wire up CLI args later
    search_term = 'problem gambling'
    start_date  = '2019-01-01'
    end_date    = '2025-10-01'
    max_rows    = 20

    run_search_to_excel(
        search_term=search_term,
        start_date=start_date,
        end_date=end_date,
        max_rows=max_rows
    )

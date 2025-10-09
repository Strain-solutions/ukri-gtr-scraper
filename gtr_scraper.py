import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


# ---------- Utilities ----------

def load_classification_label():
    default = "University of the West of Scotland – INTERNAL"
    try:
        with open("classification.txt", "r", encoding="utf-8") as f:
            return f.read().strip() or default
    except FileNotFoundError:
        return default


def setup_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=opts)


def build_gtr_project_url(ref: str) -> str:
    """
    Encode a GtR grant reference into a valid web URL.
    Example: MR/N003446/2 → https://gtr.ukri.org/projects?ref=MR%2FN003446%2F2
    """
    if not ref:
        return None
    encoded = quote(ref, safe="")
    return f"https://gtr.ukri.org/projects?ref={encoded}"

def fetch_all_hits_gtr(query, max_records=500):
    """
    Fetches records from the GTR API using a keyword search query.
    Corrected version: uses `project.query` param, proper pagination, and version headers.
    """
    base_url = "https://gtr.ukri.org/gtr/api/projects"
    headers = {"Accept": "application/vnd.rcuk.gtr.json-v7"}
    records = []
    page = 1
    fetch_size = 100

    while len(records) < max_records:
        params = {
            "project.query": query,   # Correct param name
            "page": page,             # Page number
            "pageSize": fetch_size    # Number of results per page
        }
        resp = requests.get(base_url, params=params, headers=headers, timeout=30)
        if resp.status_code == 400:
            raise ValueError(f"GTR API rejected the query '{query}' – check parameter format.")
        resp.raise_for_status()

        data = resp.json()
        items = data.get("project", [])
        if not items:
            break

        records.extend(items)
        print(f"📄 Page {page} → got {len(items)} records")
        page += 1
        if len(items) < fetch_size:
            break

    return records[:max_records]

def scrape_project_page(driver, project_url, funded_value=None):
    """
    Visit a GtR project page and extract PI / Co-I / Supervisor / Student names.
    If funded_value was not provided by API, scrape it from the page sidebar.
    """

    # --- Normalize URL ---
    if project_url.startswith("/"):
        project_url = "https://gtr.ukri.org" + project_url
    if not project_url.startswith("http"):
        project_url = "https://gtr.ukri.org/" + project_url.lstrip("/")

    print(f"\n🔗 Visiting: {project_url}")

    # --- Load the page ---
    try:
        driver.get(project_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1.2)
    except Exception as e:
        print(f"❌ Error loading {project_url}: {e}")
        return [], [], funded_value or ""

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    pis, cois = set(), set()

    # --- Page sanity check ---
    title_tag = soup.find("h1", {"id": "gtr-project-title"})
    if title_tag:
        print(f"📘 Page Title: {title_tag.get_text(strip=True)}")
    else:
        print("⚠️ No title found — page may not have loaded correctly")

    # --- 1️⃣ Sidebar parsing ---
    for aside in soup.select(".aside-category"):
        h3 = aside.find("h3")
        if not h3:
            continue
        header = h3.get_text(strip=True).lower()
        name_tag = aside.find("a")
        if not name_tag:
            continue
        name = name_tag.get_text(strip=True)

        if "principal investigator" in header or "supervisor" in header:
            pis.add(name)
            print(f"✅ Sidebar: Found PI/Supervisor: {name}")
        elif "student" in header or "co-investigator" in header:
            cois.add(name)
            print(f"✅ Sidebar: Found Co-I/Student: {name}")

    # --- 2️⃣ People tab parsing ---
    people_tab = soup.select_one("#tabPeople")
    if people_tab:
        print("🔍 Found People tab — parsing people links...")
        for a in people_tab.select("a[href*='/person/']"):
            text = a.get_text(" ", strip=True)
            if not text:
                continue
            lower = text.lower()
            name = text.split("(")[0].strip()

            if "principal investigator" in lower or "supervisor" in lower:
                pis.add(name)
                print(f"✅ People tab: Found PI/Supervisor: {name}")
            elif "co-investigator" in lower or "student" in lower:
                cois.add(name)
                print(f"✅ People tab: Found Co-I/Student: {name}")
            else:
                # fallback
                print(f"⚙️ People tab: Found unlabelled '{name}', defaulting to Co-I")
                cois.add(name)
    else:
        print("⚠️ No People tab found")

    # --- 3️⃣ Regex fallback for rare HTMLs ---
    if not pis and "supervisor" in html.lower():
        for m in re.findall(r"supervisor[^<]*<[^>]*>([^<]+)</a>", html, flags=re.I):
            pis.add(m.strip())
            print(f"🔁 Regex Supervisor: {m.strip()}")

    if not cois and "student" in html.lower():
        for m in re.findall(r"student[^<]*<[^>]*>([^<]+)</a>", html, flags=re.I):
            cois.add(m.strip())
            print(f"🔁 Regex Student: {m.strip()}")

    # --- 4️⃣ Scrape funded value (only if missing) ---
    if not funded_value:
        try:
            fv_header = soup.find("h3", string=lambda s: s and "Funded Value" in s)
            if fv_header:
                fv_tag = fv_header.find_next("strong")
                if fv_tag:
                    funded_value = fv_tag.get_text(strip=True)
                    print(f"💰 Scraped Funded Value: {funded_value}")
        except Exception as e:
            print(f"⚠️ Could not scrape funded value: {e}")

    # --- Summary ---
    if not pis and not cois:
        print("⚠️ No investigators, supervisors, or students found.")
    else:
        print(f"👤 PIs/Supervisors: {len(pis)} | Co-Is/Students: {len(cois)} | 💰 Value: {funded_value or 'N/A'}")

    return sorted(pis), sorted(cois), funded_value or ""
# def scrape_project_page(driver, project_url):
#     """
#     Visit a GtR project page and extract PI / Co-I / Supervisor / Student names.
#     Handles both Overview sidebar and People tab for all project types.
#     """
#     # --- Normalize URL ---
#     if project_url.startswith("/"):
#         project_url = "https://gtr.ukri.org" + project_url
#     if not project_url.startswith("http"):
#         project_url = "https://gtr.ukri.org/" + project_url.lstrip("/")
#
#     print(f"\n🔗 Visiting: {project_url}")
#
#     # --- Load the page ---
#     try:
#         driver.get(project_url)
#         WebDriverWait(driver, 15).until(
#             EC.presence_of_element_located((By.TAG_NAME, "body"))
#         )
#         time.sleep(1.2)
#     except Exception as e:
#         print(f"❌ Error loading {project_url}: {e}")
#         return [], []
#
#     html = driver.page_source
#     soup = BeautifulSoup(html, "html.parser")
#
#     pis, cois = set(), set()
#
#     # --- Page sanity check ---
#     title_tag = soup.find("h1", {"id": "gtr-project-title"})
#     if title_tag:
#         print(f"📘 Page Title: {title_tag.get_text(strip=True)}")
#     else:
#         print("⚠️ No title found — page may not have loaded correctly")
#
#     # --- 1️⃣ Sidebar (overview) check ---
#     for aside in soup.select(".aside-category"):
#         h3 = aside.find("h3")
#         if not h3:
#             continue
#         header = h3.get_text(strip=True).lower()
#         name_tag = aside.find("a")
#         if not name_tag:
#             continue
#         name = name_tag.get_text(strip=True)
#         if "principal investigator" in header or "supervisor" in header:
#             pis.add(name)
#             print(f"✅ Sidebar: Found PI/Supervisor: {name}")
#         elif "student" in header:
#             cois.add(name)
#             print(f"✅ Sidebar: Found Student: {name}")
#
#     # --- 2️⃣ People tab parsing ---
#     people_tab = soup.select_one("#tabPeople")
#     if people_tab:
#         print("🔍 Found People tab — parsing all people links...")
#         for a in people_tab.select("a[href*='/person/']"):
#             text = a.get_text(" ", strip=True)
#             if not text:
#                 continue
#             lower = text.lower()
#             name = text.split("(")[0].strip()
#
#             if "principal investigator" in lower or "supervisor" in lower:
#                 pis.add(name)
#                 print(f"✅ People tab: Found PI/Supervisor: {name}")
#             elif "co-investigator" in lower or "student" in lower:
#                 cois.add(name)
#                 print(f"✅ People tab: Found Co-I/Student: {name}")
#             else:
#                 # fallback — still record named people in this tab
#                 print(f"⚙️  People tab: Found unlabelled person '{name}', defaulting to Co-I")
#                 cois.add(name)
#     else:
#         print("⚠️ No People tab found")
#
#     # --- Fallback regex (rare pages) ---
#     if not pis and "supervisor" in html.lower():
#         print("🔍 Regex fallback for supervisor...")
#         for m in re.findall(r"supervisor[^<]*<[^>]*>([^<]+)</a>", html, flags=re.I):
#             pis.add(m.strip())
#             print(f"✅ Regex matched Supervisor: {m.strip()}")
#
#     if not cois and "student" in html.lower():
#         print("🔍 Regex fallback for student...")
#         for m in re.findall(r"student[^<]*<[^>]*>([^<]+)</a>", html, flags=re.I):
#             cois.add(m.strip())
#             print(f"✅ Regex matched Student: {m.strip()}")
#
#     # --- Summary ---
#     if not pis and not cois:
#         print("⚠️ No investigators, supervisors, or students found.")
#     else:
#         print(f"👤 Total PIs/Supervisors: {len(pis)} | Co-Is/Students: {len(cois)}")
#
#     return sorted(pis), sorted(cois)


def scrape_gtr_batch(batch, wid):
    driver = setup_driver()
    results = []
    for i, row in enumerate(batch, start=1):
        pid = row["Project ID"]
        url = row["Project URL"]
        print(f"🧵 Worker {wid}: [{i}/{len(batch)}] Scraping {pid}")
        try:
            pis, cois, funded_value = scrape_project_page(driver, url, funded_value=row.get("Funded Value"))

        except Exception as e:
            print(f"⚠️ Worker {wid} error {pid}: {e}")
            pis, cois = [], []
        results.append({
            **row,
            "Chief Investigators": "; ".join(pis),
            "No. of PIs": len(pis),
            "Co-Investigators": "; ".join(cois),
            "No. of Co-Is": len(cois),
            "Funded Value": funded_value
        })
    driver.quit()
    return results


def run_gtr_search_to_excel(search_term, max_records=200, threads=4):
    print(f"🔍 Querying GTR for '{search_term}' …")
    all_projects = fetch_all_hits_gtr(search_term, max_records=max_records)
    print(f"📦 Got {len(all_projects)} records from GTR")

    simplified = []
    for p in all_projects:
        pid = p.get("id")
        title = p.get("title", "")
        status = p.get("status", "")
        category = p.get("grantCategory", "")
        funder = p.get("leadFunder", "")

        # --- Funded Value extraction ---
        funded_value = ""
        start = end = ""

        # (A) Try from fund block (common for UKRI)
        fund = p.get("fund", {}) or {}
        if fund:
            start = fund.get("start", "") or ""
            end = fund.get("end", "") or ""
            if fund.get("amountPounds"):
                try:
                    funded_value = f"£{int(fund['amountPounds']):,}"
                except Exception:
                    funded_value = str(fund["amountPounds"])
            elif fund.get("fundedValue"):
                try:
                    funded_value = f"£{int(fund['fundedValue']):,}"
                except Exception:
                    funded_value = str(fund["fundedValue"])

        # (B) Fallback: participantValues (common for EU or Horizon records)
        if not funded_value:
            pv = p.get("participantValues")
            if isinstance(pv, dict) and pv.get("participant"):
                participants = pv["participant"]
                if isinstance(participants, dict):
                    participants = [participants]
                for part in participants:
                    if part.get("role", "").upper() in (
                    "LEAD_PARTICIPANT", "LEAD"):
                        offer = part.get("grantOffer") or part.get(
                            "projectCost")
                        if offer:
                            try:
                                funded_value = f"£{int(round(offer)):,}"
                            except Exception:
                                funded_value = str(offer)
                            break

        # --- Build correct project URL ---
        ref = None
        idents = p.get("identifiers") or p.get("identifier") or {}
        id_list = idents.get("identifier", []) if isinstance(idents,
                                                             dict) else idents
        for ident in id_list:
            if ident.get("type") == "RCUK":
                ref = ident.get("value")
                break

        if ref:
            url = f"https://gtr.ukri.org/projects?ref={quote(ref, safe='')}"
        else:
            url = f"https://gtr.ukri.org/projects/{quote(pid)}"

        simplified.append({
            "Project ID": pid,
            "Title": title,
            "Status": status,
            "Start Date": start,
            "End Date": end,
            "Funder": funder,
            "Project Category": category,
            "Funded Value": funded_value,
            "Project URL": url
        })

    # Split work among threads
    chunk_size = max(1, len(simplified) // threads)
    chunks = [simplified[i:i + chunk_size] for i in range(0, len(simplified), chunk_size)]

    results = []
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = {ex.submit(scrape_gtr_batch, chunk, wid): wid for wid, chunk in enumerate(chunks, 1)}
        for fut in as_completed(futs):
            try:
                results.extend(fut.result())
            except Exception as e:
                print(f"❌ Worker {futs[fut]} failed: {e}")

    # Build DataFrames
    df_all = pd.DataFrame(results)
    df_people = df_all[[
        "Project ID", "Title", "Status", "Project URL",
        "Chief Investigators", "No. of PIs", "Co-Investigators", "No. of Co-Is"
    ]]

    from collections import Counter
    names = []
    for _, row in df_all.iterrows():
        for field in ["Chief Investigators", "Co-Investigators"]:
            if row.get(field):
                names += [n.strip() for n in str(row[field]).split(";") if n.strip()]
    counts = Counter(names)
    df_counts = pd.DataFrame(sorted(counts.items(), key=lambda x: x[1], reverse=True),
                             columns=["Investigator Name", "Total Count"])

    classification = load_classification_label()
    safe_term = re.sub(r"[^A-Za-z0-9_]+", "_", search_term).strip("_")
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"gtr_search_{safe_term}_{today}.xlsx"

    # Write Excel
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        df_all.to_excel(writer, index=False, sheet_name="All Projects")
        df_people.to_excel(writer, index=False, sheet_name="PI and Co-I Summary")
        df_counts.to_excel(writer, index=False, sheet_name="Investigator Counts")

        wb = writer.book
        for sheet_name, df_curr in [("All Projects", df_all), ("PI and Co-I Summary", df_people)]:
            ws = writer.sheets[sheet_name]
            ws.set_header('&C' + classification)
            ws.set_footer('&L' + classification + ' &R&P of &N')
            for i, col in enumerate(df_curr.columns):
                vals = df_curr[col].astype(str).tolist()
                max_len = max([len(col)] + [len(v) for v in vals]) + 2
                ws.set_column(i, i, min(max_len, 80))

    print(f"✅ Done. Wrote {len(df_all)} projects to Excel → {outfile}")


# ---------- Example Usage ----------
"""
Gtr does not recognise AND OR bools but does recognise phrases in double quotes
so for example ' "micro plastics" microplastics '
"""
if __name__ == "__main__":
    run_gtr_search_to_excel('"tremor" diagnosis', max_records=100, threads=4)

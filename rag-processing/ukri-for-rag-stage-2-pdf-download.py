import os
import json
import requests
import time
import logging

# --- CONFIGURATION ---
INPUT_FILE = "nihr_rag_dataset.jsonl"
DOWNLOAD_DIR = "nihr_pdfs"  # This is where your files will actually live
LOG_FILE = "downloader.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)


def download_file(url, local_filename):
    """Downloads a file safely."""
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        logging.error(f"Failed to download {url}: {e}")
        return False


def main():
    # 1. Create the folder if it doesn't exist
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logging.info(f"Created directory: {DOWNLOAD_DIR}")

    # 2. Read the JSONL file
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found! Run the scraper first.")
        return

    logging.info("Starting download process...")

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            record = json.loads(line)

            # Get the details we prepared in Stage 1
            pdf_url = record.get("protocol_url")
            filename = record.get("protocol_filename")
            project_id = record.get("id")

            if not pdf_url or not filename:
                continue

            # Construct the full local path
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            # 3. Check if we already have it (Resume capability)
            if os.path.exists(local_path):
                # Optional: Check file size to ensure it's not a corrupted 0kb file
                if os.path.getsize(local_path) > 0:
                    logging.info(f"Skipping {project_id} (Already exists)")
                    continue

            # 4. Download
            logging.info(f"Downloading {project_id}...")
            success = download_file(pdf_url, local_path)

            if success:
                # Be polite to the server
                time.sleep(1)
            else:
                # If download failed, you might want to log it to a "failed.txt" list
                pass

    logging.info("Download run complete.")


if __name__ == "__main__":
    main()

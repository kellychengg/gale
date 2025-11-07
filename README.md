# USCIS Data Scrapers  
This repository contains two automated Python scrapers that collect, deduplicate, and organize public datasets from the **U.S. Citizenship and Immigration Services (USCIS)**. The first scraper focuses on **H-1B, H-2A, and H-2B employer data hub files**, while the second scraper dynamically crawls the USCIS data library for **I-140, I-129, I-765, I-907, I-485, and EB petition datasets**.  

## Project Structure  
uscis_data/  
├── h1b/                  # H-1B employer data files  
├── h2a/                  # H-2A employer data files  
├── h2b/                  # H-2B employer data files  
├── I-140/                # I-140 (Immigrant Petition for Alien Worker)  
├── I-129/                # I-129 (Nonimmigrant Worker)  
├── I-765/                # I-765 (Employment Authorization)  
├── I-907/                # I-907 (Premium Processing)  
├── I-485/                # I-485 (Adjustment of Status)  
├── EB/                   # Employment-Based Petitions  
├── logs/                 # Rotating logs (auto-cleaned after 90 days)  
├── metadata.json         # File metadata (for Data Hub scraper)  
├── checksums.json        # SHA-256 checksums for deduplication  
├── download_manifest.json # Manifest for Immigration Forms scraper  
└── report_YYYYMMDD.txt   # Generated reports  

## USCIS Data Hub Scraper  
**File:** `uscis_data_hub_scraper.py` — Automates downloads of H-1B, H-2A, and H-2B employer data from USCIS’s archived data hub pages. It uses checksum-based deduplication, metadata tracking, retry logic with exponential backoff, and automatic scheduling.  
**Features:** Scrapes all visa hub pages automatically • Deduplication via SHA-256 • Metadata tracking of filenames, timestamps, and sizes • Retry logic (5 attempts, exponential backoff) • Log cleanup after 90 days • Monthly scheduling at 2:00 AM on the 1st • Writes `SCRAPE_FAILURE.txt` if all retries fail.  
**Run Manually:** `python uscis_data_hub_scraper.py`  
**Automatic Schedule:** `schedule.every().month.at("02:00").do(scheduled_job_with_retry, download_dir)`  
**Example Log:**  
2025-11-06 02:00:00 – INFO – Scraping H1B data…  
2025-11-06 02:01:10 – INFO – Successfully downloaded H-1B_FY2024_Q3.xlsx  
2025-11-06 02:01:11 – INFO – Metadata saved successfully  

## 🧾 USCIS Immigration Forms Data Scraper  
**File:** `uscis_forms_scraper.py` — Crawls the USCIS “Reports and Studies” data library to find and download datasets for I-140, I-129, I-765, I-907, I-485, and EB petitions. It paginates through hundreds of pages, matches keywords, and saves results into organized subfolders.  
**Features:** Dynamic discovery • Keyword-based form matching • Organized subfolders per form • Manifest tracking • Duplicate detection via MD5 • Configurable page limits • Graceful recovery from network errors.  
**Run Manually:** `python uscis_forms_scraper.py`  
**Test Mode:** `scraper.run(max_pages=10, delay_between_downloads=1.0)`  
**Target Forms:** I-140 (Alien Worker) • I-129 (Nonimmigrant Worker – H-1B/L-1/O-1/TN) • I-765 (EAD/OPT/STEM OPT) • I-907 (Premium Processing) • I-485 (Adjustment of Status) • EB (Employment-Based Petitions).  

## Installation  
`pip install requests beautifulsoup4 schedule`  
Optional: `pip install python-crontab`  

## Outputs  
Each run produces downloaded files organized by visa/form type, manifest and metadata JSONs, logs under `/logs`, and report text files summarizing file counts and sizes. 

## Maintenance  
Logs older than 90 days are deleted • Missing/corrupted files are detected and fixed • Repeated failures trigger `SCRAPE_FAILURE.txt`.  

## Recommended Layout  
project_root/  
├── uscis_data_hub_scraper.py  
├── uscis_forms_scraper.py  
├── requirements.txt  
├── README.md  
└── uscis_data/  



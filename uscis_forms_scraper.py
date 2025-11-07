#!/usr/bin/env python3
"""
USCIS Immigration Forms Data Scraper
Dynamically scrapes I-140, I-129, I-765, I-907, I-485, and EB petition data
with pagination through all 1600+ pages and deduplication.
"""

print("Script started - loading libraries...")

import os
import json
import hashlib
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set
from bs4 import BeautifulSoup
import time
import re

class USCISScraper:
    def __init__(self, data_dir: str = "./uscis_data", manifest_file: str = "download_manifest.json"):
        """
        Initialize the scraper with data directory and manifest tracking.
        
        Args:
            data_dir: Directory to store downloaded files
            manifest_file: JSON file to track downloaded files
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.manifest_path = self.data_dir / manifest_file
        self.manifest = self._load_manifest()
        
        self.base_url = "https://www.uscis.gov"
        self.data_page = f"{self.base_url}/tools/reports-and-studies/immigration-and-citizenship-data"
        
        # Form types we're interested in
        self.target_forms = {
            'I-140': ['I-140', 'Immigrant Petition for Alien Worker', 'EB-1', 'EB-2', 'EB-3'],
            'I-129': ['I-129', 'Nonimmigrant Worker', 'H-1B', 'L-1', 'O-1', 'TN'],
            'I-765': ['I-765', 'Employment Authorization', 'EAD', 'OPT', 'STEM OPT'],
            'I-907': ['I-907', 'Premium Processing'],
            'I-485': ['I-485', 'Adjustment of Status'],
            'EB': ['EB Petitions', 'Priority Date', 'I-526', 'I-360', 'Approved EB', 'Visa Bulletin']
        }
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("Initialization complete!")
    
    def _load_manifest(self) -> Dict:
        """Load the manifest of previously downloaded files."""
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        return {
            'last_run': None,
            'downloaded_files': {},
            'stats': {'total_downloads': 0, 'skipped_duplicates': 0}
        }
    
    def _save_manifest(self):
        """Save the manifest to disk."""
        self.manifest['last_run'] = datetime.now().isoformat()
        with open(self.manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)
    
    def _get_file_hash(self, url: str) -> str:
        """Generate a unique hash for a file URL."""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _is_duplicate(self, url: str, size: int = None) -> bool:
        """Check if a file has already been downloaded."""
        file_hash = self._get_file_hash(url)
        if file_hash in self.manifest['downloaded_files']:
            existing = self.manifest['downloaded_files'][file_hash]
            # Check if file still exists locally
            if Path(existing['local_path']).exists():
                return True
        return False
    
    def _matches_target_forms(self, text: str) -> List[str]:
        """Check if text matches any of our target form keywords."""
        text_lower = text.lower()
        matches = []
        
        for form_type, keywords in self.target_forms.items():
            if any(keyword.lower() in text_lower for keyword in keywords):
                matches.append(form_type)
        
        return matches
    
    def get_total_pages(self) -> int:
        """Determine the total number of pages in the data library."""
        print("Determining total number of pages...")
        
        try:
            response = self.session.get(self.data_page, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for pagination info - "1 - 10 of 1667"
            pagination_text = soup.find(text=re.compile(r'\d+\s*-\s*\d+\s+of\s+\d+'))
            
            if pagination_text:
                match = re.search(r'of\s+(\d+)', pagination_text)
                if match:
                    total_items = int(match.group(1))
                    # Assuming 10 items per page
                    total_pages = (total_items + 9) // 10
                    print(f"Found {total_items} total items across ~{total_pages} pages")
                    return total_pages
            
            # Fallback: look for last page link
            pager = soup.find('nav', class_='pager') or soup.find('ul', class_='pager')
            if pager:
                page_links = pager.find_all('a', href=re.compile(r'page=\d+'))
                if page_links:
                    max_page = max([int(re.search(r'page=(\d+)', a['href']).group(1)) 
                                   for a in page_links if re.search(r'page=(\d+)', a['href'])])
                    print(f"Found maximum page number: {max_page}")
                    return max_page + 1  # Pages are 0-indexed
            
            print("Could not determine total pages, will paginate until no more results")
            return 999  # Fallback: try many pages
            
        except Exception as e:
            print(f"Error determining page count: {e}")
            return 999  # Fallback
    
    def discover_data_links(self, max_pages: int = None) -> List[Dict]:
        """
        Discover all relevant data file links from all pages of the USCIS data library.
        
        Args:
            max_pages: Maximum number of pages to scrape (None = all pages)
        
        Returns:
            List of dicts with url, title, form_types, and file_type
        """
        print(f"\n{'='*80}")
        print("DISCOVERING DATA FILES")
        print(f"{'='*80}\n")
        
        if max_pages is None:
            total_pages = self.get_total_pages()
        else:
            total_pages = max_pages
        
        discovered_links = []
        seen_urls = set()
        
        page = 0
        consecutive_empty_pages = 0
        max_empty_pages = 5  # Stop if we hit 5 empty pages in a row
        
        while page < total_pages:
            # Construct paginated URL
            if page == 0:
                url = self.data_page
            else:
                url = f"{self.data_page}?page={page}"
            
            print(f"[Page {page + 1}/{total_pages}] Scanning: {url}")
            
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                page_links = 0
                
                # Find all links on this page
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    link_text = link.get_text(strip=True)
                    
                    # Look for data files
                    if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv', '.pdf', '.zip']):
                        # Check if this link matches our target forms
                        combined_text = f"{link_text} {href}"
                        form_matches = self._matches_target_forms(combined_text)
                        
                        if form_matches:
                            # Make URL absolute
                            if href.startswith('/'):
                                full_url = f"{self.base_url}{href}"
                            elif not href.startswith('http'):
                                full_url = f"{self.base_url}/{href}"
                            else:
                                full_url = href
                            
                            # Deduplicate
                            if full_url not in seen_urls:
                                seen_urls.add(full_url)
                                discovered_links.append({
                                    'url': full_url,
                                    'title': link_text,
                                    'form_types': form_matches,
                                    'file_type': href.split('.')[-1].lower().split('?')[0],
                                    'page': page + 1
                                })
                                page_links += 1
                
                print(f"  Found {page_links} new relevant files on this page (Total: {len(discovered_links)})")
                
                if page_links == 0:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_empty_pages:
                        print(f"\n  No relevant files found on last {max_empty_pages} pages. Stopping pagination.")
                        break
                else:
                    consecutive_empty_pages = 0
                
                # Be polite to the server
                time.sleep(0.5)
                page += 1
                
            except requests.RequestException as e:
                print(f"  [ERROR] Failed to fetch page: {e}")
                page += 1
                continue
        
        print(f"\n{'='*80}")
        print(f"DISCOVERY COMPLETE: Found {len(discovered_links)} unique relevant files")
        print(f"{'='*80}\n")
        
        return discovered_links
    
    def download_file(self, url: str, form_types: List[str], title: str, file_type: str) -> bool:
        """
        Download a file if it hasn't been downloaded before.
        Returns True if downloaded, False if skipped.
        """
        file_hash = self._get_file_hash(url)
        
        # Check for duplicates
        if self._is_duplicate(url):
            print(f"  [SKIP] Already downloaded: {title[:60]}")
            self.manifest['stats']['skipped_duplicates'] += 1
            return False
        
        # Create subdirectory for form type
        primary_form = form_types[0]
        form_dir = self.data_dir / primary_form
        form_dir.mkdir(exist_ok=True)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title[:80]  # Limit length
        filename = f"{safe_title}_{timestamp}.{file_type}"
        filepath = form_dir / filename
        
        try:
            print(f"  [DOWNLOAD] {title[:60]}")
            
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Download with progress
            total_size = int(response.headers.get('content-length', 0))
            with open(filepath, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
            
            file_size = os.path.getsize(filepath)
            print(f"    Saved: {filepath.name} ({file_size / 1024:.1f} KB)")
            
            # Update manifest
            self.manifest['downloaded_files'][file_hash] = {
                'url': url,
                'title': title,
                'form_types': form_types,
                'local_path': str(filepath),
                'download_date': datetime.now().isoformat(),
                'file_size': file_size
            }
            self.manifest['stats']['total_downloads'] += 1
            
            return True
            
        except requests.RequestException as e:
            print(f"    [ERROR] Failed to download: {e}")
            return False
        except Exception as e:
            print(f"    [ERROR] Unexpected error: {e}")
            return False
    
    def run(self, max_pages: int = None, delay_between_downloads: float = 1.0):
        """
        Main execution: discover links and download new files.
        
        Args:
            max_pages: Maximum pages to scrape (None = all pages)
            delay_between_downloads: Seconds to wait between downloads
        """
        print("\n" + "=" * 80)
        print("USCIS IMMIGRATION FORMS DATA SCRAPER")
        print("=" * 80)
        print(f"Data directory: {self.data_dir.absolute()}")
        print(f"Last run: {self.manifest['last_run'] or 'Never'}")
        print(f"Previous downloads: {len(self.manifest['downloaded_files'])}")
        print(f"Target forms: {', '.join(self.target_forms.keys())}")
        print()
        
        # Discover all data links across all pages
        links = self.discover_data_links(max_pages=max_pages)
        
        if not links:
            print("No relevant data files found.")
            return
        
        print(f"\n{'='*80}")
        print(f"DOWNLOADING FILES")
        print(f"{'='*80}\n")
        print(f"Processing {len(links)} files...\n")
        
        # Download each file
        downloaded_count = 0
        for i, link in enumerate(links, 1):
            print(f"[{i}/{len(links)}] {link['title'][:60]}")
            
            success = self.download_file(
                url=link['url'],
                form_types=link['form_types'],
                title=link['title'],
                file_type=link['file_type']
            )
            
            if success:
                downloaded_count += 1
                time.sleep(delay_between_downloads)
            
            # Save manifest periodically (every 10 downloads)
            if downloaded_count > 0 and downloaded_count % 10 == 0:
                self._save_manifest()
        
        # Final manifest save
        self._save_manifest()
        
        # Print summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Files discovered: {len(links)}")
        print(f"New downloads: {downloaded_count}")
        print(f"Skipped (duplicates): {len(links) - downloaded_count}")
        print(f"Total historical downloads: {self.manifest['stats']['total_downloads']}")
        print(f"Data saved to: {self.data_dir.absolute()}")
        print(f"Manifest: {self.manifest_path}")
        print(f"{'='*80}\n")


def main():
    """Main entry point."""
    scraper = USCISScraper(
        data_dir="./uscis_data",
        manifest_file="download_manifest.json"
    )
    
    # Run with all pages (set max_pages=10 to test with first 10 pages only)
    scraper.run(max_pages=None, delay_between_downloads=1.0)


if __name__ == "__main__":
    main()

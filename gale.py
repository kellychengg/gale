#!/usr/bin/env python3
"""
USCIS Immigration Data Scraper
Scrapes specific form data for any fiscal year and quarter
"""

import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
import argparse


class USCISDataScraper:
    """Scraper for USCIS immigration and citizenship data reports"""
    
    BASE_URL = "https://www.uscis.gov"
    DATA_PAGE_URL = f"{BASE_URL}/tools/reports-and-studies/immigration-and-citizenship-data"
    
    # Target form patterns to match
    FORM_PATTERNS = {
        'all_forms': r'All USCIS Application and Petition Form Types',
        'i140_status': r'Form I-140 by Fiscal Year, Quarter and Case Status',
        'i140_preference': r'Form I-140, Receipts and Current Status by Preference and Country',
        'eb_petitions': r'Form I-140, I-360, I-526 Approved EB Petitions Awaiting Visa Final Priority Dates'
    }
    
    def __init__(self, fiscal_year: int, quarter: int, output_dir: str = 'uscis_data'):
        """
        Initialize the scraper
        
        Args:
            fiscal_year: Fiscal year (e.g., 2025)
            quarter: Quarter number (1-4)
            output_dir: Directory to save downloaded files
        """
        self.fiscal_year = fiscal_year
        self.quarter = quarter
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Build the search pattern for this FY/Q combination
        self.fy_q_pattern = rf'Fiscal Year {fiscal_year},?\s*Quarter {quarter}'
    
    def fetch_page(self) -> BeautifulSoup:
        """Fetch and parse the USCIS data page"""
        print(f"Fetching data from: {self.DATA_PAGE_URL}")
        response = requests.get(self.DATA_PAGE_URL)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    
    def find_matching_links(self, soup: BeautifulSoup) -> Dict[str, Optional[Dict[str, str]]]:
        """
        Find all links matching the target forms for the specified FY and Quarter
        
        Returns:
            Dictionary mapping form keys to their info (title, url, filename)
        """
        results = {key: None for key in self.FORM_PATTERNS.keys()}
        
        # Find all links on the page
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            link_text = link.get_text(strip=True)
            href = link['href']
            
            # Check if this link matches our FY/Q pattern
            if not re.search(self.fy_q_pattern, link_text, re.IGNORECASE):
                continue
            
            # Check if this link matches any of our target forms
            for form_key, pattern in self.FORM_PATTERNS.items():
                if results[form_key] is not None:
                    continue  # Already found this form
                
                if re.search(pattern, link_text, re.IGNORECASE):
                    # Extract filename from href
                    filename = href.split('/')[-1]
                    
                    results[form_key] = {
                        'title': link_text,
                        'url': urljoin(self.BASE_URL, href),
                        'filename': filename
                    }
                    print(f"✓ Found: {form_key} - {filename}")
                    break
        
        return results
    
    def download_file(self, url: str, filename: str) -> Path:
        """
        Download a file from the given URL
        
        Args:
            url: URL to download from
            filename: Name to save the file as
            
        Returns:
            Path to the downloaded file
        """
        filepath = self.output_dir / filename
        
        print(f"Downloading: {filename}...", end=' ')
        response = requests.get(url)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"✓ Saved to {filepath}")
        return filepath
    
    def scrape(self) -> Dict[str, Optional[Path]]:
        """
        Main scraping method - finds and downloads all target forms
        
        Returns:
            Dictionary mapping form keys to downloaded file paths
        """
        print(f"\n{'='*60}")
        print(f"USCIS Data Scraper")
        print(f"Fiscal Year {self.fiscal_year}, Quarter {self.quarter}")
        print(f"{'='*60}\n")
        
        # Fetch and parse the page
        soup = self.fetch_page()
        
        # Find matching links
        print("\nSearching for matching forms...")
        matched_links = self.find_matching_links(soup)
        
        # Check what we found
        found_count = sum(1 for v in matched_links.values() if v is not None)
        print(f"\nFound {found_count} of {len(self.FORM_PATTERNS)} target forms")
        
        # Report missing forms
        missing = [k for k, v in matched_links.items() if v is None]
        if missing:
            print(f"\n⚠ Warning: Could not find the following forms:")
            for form_key in missing:
                print(f"  - {form_key}: {self.FORM_PATTERNS[form_key]}")
        
        # Download found files
        downloaded_files = {}
        if found_count > 0:
            print(f"\nDownloading files to: {self.output_dir}")
            print("-" * 60)
            
            for form_key, info in matched_links.items():
                if info is not None:
                    try:
                        filepath = self.download_file(info['url'], info['filename'])
                        downloaded_files[form_key] = filepath
                    except Exception as e:
                        print(f"✗ Error downloading {form_key}: {e}")
                        downloaded_files[form_key] = None
        
        print(f"\n{'='*60}")
        print(f"Scraping complete!")
        print(f"Downloaded {len([p for p in downloaded_files.values() if p])} files")
        print(f"{'='*60}\n")
        
        return downloaded_files


def main():
    """Main function with command-line argument parsing"""
    parser = argparse.ArgumentParser(
        description='Scrape USCIS immigration data for a specific fiscal year and quarter'
    )
    parser.add_argument(
        'fiscal_year',
        type=int,
        help='Fiscal year (e.g., 2025)'
    )
    parser.add_argument(
        'quarter',
        type=int,
        choices=[1, 2, 3, 4],
        help='Quarter number (1-4)'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default=None,
        help='Output directory (default: uscis_fyYYYY_qQ)'
    )
    
    args = parser.parse_args()
    
    # Set default output directory if not specified
    if args.output is None:
        output_dir = f'uscis_fy{args.fiscal_year}_q{args.quarter}'
    else:
        output_dir = args.output
    
    # Create scraper and run
    scraper = USCISDataScraper(
        fiscal_year=args.fiscal_year,
        quarter=args.quarter,
        output_dir=output_dir
    )
    files = scraper.scrape()
    
    # Print results summary
    print("\nDownloaded files:")
    for form_key, filepath in files.items():
        if filepath:
            print(f"  {form_key}: {filepath}")


if __name__ == "__main__":
    main()
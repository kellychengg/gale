"""
USCIS Data Hub Scraper
Automatically downloads H-1B, H-2A, and H-2B employer data files
with deduplication, metadata tracking, retry logic, and periodic cleanup.
"""

import os
import json
import hashlib
import logging
import requests
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Set, Optional
from bs4 import BeautifulSoup
import schedule
import time


# Retry behavior on failure
MAX_RETRIES = 5                # total attempts per scheduled run
BACKOFF_BASE_SECONDS = 60      # 1 minute base
BACKOFF_MAX_SECONDS = 30 * 60  # cap at 30 minutes

# Log retention (cleanup logs older than this)
LOG_RETENTION_DAYS = 90        # Keep 3 months of logs


def setup_logging(log_dir: Path):
    """Configure logging with rotation."""
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f'uscis_scraper_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = None  


class USCISDataScraper:
    """Scraper for USCIS H-1B, H-2A, and H-2B employer data hub files."""
    
    BASE_URLS = {
        'h1b': 'https://www.uscis.gov/archive/h-1b-employer-data-hub-files',
        'h2a': 'https://www.uscis.gov/archive/h-2a-employer-data-hub-files',
        'h2b': 'https://www.uscis.gov/archive/h-2b-employer-data-hub-files'
    }
    
    def __init__(self, download_dir: str = './uscis_data'):
        """
        Initialize the scraper.
        
        Args:
            download_dir: Directory to store downloaded files
        """
        self.download_dir = Path(download_dir)
        self.metadata_file = self.download_dir / 'metadata.json'
        self.checksums_file = self.download_dir / 'checksums.json'
        self.log_dir = self.download_dir / 'logs'
        
        # Setup logging
        global logger
        logger = setup_logging(self.log_dir)
        
        # Create directory structure
        for visa_type in ['h1b', 'h2a', 'h2b']:
            (self.download_dir / visa_type).mkdir(parents=True, exist_ok=True)
        
        # Load existing metadata
        self.metadata = self._load_metadata()
        self.checksums = self._load_checksums()
        
        logger.info(f"Initialized scraper with download directory: {self.download_dir}")
    
    def _load_metadata(self) -> Dict:
        """Load metadata from file."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading metadata: {e}")
                return {}
        return {}
    
    def _load_checksums(self) -> Dict:
        """Load checksums from file."""
        if self.checksums_file.exists():
            try:
                with open(self.checksums_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading checksums: {e}")
                return {}
        return {}
    
    def _save_metadata(self):
        """Atomically save metadata to file."""
        temp_file = self.metadata_file.with_suffix('.tmp')
        try:
            with open(temp_file, 'w') as f:
                json.dump(self.metadata, f, indent=2, sort_keys=True)
            temp_file.replace(self.metadata_file)
            logger.info("Metadata saved successfully")
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
            if temp_file.exists():
                temp_file.unlink()
    
    def _save_checksums(self):
        """Atomically save checksums to file."""
        temp_file = self.checksums_file.with_suffix('.tmp')
        try:
            with open(temp_file, 'w') as f:
                json.dump(self.checksums, f, indent=2, sort_keys=True)
            temp_file.replace(self.checksums_file)
            logger.info("Checksums saved successfully")
        except Exception as e:
            logger.error(f"Error saving checksums: {e}")
            if temp_file.exists():
                temp_file.unlink()
    
    def _calculate_checksum(self, filepath: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _is_duplicate(self, url: str, filepath: Path) -> bool:
        """
        Check if file is a duplicate based on URL and checksum.
        
        Args:
            url: URL of the file
            filepath: Path to the downloaded file
            
        Returns:
            True if file is a duplicate, False otherwise
        """
        # Check if URL already exists in metadata
        if url in self.metadata:
            logger.info(f"URL already in metadata: {url}")
            return True
        
        # Calculate checksum and check for duplicates
        checksum = self._calculate_checksum(filepath)
        if checksum in self.checksums:
            logger.info(f"Duplicate file detected (checksum match): {filepath.name}")
            return True
        
        return False
    
    def _extract_file_links(self, url: str) -> List[Dict[str, str]]:
        """
        Extract download links from a USCIS data hub page.
        
        Args:
            url: URL of the data hub page
            
        Returns:
            List of dictionaries with 'url' and 'name' keys
        """
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = []
            
            # Find all links to CSV or Excel files
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Look for CSV, XLS, XLSX files
                if any(ext in href.lower() for ext in ['.csv', '.xls', '.xlsx', '.zip']):
                    full_url = urljoin(url, href)
                    link_text = link.get_text(strip=True)
                    links.append({
                        'url': full_url,
                        'name': link_text or Path(urlparse(full_url).path).name
                    })
            
            logger.info(f"Found {len(links)} file links on {url}")
            return links
            
        except Exception as e:
            logger.error(f"Error extracting links from {url}: {e}")
            return []
    
    def _download_file(self, url: str, dest_path: Path) -> bool:
        """
        Download a file with progress tracking.
        
        Args:
            url: URL of the file to download
            dest_path: Destination path for the downloaded file
            
        Returns:
            True if download successful, False otherwise
        """
        tmp_path = None
        try:
            # Download to temporary file first
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                response = requests.get(url, stream=True, timeout=60)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        tmp_file.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            if downloaded % (8192 * 100) == 0:  # Log every ~800KB
                                logger.info(f"Downloading {dest_path.name}: {progress:.1f}%")
                
                tmp_path = Path(tmp_file.name)
            
            # Check for duplicates
            if self._is_duplicate(url, tmp_path):
                tmp_path.unlink()
                return False
            
            # Move to final destination atomically
            tmp_path.replace(dest_path)
            
            # Update metadata
            checksum = self._calculate_checksum(dest_path)
            self.metadata[url] = {
                'filename': dest_path.name,
                'download_date': datetime.now().isoformat(),
                'checksum': checksum,
                'size_bytes': dest_path.stat().st_size
            }
            self.checksums[checksum] = url
            
            logger.info(f"Successfully downloaded: {dest_path.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
            return False
    
    def scrape_visa_type(self, visa_type: str) -> int:
        """
        Scrape all files for a specific visa type.
        
        Args:
            visa_type: Type of visa ('h1b', 'h2a', or 'h2b')
            
        Returns:
            Number of new files downloaded
        """
        if visa_type not in self.BASE_URLS:
            logger.error(f"Invalid visa type: {visa_type}")
            return 0
        
        url = self.BASE_URLS[visa_type]
        logger.info(f"Scraping {visa_type.upper()} data from {url}")
        
        # Extract file links
        file_links = self._extract_file_links(url)
        
        if not file_links:
            logger.warning(f"No files found for {visa_type.upper()}")
            return 0
        
        # Download files
        download_count = 0
        dest_dir = self.download_dir / visa_type
        
        for link in file_links:
            # Generate filename from URL if not provided
            filename = Path(urlparse(link['url']).path).name
            if not filename:
                filename = f"{visa_type}_{hashlib.md5(link['url'].encode()).hexdigest()[:8]}.csv"
            
            dest_path = dest_dir / filename
            
            if self._download_file(link['url'], dest_path):
                download_count += 1
        
        return download_count
    
    def scrape_all(self) -> Dict[str, int]:
        """
        Scrape all visa types.
        
        Returns:
            Dictionary with counts of new files per visa type
        """
        results = {}
        
        for visa_type in self.BASE_URLS.keys():
            count = self.scrape_visa_type(visa_type)
            results[visa_type] = count
        
        # Save metadata and checksums
        self._save_metadata()
        self._save_checksums()
        
        logger.info(f"Scraping complete. Results: {results}")
        return results
    
    def cleanup_inconsistencies(self) -> Dict[str, List[str]]:
        """
        Check for and report inconsistencies in downloaded files.
        
        Returns:
            Dictionary of issues found
        """
        issues = {
            'missing_files': [],
            'checksum_mismatches': [],
            'orphaned_files': []
        }
        
        logger.info("Running cleanup and consistency checks...")
        
        # Check if metadata files exist on disk
        for url, meta in list(self.metadata.items()):
            # Find the file (could be in any visa_type folder)
            filepath = None
            for visa_type in self.BASE_URLS.keys():
                potential_path = self.download_dir / visa_type / meta['filename']
                if potential_path.exists():
                    filepath = potential_path
                    break
            
            # Also check root directory
            if not filepath:
                root_path = self.download_dir / meta['filename']
                if root_path.exists():
                    filepath = root_path
            
            # Check if file exists
            if not filepath:
                issues['missing_files'].append(meta['filename'])
                logger.warning(f"Missing file: {meta['filename']}")
                # Remove from metadata
                del self.metadata[url]
                if meta['checksum'] in self.checksums:
                    del self.checksums[meta['checksum']]
                continue
            
            # Verify checksum
            current_checksum = self._calculate_checksum(filepath)
            if current_checksum != meta['checksum']:
                issues['checksum_mismatches'].append(meta['filename'])
                logger.warning(f"Checksum mismatch: {meta['filename']}")
                # Update checksum
                meta['checksum'] = current_checksum
                self.checksums[current_checksum] = url
        
        # Check for orphaned files (files without metadata)
        tracked_files = set(meta['filename'] for meta in self.metadata.values())
        for visa_type in self.BASE_URLS.keys():
            visa_dir = self.download_dir / visa_type
            if visa_dir.exists():
                for file in visa_dir.iterdir():
                    if file.is_file() and file.name not in tracked_files:
                        issues['orphaned_files'].append(str(file.relative_to(self.download_dir)))
                        logger.warning(f"Orphaned file: {file.name}")
        
        # Save updated metadata
        if issues['missing_files'] or issues['checksum_mismatches']:
            self._save_metadata()
            self._save_checksums()
        
        logger.info(f"Cleanup complete. Issues found: {sum(len(v) for v in issues.values())}")
        return issues
    
    def cleanup_old_logs(self):
        """Remove log files older than LOG_RETENTION_DAYS."""
        cutoff_date = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
        removed_count = 0
        
        logger.info(f"Cleaning up logs older than {LOG_RETENTION_DAYS} days...")
        
        for log_file in self.log_dir.glob("*.log"):
            try:
                # Get file modification time
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                
                if mtime < cutoff_date:
                    log_file.unlink()
                    removed_count += 1
                    logger.debug(f"Removed old log: {log_file.name}")
            except Exception as e:
                logger.warning(f"Failed to remove {log_file.name}: {e}")
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} old log files")
        else:
            logger.info("No old logs to remove")
    
    def generate_report(self) -> str:
        """
        Generate a summary report of downloaded files.
        
        Returns:
            Report as a string
        """
        report_lines = [
            "=" * 60,
            "USCIS Data Hub Scraper Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 60,
            ""
        ]
        
        # Count files by visa type
        for visa_type in self.BASE_URLS.keys():
            count = sum(1 for meta in self.metadata.values() 
                       if visa_type in meta['filename'].lower())
            total_size = sum(meta['size_bytes'] for meta in self.metadata.values() 
                           if visa_type in meta['filename'].lower())
            
            report_lines.append(f"{visa_type.upper()}: {count} files ({total_size / 1024 / 1024:.2f} MB)")
        
        report_lines.extend([
            "",
            f"Total files: {len(self.metadata)}",
            f"Total size: {sum(m['size_bytes'] for m in self.metadata.values()) / 1024 / 1024:.2f} MB",
            "=" * 60
        ])
        
        report = "\n".join(report_lines)
        logger.info("\n" + report)
        
        # Save report to file
        report_file = self.download_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        return report


def send_failure_notification(download_dir: Path, attempt: int):
    """
    Send notification about persistent scraper failure.
    Creates a failure file that monitoring systems can detect.
    """
    message = (
        f"ALERT: USCIS scraper failed after {attempt} attempts\n"
        f"Time: {datetime.now()}\n"
        f"Check logs in: {download_dir / 'logs'}"
    )
    
    logger.critical("="*60)
    logger.critical("FAILURE NOTIFICATION")
    logger.critical(message)
    logger.critical("="*60)
    
    # Write to a failure file that monitoring can pick up
    failure_file = download_dir / "SCRAPE_FAILURE.txt"
    try:
        with open(failure_file, 'w') as f:
            f.write(message)
        logger.info(f"Failure flag written to: {failure_file}")
    except Exception as e:
        logger.error(f"Failed to write failure flag: {e}")


def scheduled_job_with_retry(download_dir: str = './uscis_data'):
    """
    Job to run on schedule with retry logic.
    
    Args:
        download_dir: Directory to store downloaded files
    """
    logger_temp = logging.getLogger(__name__)
    logger_temp.info("=" * 60)
    logger_temp.info("Starting scheduled scraping job")
    logger_temp.info("=" * 60)
    
    attempt = 0
    success = False
    
    while attempt < MAX_RETRIES and not success:
        attempt += 1
        
        try:
            logger_temp.info(f"Attempt {attempt}/{MAX_RETRIES}")
            
            scraper = USCISDataScraper(download_dir=download_dir)
            
            # Scrape all data sources
            results = scraper.scrape_all()
            
            # Run cleanup
            issues = scraper.cleanup_inconsistencies()
            
            # Cleanup old logs
            scraper.cleanup_old_logs()
            
            # Generate report
            scraper.generate_report()
            
            # Log summary
            logger.info(f"Job complete - New files: {sum(results.values())}, Issues: {sum(len(v) for v in issues.values())}")
            
            # Clear any previous failure flags
            failure_file = Path(download_dir) / "SCRAPE_FAILURE.txt"
            if failure_file.exists():
                failure_file.unlink()
                logger.info("Cleared previous failure flag")
            
            success = True
            
        except Exception as e:
            logger_temp.error(f"Attempt {attempt}/{MAX_RETRIES} failed with error: {e}")
            logger_temp.exception("Full traceback:")
            
            # Calculate backoff for next attempt
            if attempt < MAX_RETRIES:
                sleep_seconds = min(
                    BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), 
                    BACKOFF_MAX_SECONDS
                )
                logger_temp.warning(
                    f"Retrying in {sleep_seconds} seconds..."
                )
                time.sleep(sleep_seconds)
    
    if not success:
        logger_temp.error(f"All {MAX_RETRIES} retry attempts exhausted")
        send_failure_notification(Path(download_dir), attempt)


def main():
    """Main entry point with scheduler."""
    print("=" * 60)
    print("USCIS Data Hub Scraper with Retry Logic")
    print("=" * 60)
    print("Schedule: Monthly on the 1st at 2:00 AM")
    print("Retry Logic: Up to 5 attempts with exponential backoff")
    print("Log Retention: 90 days")
    print("=" * 60)
    
    download_dir = './uscis_data'
    
    print("\nRunning initial scrape on startup...")
    scheduled_job_with_retry(download_dir)
    
    # Schedule monthly runs 
    schedule.every().month.at("02:00").do(scheduled_job_with_retry, download_dir)
    
    print(f"\nScheduler active - will run monthly on the 1st at 2:00 AM")
    print("Press Ctrl+C to stop\n")
    
    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(3600)  # Check every hour
    except KeyboardInterrupt:
        print("\nScheduler stopped by user")


if __name__ == "__main__":
    main()
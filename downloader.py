"""
This downloads all data from ZENODO that is in the IODP community. The data is 
downloaded as is and is not parsed. This will create a /data/ folder if it
does not already exist.

You will need a Zenodo API key which can be created via the Zenodo Applications
link when you login. Once created you should export the key like so:

export ZENODO_API_KEY='<your key that you copied from zenodo>'
"""

import os
import sys
import requests
import json
from pathlib import Path
import time
import argparse

# Create data directory if it doesn't exist
data_dir = Path('data')
data_dir.mkdir(exist_ok=True)

# Get Zenodo API key from environment
ZENODO_API_KEY = os.getenv('ZENODO_API_KEY')
if not ZENODO_API_KEY:
    print("Error: ZENODO_API_KEY environment variable not set")
    print("Please set it with: export ZENODO_API_KEY='your_api_key_here'")
    sys.exit(1)

# IODP community identifier
IODP_COMMUNITY_ID = "c2f742bc-82f9-4f1e-911e-d1542e88cad7"
BASE_URL = "https://zenodo.org/api"

# Debug mode - can be set via environment variable or command line
DEBUG = os.getenv('DEBUG', 'false').lower() in ('true', '1', 'yes')

def get_iodp_records(debug=False):
    """Fetch all records from the IODP community."""
    if debug:
        print("Fetching IODP community records (DEBUG MODE - limited to 2 records)...")
    else:
        print("Fetching IODP community records...")
    
    all_records = []
    page = 1
    size = 2 if debug else 50  # Limit to 2 records in debug mode
    
    while True:
        url = f"{BASE_URL}/records"
        params = {
            'communities': IODP_COMMUNITY_ID,
            'page': page,
            'size': size,
            'access_token': ZENODO_API_KEY
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching records: {response.status_code}")
            print(f"Response: {response.text}")
            break
            
        data = response.json()
        records = data.get('hits', {}).get('hits', [])
        
        if not records:
            break
            
        all_records.extend(records)
        print(f"Fetched page {page}, total records so far: {len(all_records)}")
        
        # In debug mode, stop after getting 2 records
        if debug and len(all_records) >= 2:
            all_records = all_records[:2]  # Ensure exactly 2 records
            break
        
        # Check if there are more pages
        total = data.get('hits', {}).get('total', 0)
        if len(all_records) >= total:
            break
            
        page += 1
        time.sleep(0.1)  # Be nice to the API
    
    print(f"Total IODP records found: {len(all_records)}")
    return all_records

def download_file(file_info, record_id, record_title):
    """Download a single file from a record."""
    filename = file_info['key']
    download_url = file_info['links']['self']
    file_size = file_info.get('size', 0)
    
    # Create subdirectory for this record
    record_dir = data_dir / f"record_{record_id}"
    record_dir.mkdir(exist_ok=True)
    
    # Sanitize record title for directory name
    safe_title = "".join(c for c in record_title if c.isalnum() or c in (' ', '-', '_')).rstrip()
    title_dir = record_dir / safe_title[:100]  # Limit length
    title_dir.mkdir(exist_ok=True)
    
    file_path = title_dir / filename
    
    # Skip if file already exists
    if file_path.exists() and file_path.stat().st_size == file_size:
        print(f"  Skipping {filename} (already exists)")
        return True
    
    print(f"  Downloading {filename} ({file_size} bytes)...")
    
    try:
        response = requests.get(download_url, params={'access_token': ZENODO_API_KEY}, stream=True)
        response.raise_for_status()
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"  ✓ Downloaded {filename}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error downloading {filename}: {e}")
        return False

def download_record_data(record, debug=False):
    """Download all files from a single record."""
    record_id = record['id']
    title = record['metadata'].get('title', 'Unknown Title')
    files = record.get('files', [])
    
    # In debug mode, limit to 2 files per record
    if debug and len(files) > 2:
        files = files[:2]
        print(f"\nProcessing record {record_id}: {title} (DEBUG MODE - limiting to 2 files)")
    else:
        print(f"\nProcessing record {record_id}: {title}")
    
    print(f"Files to download: {len(files)}")
    
    if not files:
        print("  No files to download")
        return
    
    success_count = 0
    for file_info in files:
        if download_file(file_info, record_id, title):
            success_count += 1
    
    print(f"  Downloaded {success_count}/{len(files)} files successfully")

def save_metadata(records):
    """Save record metadata to JSON file."""
    metadata_file = data_dir / "iodp_metadata.json"
    
    # Extract relevant metadata
    metadata = []
    for record in records:
        meta = {
            'id': record['id'],
            'title': record['metadata'].get('title'),
            'description': record['metadata'].get('description'),
            'creators': record['metadata'].get('creators', []),
            'publication_date': record['metadata'].get('publication_date'),
            'doi': record.get('doi'),
            'files': [{'key': f['key'], 'size': f.get('size', 0)} for f in record.get('files', [])]
        }
        metadata.append(meta)
    
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Metadata saved to {metadata_file}")

def main():
    """Main function to download all IODP data from Zenodo."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Download IODP data from Zenodo')
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug mode (download only 2 records with 2 files each)')
    args = parser.parse_args()
    
    # Override environment DEBUG setting if command line flag is provided
    debug_mode = args.debug or DEBUG
    
    print("IODP Zenodo Downloader")
    print("======================")
    if debug_mode:
        print("🐛 DEBUG MODE ENABLED - Limited downloads for testing")
    
    # Fetch all records
    records = get_iodp_records(debug=debug_mode)
    
    if not records:
        print("No records found. Exiting.")
        return
    
    # Save metadata
    save_metadata(records)
    
    # Download all files
    if debug_mode:
        print(f"\nStarting download of files from {len(records)} records (DEBUG MODE)...")
    else:
        print(f"\nStarting download of files from {len(records)} records...")
    
    for i, record in enumerate(records, 1):
        print(f"\n[{i}/{len(records)}] ", end="")
        download_record_data(record, debug=debug_mode)
    
    print(f"\n✓ Download complete! Data saved to {data_dir.absolute()}")

if __name__ == "__main__":
    main() 
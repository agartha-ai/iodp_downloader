# IODP Zenodo Downloader

Downloads all data from the [IODP community on Zenodo](https://zenodo.org/communities/iodp).

## Setup

1. Get a [Zenodo API key](https://zenodo.org/account/settings/applications/)
2. Set environment variable:
   ```bash
   export ZENODO_API_KEY='your_api_key_here'
   ```

## Usage

```bash
# Download all IODP data
python downloader.py

# Test with limited downloads (2 records, 2 files each)
python downloader.py --debug
```

## Output

- Data saved to `./data/` directory
- Organized by record ID and title
- Metadata saved to `iodp_metadata.json`
- Resumes interrupted downloads automatically
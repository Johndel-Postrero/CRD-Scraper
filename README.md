# CRD Scraper

A Python web scraper for extracting reaction data from the Chemical Reaction Database (CRD) at kmt.vander-lingen.nl.

## Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

   Or install manually:
   ```bash
   pip install requests beautifulsoup4
   ```

2. **(Optional) Create a virtual environment:**
   ```bash
   python -m venv venv
   # Windows
   venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```

## How to Run

**Basic usage (scrapes all entries from archive):**
```bash
python crd_scraper.py
```

**With options:**
```bash
python crd_scraper.py --max-datasets 10 --max-reactions-per-dataset 50
```

**Custom archive URL:**
```bash
python crd_scraper.py --archive-url "https://kmt.vander-lingen.nl/archive"
```

## Command Line Arguments

- `--archive-url`: Archive page URL (default: https://kmt.vander-lingen.nl/archive)
- `--max-datasets`: Maximum number of datasets to scrape (default: None, scrapes all)
- `--max-reactions-per-dataset`: Maximum reactions to pull per dataset (default: 100)

## Output

Data is saved to `scraped_data.json` (auto-saved every 10 reactions). The file structure:
```json
{
  "dataset_id": {
    "/id/reaction_id": {
      "raw_inputs": [...],
      "outcomes": [...]
    }
  }
}
```

## Features

- Scrapes the archive page to find all reaction data entries
- Extracts SMILES strings from reaction data pages
- Parses reaction strings (reactants>solvents>products)
- Saves data in JSON format similar to ORD scraper
- Progress is saved incrementally

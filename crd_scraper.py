import requests
from bs4 import BeautifulSoup
import json
import re
import time
import argparse
import sys
from urllib.parse import urljoin

if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


class CRDScraper:
    def __init__(self, base_url="https://kmt.vander-lingen.nl", json_file='scraped_data.json'):
        self.base_url = base_url
        self.json_file = json_file
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        })
        self.scraped_data = {
            'datasets': []
        }
        self.existing_data = {}
    
    def _make_request_with_retry(self, url, max_retries=3, retry_delay=2):
        """Make HTTP request with retry logic for connection errors"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if hasattr(e.response, 'status_code') and e.response.status_code == 404:
                    return None
                raise
            except (requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    time.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ['connection', 'disconnected', 'aborted', 'remote']):
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        time.sleep(wait_time)
                    else:
                        raise
                else:
                    raise
        return None
    
    def scrape_archive_page(self, archive_url):
        """Step 1: Scrape the archive page to get all reaction data links"""
        try:
            response = self._make_request_with_retry(archive_url)
            if not response:
                return []
            html_content = response.text
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            entries = self._extract_entries_by_year(soup, archive_url)
            return entries
            
        except Exception as e:
            return []
    
    def _extract_entries_by_year(self, soup, archive_url):
        """Extract entries organized by year sections"""
        entries = []
        
        for li in soup.find_all(['li', 'div', 'p']):
            text = li.get_text(' ', strip=True)
            if not text or 'reaction data' not in text.lower():
                continue
            
            header_phrases = [
                'you have reached the archives',
                'chemical reaction database',
                'archives of the'
            ]
            for phrase in header_phrases:
                if phrase in text.lower():
                    text = re.sub(rf'.*?{re.escape(phrase)}.*?(?=\w)', '', text, flags=re.IGNORECASE).strip()
                    text = re.sub(r'^\d{4}\s*', '', text).strip()
                    break
            
            if not text or 'reaction data' not in text.lower():
                continue
            
            links = li.find_all('a', href=True)
            reaction_data_url = None
            doi_url = None
            
            for link in links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()
                
                if 'reaction data' in link_text or ('reaction' in href.lower() and 'data' in href.lower()):
                    reaction_data_url = urljoin(self.base_url, href)
                
                if link_text == 'doi' or 'doi' in href.lower():
                    doi_url = href
                    doi_match = re.search(r'doi[=/]([0-9]+\.[^/\s&]+)', href)
                    if doi_match:
                        doi_url = doi_match.group(1)
            
            if not reaction_data_url:
                for link in links:
                    href = link.get('href', '')
                    if 'reaction' in href.lower() or 'data' in href.lower():
                        reaction_data_url = urljoin(self.base_url, href)
                        break
            
            if not reaction_data_url:
                continue
            
            match = re.search(r'^([^,]+),\s*([^0-9]+?)\s*(\d{4})\s*reaction data', text, re.IGNORECASE)
            if match:
                entry = {
                    'compound_name': match.group(1).strip(),
                    'authors': match.group(2).strip(),
                    'year': match.group(3).strip(),
                    'reaction_data_url': reaction_data_url,
                }
                if doi_url and isinstance(doi_url, str) and not doi_url.startswith('http'):
                    entry['doi'] = doi_url
                entries.append(entry)
            else:
                compound_text = text.split(',')[0].strip() if ',' in text else text[:50]
                header_phrases = [
                    'you have reached the archives',
                    'chemical reaction database',
                    'archives of the'
                ]
                for phrase in header_phrases:
                    if phrase in compound_text.lower():
                        parts = compound_text.split(phrase, 1)
                        if len(parts) > 1:
                            compound_text = parts[1].strip()
                            compound_text = re.sub(r'^\d{4}\s*', '', compound_text).strip()
                        break
                
                entry = {
                    'compound_name': compound_text if compound_text else 'Unknown',
                    'authors': '',
                    'year': '',
                    'reaction_data_url': reaction_data_url,
                }
                if doi_url and isinstance(doi_url, str) and not doi_url.startswith('http'):
                    entry['doi'] = doi_url
                entries.append(entry)
        
        return entries
    
    def scrape_reaction_data_page(self, reaction_data_url, entry_info=None):
        all_reactions = []
        current_url = reaction_data_url
        page_num = 0
        
        try:
            while current_url:
                response = self._make_request_with_retry(current_url)
                if not response:
                    break
                html_content = response.text
                
                reactions = self._extract_reactions_from_page(html_content)
                
                if len(reactions) == 0:
                    break
                
                all_reactions.extend(reactions)
                
                next_url = self._find_next_page_link(html_content, current_url)
                if next_url and next_url != current_url:
                    current_url = next_url
                    page_num += 1
                    time.sleep(0.5)
                else:
                    break
            
            doi = self._extract_dataset_id_from_url(reaction_data_url)
            if entry_info:
                entry_doi = self._extract_doi_from_entry(entry_info)
                if entry_doi:
                    doi = entry_doi
            
            dataset_info = {
                'dataset_id': doi,
                'dataset_name': entry_info.get('compound_name', '') if entry_info else '',
                'authors': entry_info.get('authors', '') if entry_info else '',
                'year': entry_info.get('year', '') if entry_info else '',
                'url': reaction_data_url,
                'reaction_samples': all_reactions,
                'reaction_details': []
            }
            
            return dataset_info, all_reactions
            
        except Exception as e:
            return None, []
    
    def _find_next_page_link(self, html, current_url):
        """Find the Next page link in HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            if link_text in ['next', '>', '»']:
                href = link.get('href')
                if href:
                    return urljoin(current_url, href)
        
        for button in soup.find_all(['button', 'a']):
            button_text = button.get_text(strip=True).lower()
            if button_text == 'next':
                onclick = button.get('onclick', '')
                href = button.get('href', '')
                if href:
                    return urljoin(current_url, href)
                if onclick:
                    match = re.search(r"['\"]([^'\"]+)['\"]", onclick)
                    if match:
                        return urljoin(current_url, match.group(1))
        
        return None
    
    def _extract_dataset_id_from_url(self, url):
        """Extract DOI from URL"""
        match = re.search(r'/doi/([^/]+/[^/]+)', url)
        if match:
            return match.group(1)
        
        match = re.search(r'/doi/([^/]+)', url)
        if match:
            return match.group(1)
        
        match = re.search(r'doi[=/]([0-9]+\.[^/\s&]+)', url)
        if match:
            return match.group(1)
        
        return f"dataset_{hash(url) % 1000000}"
    
    def _extract_doi_from_entry(self, entry_info):
        """Extract DOI from entry info or URL"""
        if 'doi' in entry_info:
            doi = entry_info['doi']
            if doi and not doi.startswith('http'):
                return doi
        
        url = entry_info.get('reaction_data_url', '')
        if url:
            doi = self._extract_dataset_id_from_url(url)
            if doi and not doi.startswith('dataset_'):
                return doi
        
        for key, value in entry_info.items():
            if 'url' in key.lower() and isinstance(value, str):
                doi = self._extract_dataset_id_from_url(value)
                if doi and not doi.startswith('dataset_'):
                    return doi
        
        return None
    
    def _extract_reactions_from_page(self, html):
        """Extract reaction SMILES strings from HTML, maintaining order with reaction-pane divs"""
        reactions = []
        soup = BeautifulSoup(html, 'html.parser')
        
        reaction_panes = soup.find_all('div', id=re.compile(r'reaction-pane-\d+'))
        
        for pane in reaction_panes:
            smiles_button = pane.find('button', attrs={'data-reaction-smiles': True})
            if smiles_button:
                reaction_str = smiles_button.get('data-reaction-smiles', '')
                if reaction_str:
                    reaction_str = reaction_str.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&')
                    if reaction_str not in reactions:
                        reactions.append(reaction_str)
        
        if reactions:
            return reactions
        
        for match in re.finditer(r"reactions\.push\(\s*['\"]([\s\S]*?)['\"]\s*\)", html):
            reaction_str = match.group(1)
            if reaction_str:
                reactions.append(reaction_str)
        
        for match in re.finditer(r"data-reaction-smiles\s*=\s*['\"]([^'\"]+)['\"]", html):
            reaction_str = match.group(1)
            if reaction_str:
                reaction_str = reaction_str.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&')
                if reaction_str not in reactions:
                    reactions.append(reaction_str)
        
        for elem in soup.find_all(attrs={'data-reaction-smiles': True}):
            reaction_str = elem.get('data-reaction-smiles')
            if reaction_str:
                reaction_str = reaction_str.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&')
                if reaction_str not in reactions:
                    reactions.append(reaction_str)
        
        for script in soup.find_all('script'):
            script_text = script.string or ''
            for match in re.finditer(r"['\"]([A-Za-z0-9@\+\-\[\]\(\)=#\.]+>[A-Za-z0-9@\+\-\[\]\(\)=#\.]*>[A-Za-z0-9@\+\-\[\]\(\)=#\.]+)['\"]", script_text):
                reaction_str = match.group(1)
                if '>' in reaction_str and reaction_str not in reactions:
                    reactions.append(reaction_str)
        
        return reactions
    
    
    def extract_reaction_details(self, reaction_smiles, reaction_index=0):
        try:
            parsed = self._parse_reaction_string(reaction_smiles)
            
            reactant_smiles = '.'.join(parsed.get('reactant_smiles', []))
            solvent_smiles = '.'.join(parsed.get('solvents', []))
            product_smiles = '.'.join(parsed.get('product_smiles', []))
            
            reaction_detail = {
                'reaction_id': f"reaction_{reaction_index + 1}",
                'reaction_smiles': reaction_smiles,
                'reactant_smiles': reactant_smiles,
                'solvent_smiles': solvent_smiles,
                'product_smiles': product_smiles
            }
            
            return reaction_detail
            
        except Exception as e:
            return {
                'reaction_id': f"reaction_{reaction_index + 1}",
                'reaction_smiles': reaction_smiles if reaction_smiles else '',
                'reactant_smiles': '',
                'solvent_smiles': '',
                'product_smiles': ''
            }
    
    def _parse_reaction_string(self, reaction_str):
        """Parse a reaction SMILES string into components"""
        parts = reaction_str.split('>')
        while len(parts) < 3:
            parts.append('')
        
        reactants = [p.strip() for p in parts[0].split('.') if p.strip()]
        solvents = [p.strip() for p in parts[1].split('.') if p.strip()]
        products = [p.strip() for p in parts[2].split('.') if p.strip()]
        
        return {
            'reactant_smiles': reactants,
            'solvents': solvents,
            'product_smiles': products,
        }
    
    
    def save_current_data(self):
        """Save data incrementally, merging with existing data"""
        final_output = json.loads(json.dumps(self.existing_data)) if self.existing_data else {}
        
        if not self.scraped_data['datasets']:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False)
            self.existing_data = final_output
            return
        
        for idx, dataset in enumerate(self.scraped_data['datasets']):
            doi = dataset.get('dataset_id', '')
            if not doi or doi.startswith('dataset_'):
                doi = self._extract_dataset_id_from_url(dataset.get('url', ''))
            
            if not doi or doi.startswith('dataset_'):
                continue
            
            doi_parts = doi.split('/', 1)
            if len(doi_parts) == 2:
                doi_prefix = doi_parts[0]
                doi_suffix = '/' + doi_parts[1]
            else:
                doi_prefix = doi
                doi_suffix = ''
            
            doi_key = f"DOI {doi_prefix}"
            
            if doi_key not in final_output:
                final_output[doi_key] = {}
            
            if doi_suffix not in final_output[doi_key]:
                final_output[doi_key][doi_suffix] = {}
            
            if 'reaction_details' not in dataset:
                continue
            
            for reaction in dataset['reaction_details']:
                reaction_id = reaction.get('reaction_id', '')
                if not reaction_id:
                    continue
                
                reaction_smiles = reaction.get('reaction_smiles', '')
                reactant_smiles = reaction.get('reactant_smiles', '')
                solvent_smiles = reaction.get('solvent_smiles', '')
                product_smiles = reaction.get('product_smiles', '')
                
                reaction_data = {
                    "reaction_smiles": reaction_smiles,
                    "components": {
                        "reactant_smiles": reactant_smiles,
                        "solvent_smiles": solvent_smiles,
                        "product_smiles": product_smiles
                    }
                }
                
                final_output[doi_key][doi_suffix][reaction_id] = reaction_data
        
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False)
            
            self.existing_data = final_output
            
            # Data saved silently
        except Exception as e:
            print(f"✗ Error saving data: {e}")
            print(f"  Existing data preserved in memory, will retry on next save")
    
    def scrape_dataset(self, entry_info, max_reactions_per_dataset=100, dataset_idx=0):
        """Scrape a single dataset (all reactions up to max_reactions_per_dataset)"""
        reaction_data_url = entry_info.get('reaction_data_url')
        if not reaction_data_url:
            return
        
        compound_name = entry_info.get('compound_name', 'Unknown')
        authors = entry_info.get('authors', '')
        year = entry_info.get('year', '')
        
        header_patterns = [
            r'.*?you have reached the archives.*?',
            r'.*?chemical reaction database.*?',
            r'.*?archives of the.*?',
            r'^.*?2025\s+',
        ]
        for pattern in header_patterns:
            compound_name = re.sub(pattern, '', compound_name, flags=re.IGNORECASE).strip()
        
        if 'chemical reaction database' in compound_name.lower() or 'archives' in compound_name.lower():
            match = re.search(r'(?:database|archives).*?(\w+.*?,\s*[^,]+)', compound_name, re.IGNORECASE)
            if match:
                compound_name = match.group(1).strip()
            else:
                parts = compound_name.split(',')
                for part in parts:
                    part = part.strip()
                    if part and 'database' not in part.lower() and 'archives' not in part.lower() and len(part) > 3:
                        compound_name = part
                        break
        
        print(reaction_data_url)
        
        dataset_info, reactions = self.scrape_reaction_data_page(reaction_data_url, entry_info)
        if not dataset_info:
            return
        
        reactions = reactions[:max_reactions_per_dataset]
        dataset_info['reaction_samples'] = reactions
        
        for idx, reaction_smiles in enumerate(reactions):
            self.global_reaction_counter += 1
            print(f"Scraping Reaction-{idx + 1}")
            
            reaction_detail = self.extract_reaction_details(
                reaction_smiles, 
                reaction_index=idx
            )
            
            dataset_info['reaction_details'].append(reaction_detail)
            
            self.scraped_data['datasets'].append(dataset_info)
            self.save_current_data()
            self.scraped_data['datasets'].pop()
            
            time.sleep(0.1)

        self.scraped_data['datasets'].append(dataset_info)
        self.save_current_data()
        
        print("-" * 70)
    
    def run_scrape(self, archive_url, max_datasets=None, max_reactions_per_dataset=100):
        """Run the complete scraping process"""
        # Clear the JSON file and start fresh
        with open(self.json_file, 'w', encoding='utf-8') as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        self.existing_data = {}

        self.scraped_data = {'datasets': []}

        entries = self.scrape_archive_page(archive_url)
        if not entries:
            print("✗ No entries found in archive")
            return
        
        # Limit datasets if specified
        if max_datasets:
            entries = entries[:max_datasets]

        self.global_reaction_counter = 0
        
        dataset_idx = 0
        for idx, entry_info in enumerate(entries, 1):
            self.scrape_dataset(entry_info, max_reactions_per_dataset=max_reactions_per_dataset, 
                               dataset_idx=dataset_idx)
            dataset_idx += 1
            
            time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="CRD scraper for Chemical Reaction Database")
    parser.add_argument("--archive-url", type=str, 
                       default="https://kmt.vander-lingen.nl/archive",
                       help="Archive page URL")
    parser.add_argument("--max-datasets", type=int, default=None,
                       help="Maximum number of datasets to scrape")
    parser.add_argument("--max-reactions-per-dataset", type=int, default=100,
                       help="Maximum reactions to pull per dataset")
    args = parser.parse_args()
    
    scraper = CRDScraper()
    
    # Run the scraping process
    scraper.run_scrape(
        archive_url=args.archive_url,
        max_datasets=args.max_datasets,
        max_reactions_per_dataset=args.max_reactions_per_dataset,
    )


if __name__ == "__main__":
    main()


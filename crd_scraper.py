import requests
from bs4 import BeautifulSoup
import json
import re
import time
import argparse
import sys
from urllib.parse import urljoin

# Set output encoding to UTF-8
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
                # Don't raise for 404 - return None to skip silently
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                # 404 errors - don't retry, just return None
                if hasattr(e.response, 'status_code') and e.response.status_code == 404:
                    return None
                # Other HTTP errors - don't retry
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
                # Check if it's a connection-related error (like RemoteDisconnected)
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ['connection', 'disconnected', 'aborted', 'remote']):
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        time.sleep(wait_time)
                    else:
                        raise
                else:
                    # For other exceptions, don't retry
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
            
            # Extract entries by year sections
            entries = self._extract_entries_by_year(soup, archive_url)
            return entries
            
        except Exception as e:
            return []
    
    def _extract_entries_by_year(self, soup, archive_url):
        """Extract entries organized by year sections"""
        entries = []
        
        # Find all list items or entries
        for li in soup.find_all(['li', 'div', 'p']):
            text = li.get_text(' ', strip=True)
            if not text or 'reaction data' not in text.lower():
                continue
            
            # Filter out page header text
            header_phrases = [
                'you have reached the archives',
                'chemical reaction database',
                'archives of the'
            ]
            for phrase in header_phrases:
                if phrase in text.lower():
                    # Remove header text from the beginning
                    text = re.sub(rf'.*?{re.escape(phrase)}.*?(?=\w)', '', text, flags=re.IGNORECASE).strip()
                    # Remove leading year if present
                    text = re.sub(r'^\d{4}\s*', '', text).strip()
                    break
            
            if not text or 'reaction data' not in text.lower():
                continue
            
            # Find all links in this element
            links = li.find_all('a', href=True)
            reaction_data_url = None
            doi_url = None
            
            for link in links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()
                
                # Find reaction data link
                if 'reaction data' in link_text or ('reaction' in href.lower() and 'data' in href.lower()):
                    reaction_data_url = urljoin(self.base_url, href)
                
                # Find DOI link
                if link_text == 'doi' or 'doi' in href.lower():
                    doi_url = href
                    # Extract DOI from URL
                    doi_match = re.search(r'doi[=/]([0-9]+\.[^/\s&]+)', href)
                    if doi_match:
                        doi_url = doi_match.group(1)
            
            if not reaction_data_url:
                # Try to find any link that might be reaction data
                for link in links:
                    href = link.get('href', '')
                    if 'reaction' in href.lower() or 'data' in href.lower():
                        reaction_data_url = urljoin(self.base_url, href)
                        break
            
            if not reaction_data_url:
                continue
            
            # Extract entry info from text
            # Pattern: "Compound Name, Authors Year reaction data | DOI"
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
                # Fallback: just use the URL
                # Filter out page header text
                compound_text = text.split(',')[0].strip() if ',' in text else text[:50]
                # Remove common page header phrases
                header_phrases = [
                    'you have reached the archives',
                    'chemical reaction database',
                    'archives of the'
                ]
                for phrase in header_phrases:
                    if phrase in compound_text.lower():
                        # Try to extract after the header
                        parts = compound_text.split(phrase, 1)
                        if len(parts) > 1:
                            compound_text = parts[1].strip()
                            # Remove leading year if present
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
        """Step 2: Scrape a reaction data page to extract reactions, handling pagination"""
        
        all_reactions = []
        all_details_links = []
        current_url = reaction_data_url
        page_num = 0
        
        try:
            while current_url:
                response = self._make_request_with_retry(current_url)
                if not response:
                    break
                html_content = response.text
                
                # Extract reactions from the page
                reactions = self._extract_reactions_from_page(html_content)
                
                # Extract details links from the page
                details_links = self._extract_details_links(html_content, current_url)
                all_details_links.extend(details_links)
                
                # Check if page has reactions - stop immediately if empty
                if len(reactions) == 0:
                    break
                
                # Add reactions if found
                all_reactions.extend(reactions)
                
                # Find next page link
                next_url = self._find_next_page_link(html_content, current_url)
                if next_url and next_url != current_url:
                    current_url = next_url
                    page_num += 1
                    time.sleep(0.5)  # Be polite between pages
                else:
                    # No next page found, stop
                    break
            
            # Extract DOI from URL
            doi = self._extract_dataset_id_from_url(reaction_data_url)
            # Also try to extract from entry_info if available
            if entry_info:
                entry_doi = self._extract_doi_from_entry(entry_info)
                if entry_doi:
                    doi = entry_doi
            
            dataset_info = {
                'dataset_id': doi,  # Store DOI as dataset_id
                'dataset_name': entry_info.get('compound_name', '') if entry_info else '',
                'authors': entry_info.get('authors', '') if entry_info else '',
                'year': entry_info.get('year', '') if entry_info else '',
                'url': reaction_data_url,
                'reaction_samples': all_reactions,
                'details_links': all_details_links,
                'reaction_details': []
            }
            
            return dataset_info, all_reactions
            
        except Exception as e:
            # Silently skip errors (404s and connection errors are handled in _make_request_with_retry)
            return None, []
    
    def _find_next_page_link(self, html, current_url):
        """Find the Next page link in HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for "Next" button/link
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            if link_text in ['next', '>', '»']:
                href = link.get('href')
                if href:
                    return urljoin(current_url, href)
        
        # Also check buttons
        for button in soup.find_all(['button', 'a']):
            button_text = button.get_text(strip=True).lower()
            if button_text == 'next':
                onclick = button.get('onclick', '')
                href = button.get('href', '')
                if href:
                    return urljoin(current_url, href)
                # Try to extract URL from onclick
                if onclick:
                    match = re.search(r"['\"]([^'\"]+)['\"]", onclick)
                    if match:
                        return urljoin(current_url, match.group(1))
        
        return None
    
    def _extract_dataset_id_from_url(self, url):
        """Extract DOI from URL"""
        # Try to extract full DOI from URL (format: /doi/10.1021/jacsau.4c01276/start/0)
        # Capture everything after /doi/ until the next path segment (like /start)
        match = re.search(r'/doi/([^/]+/[^/]+)', url)
        if match:
            return match.group(1)  # Returns "10.1021/jacsau.4c01276"
        
        # Fallback: try to extract just the prefix if no suffix found
        match = re.search(r'/doi/([^/]+)', url)
        if match:
            return match.group(1)
        
        # Also try to extract from query parameters or path
        match = re.search(r'doi[=/]([0-9]+\.[^/\s&]+)', url)
        if match:
            return match.group(1)
        
        # Fallback: use a hash of the URL
        return f"dataset_{hash(url) % 1000000}"
    
    def _extract_doi_from_entry(self, entry_info):
        """Extract DOI from entry info or URL"""
        # First check if DOI is already in entry_info
        if 'doi' in entry_info:
            doi = entry_info['doi']
            if doi and not doi.startswith('http'):
                return doi
        
        # Try to get DOI from reaction_data_url
        url = entry_info.get('reaction_data_url', '')
        if url:
            doi = self._extract_dataset_id_from_url(url)
            if doi and not doi.startswith('dataset_'):
                return doi
        
        # Try to extract from any URL in entry_info
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
        
        # PRIMARY METHOD: Extract reactions from reaction-pane divs in order
        # Structure: <div id="reaction-pane-0"> contains <button id="modal-0" data-reaction-smiles="...">
        reaction_panes = soup.find_all('div', id=re.compile(r'reaction-pane-\d+'))
        
        for pane in reaction_panes:
            # Find the Smiles button inside this pane
            smiles_button = pane.find('button', attrs={'data-reaction-smiles': True})
            if smiles_button:
                reaction_str = smiles_button.get('data-reaction-smiles', '')
                if reaction_str:
                    # Decode HTML entities (like &gt; to >)
                    reaction_str = reaction_str.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&')
                    if reaction_str not in reactions:
                        reactions.append(reaction_str)
        
        # If we found reactions from panes, return them (they're in correct order)
        if reactions:
            return reactions
        
        # FALLBACK METHOD 1: Look for inline JS array pattern
        for match in re.finditer(r"reactions\.push\(\s*['\"]([\s\S]*?)['\"]\s*\)", html):
            reaction_str = match.group(1)
            if reaction_str:
                reactions.append(reaction_str)
        
        # FALLBACK METHOD 2: Look for data-reaction-smiles attributes (all buttons)
        for match in re.finditer(r"data-reaction-smiles\s*=\s*['\"]([^'\"]+)['\"]", html):
            reaction_str = match.group(1)
            if reaction_str:
                # Decode HTML entities
                reaction_str = reaction_str.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&')
                if reaction_str not in reactions:
                    reactions.append(reaction_str)
        
        # FALLBACK METHOD 3: Parse HTML with BeautifulSoup
        for elem in soup.find_all(attrs={'data-reaction-smiles': True}):
            reaction_str = elem.get('data-reaction-smiles')
            if reaction_str:
                # Decode HTML entities
                reaction_str = reaction_str.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&')
                if reaction_str not in reactions:
                    reactions.append(reaction_str)
        
        # FALLBACK METHOD 4: Look for reaction strings in script tags
        for script in soup.find_all('script'):
            script_text = script.string or ''
            for match in re.finditer(r"['\"]([A-Za-z0-9@\+\-\[\]\(\)=#\.]+>[A-Za-z0-9@\+\-\[\]\(\)=#\.]*>[A-Za-z0-9@\+\-\[\]\(\)=#\.]+)['\"]", script_text):
                reaction_str = match.group(1)
                if '>' in reaction_str and reaction_str not in reactions:
                    reactions.append(reaction_str)
        
        return reactions
    
    def _extract_details_links(self, html, base_url):
        """Extract Details button links from HTML - match with reaction-pane divs"""
        details_links = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # PRIMARY METHOD: Match Details buttons with reaction-pane divs
        # Structure: <div id="reaction-pane-0"> contains <a id="title-0" href="/data/reaction/profile/...">Details</a>
        reaction_panes = soup.find_all('div', id=re.compile(r'reaction-pane-\d+'))
        
        pane_to_details = {}
        for pane in reaction_panes:
            # Get the pane index
            pane_id = pane.get('id', '')
            match = re.search(r'reaction-pane-(\d+)', pane_id)
            if match:
                pane_index = int(match.group(1))
                
                # Find the Details button inside this pane
                details_button = pane.find('a', id=re.compile(r'title-\d+'))
                if not details_button:
                    # Try finding any Details link in the pane
                    details_button = pane.find('a', string=re.compile(r'Details', re.I))
                
                if details_button:
                    href = details_button.get('href', '')
                    if href:
                        full_url = urljoin(base_url, href)
                        pane_to_details[pane_index] = full_url
        
        # Sort by pane index and add to details_links
        for idx in sorted(pane_to_details.keys()):
            details_links.append(pane_to_details[idx])
        
        # If we found details links from panes, return them (they're in correct order)
        if details_links:
            return details_links
        
        # FALLBACK METHOD: Find Details buttons with id="title-0", "title-1", etc.
        title_links = {}
        for link in soup.find_all('a', href=True):
            link_id = link.get('id', '')
            link_text = link.get_text(strip=True).strip()
            href = link.get('href', '')
            
            # Check if it's a Details button with title-X id
            if link_id.startswith('title-') and link_text.lower() == 'details':
                try:
                    index = int(link_id.split('-')[1])
                    if href:
                        full_url = urljoin(base_url, href)
                        title_links[index] = full_url
                except:
                    pass
        
        # Sort by index and add to details_links
        for idx in sorted(title_links.keys()):
            details_links.append(title_links[idx])
        
        if details_links:
            print(f"    Found {len(details_links)} Details buttons with title-X IDs")
            return details_links
        
        # FALLBACK METHOD 2: Find all links with "Details" text that have profile URLs
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            href = link.get('href', '')
            
            if link_text == 'details' and '/data/reaction/profile/' in href:
                full_url = urljoin(base_url, href)
                if full_url not in details_links:
                    details_links.append(full_url)
        
        # FALLBACK METHOD 3: Find links with href containing "/data/reaction/profile/"
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/data/reaction/profile/' in href:
                full_url = urljoin(base_url, href)
                if full_url not in details_links:
                    details_links.append(full_url)
        
        # Sort by id to maintain order (title-0, title-1, etc.)
        details_links = self._sort_details_links(details_links, html, base_url)
        
        return details_links
    
    def _sort_details_links(self, details_links, html, base_url):
        """Sort details links by their title-X id to maintain order"""
        if not details_links:
            return details_links
        
        soup = BeautifulSoup(html, 'html.parser') if isinstance(html, str) else html
        
        # Create a mapping of links to their indices
        link_to_index = {}
        for link in soup.find_all('a', href=True):
            link_id = link.get('id', '')
            href = link.get('href', '')
            if link_id.startswith('title-') and href:
                try:
                    index = int(link_id.split('-')[1])
                    full_url = urljoin(base_url, href)
                    link_to_index[full_url] = index
                except:
                    pass
        
        # Sort details links by their title-X id if available
        def get_sort_key(link):
            return link_to_index.get(link, 9999)
        
        try:
            details_links.sort(key=get_sort_key)
        except:
            pass
        
        return details_links
    
    def _find_details_url_for_reaction(self, reaction_data_url, reaction_index):
        """Try to find or construct details URL for a specific reaction"""
        # Try to fetch the reaction data page and find details links
        try:
            response = self._make_request_with_retry(reaction_data_url)
            if response and response.status_code == 200:
                html = response.text
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for all details links on the page
                details_links = self._extract_details_links(html, reaction_data_url)
                
                # Try to find the specific link for this reaction index
                # Look for link with id="title-{reaction_index}"
                target_id = f"title-{reaction_index}"
                target_link = soup.find('a', id=target_id, href=True)
                if target_link:
                    href = target_link.get('href')
                    if href:
                        return urljoin(reaction_data_url, href)
                
                # If we have details links and the index is within range, use it
                if reaction_index < len(details_links):
                    return details_links[reaction_index]
                
                # Try pagination - calculate which page this reaction is on
                # Assuming 10 reactions per page (common default)
                reactions_per_page = 10
                page_num = reaction_index // reactions_per_page
                page_start = page_num * reactions_per_page
                reaction_on_page = reaction_index - page_start
                
                # Try to fetch the specific page
                if page_num > 0:
                    # Construct page URL
                    page_url = reaction_data_url.rsplit('/start/', 1)[0] if '/start/' in reaction_data_url else reaction_data_url
                    page_url = f"{page_url}/start/{page_start}"
                    
                    try:
                        page_response = self._make_request_with_retry(page_url)
                        if page_response and page_response.status_code == 200:
                            page_html = page_response.text
                            page_details_links = self._extract_details_links(page_html, page_url)
                            if reaction_on_page < len(page_details_links):
                                return page_details_links[reaction_on_page]
                    except:
                        pass
        except Exception as e:
            print(f"  Error finding details URL: {e}")
        
        return None
    
    def _extract_name_from_reaction_page(self, reaction_data_url, reaction_index):
        """Try to extract reaction name directly from the reaction data page"""
        try:
            response = self._make_request_with_retry(reaction_data_url)
            if response and response.status_code == 200:
                html = response.text
                soup = BeautifulSoup(html, 'html.parser')
                
                # First priority: Look for badge inside h2 tags
                # Structure: <h2><span class="badge badge-pill badge-info">Riley oxidation</span></h2>
                h2_elem = soup.find('h2')
                if h2_elem:
                    # Try to find badge inside h2
                    badge_elem = h2_elem.find('span', class_=['badge', 'badge-pill', 'badge-info'])
                    if not badge_elem:
                        badge_elem = h2_elem.find('span', class_='badge badge-pill badge-info')
                    if not badge_elem:
                        # Try flexible search inside h2
                        for span in h2_elem.find_all('span'):
                            classes = span.get('class', [])
                            if isinstance(classes, list):
                                class_str = ' '.join(classes)
                            else:
                                class_str = str(classes)
                            if 'badge-pill' in class_str and 'badge-info' in class_str:
                                badge_elem = span
                                break
                    
                    if badge_elem:
                        name_text = badge_elem.get_text(strip=True)
                        if name_text and len(name_text) < 100 and len(name_text) > 0:
                            return name_text
                
                # Second priority: Look for badge elements anywhere on the page
                for span in soup.find_all('span'):
                    classes = span.get('class', [])
                    if isinstance(classes, list):
                        class_str = ' '.join(classes)
                    else:
                        class_str = str(classes)
                    if 'badge-pill' in class_str and 'badge-info' in class_str:
                        name_text = span.get_text(strip=True)
                        if name_text and len(name_text) < 100 and len(name_text) > 0:
                            return name_text
        except Exception as e:
            print(f"  Error extracting name from page: {e}")
        
        return None
    
    def extract_reaction_details(self, reaction_smiles, reaction_index=0, details_url=None):
        """Step 3: Extract reaction name and properly separated SMILES"""
        try:
            reaction_name = ''
            
            # Parse reaction SMILES string: reactants>solvents>products
            parsed = self._parse_reaction_string(reaction_smiles)
            
            # Join multiple SMILES with dots for each category
            reactant_smiles = '.'.join(parsed.get('reactant_smiles', []))
            solvent_smiles = '.'.join(parsed.get('solvents', []))
            product_smiles = '.'.join(parsed.get('product_smiles', []))
            
            # If details URL is provided, scrape it for reaction name
            if details_url:
                details_data = self.scrape_details_page(details_url)
                if details_data:
                    reaction_name = details_data.get('name', '')
            
            reaction_detail = {
                'reaction_id': f"reaction_{reaction_index + 1}",  # 1-indexed
                'name': reaction_name,
                'reactant_smiles': reactant_smiles,
                'solvent_smiles': solvent_smiles,
                'product_smiles': product_smiles
            }
            
            return reaction_detail
            
        except Exception as e:
            print(f"Error extracting reaction details: {e}")
            return {
                'reaction_id': f"reaction_{reaction_index + 1}",
                'name': '',
                'reactant_smiles': '',
                'solvent_smiles': '',
                'product_smiles': ''
            }
    
    def _parse_reaction_string(self, reaction_str):
        """Parse a reaction SMILES string into components"""
        # Format: reactants>solvents>products
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
    
    def scrape_details_page(self, details_url):
        """Scrape a details page to get additional reaction information"""
        try:
            response = self._make_request_with_retry(details_url)
            if not response:
                return None
            html_content = response.text
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            details = {
                'name': '',  # Reaction name (e.g., "Riley oxidation")
                'reactant': '',  # Reactant name
                'solvent': '',  # Solvent name
                'product': '',  # Product name
                'reactant_smiles': [],
                'solvents': [],
                'product_smiles': [],
                'product_names': []
            }
            
            # Extract reaction name - FIRST priority: look for badge element inside h2 tags
            # Structure: <h2><span class="badge badge-pill badge-info">Riley oxidation</span></h2>
            
            badge_elem = None
            h2_elem = None
            
            # Method 1: Look for h2 tags first, then find badge inside
            h2_elem = soup.find('h2')
            if h2_elem:
                # Try to find badge inside h2
                badge_elem = h2_elem.find('span', class_=['badge', 'badge-pill', 'badge-info'])
                if not badge_elem:
                    # Try with string class
                    badge_elem = h2_elem.find('span', class_='badge badge-pill badge-info')
                if not badge_elem:
                    # Try flexible search inside h2 - check all spans
                    for span in h2_elem.find_all('span'):
                        classes = span.get('class', [])
                        if isinstance(classes, list):
                            class_str = ' '.join(classes)
                        else:
                            class_str = str(classes) if classes else ''
                        if 'badge' in class_str and 'badge-pill' in class_str and 'badge-info' in class_str:
                            badge_elem = span
                            break
            
            # Method 2: If not found in h2, try direct span search
            if not badge_elem:
                # Try exact class match
                badge_elem = soup.find('span', class_=['badge', 'badge-pill', 'badge-info'])
                if not badge_elem:
                    # Try with string class
                    badge_elem = soup.find('span', class_='badge badge-pill badge-info')
                if not badge_elem:
                    # Try with lambda function to check all classes are present
                    badge_elem = soup.find('span', class_=lambda x: x and isinstance(x, list) and 'badge' in x and 'badge-pill' in x and 'badge-info' in x)
                if not badge_elem:
                    # Try with string class matching
                    badge_elem = soup.find('span', class_=lambda x: x and isinstance(x, str) and 'badge' in x and 'badge-pill' in x and 'badge-info' in x)
                if not badge_elem:
                    # Fallback: find any span with badge-pill and badge-info (most flexible)
                    for span in soup.find_all('span'):
                        classes = span.get('class', [])
                        if isinstance(classes, list):
                            class_str = ' '.join(classes)
                        else:
                            class_str = str(classes) if classes else ''
                        if 'badge' in class_str and 'badge-pill' in class_str and 'badge-info' in class_str:
                            badge_elem = span
                            break
            
            if badge_elem:
                name_text = badge_elem.get_text(strip=True)
                if name_text and len(name_text) < 100 and len(name_text) > 0:
                    details['name'] = name_text
            elif h2_elem:
                # Fallback: if h2 exists but no badge found, try h2 text directly
                h2_text = h2_elem.get_text(strip=True)
                if h2_text and len(h2_text) < 100 and len(h2_text) > 0:
                    # Check if it looks like a reaction name
                    if any(keyword in h2_text.lower() for keyword in ['oxidation', 'reduction', 'coupling', 'synthesis', 'addition', 'substitution', 'elimination', 'cyclization', 'reaction']):
                        details['name'] = h2_text
            
            # SECOND priority: look for title or heading
            if not details['name']:
                title_elem = soup.find(['h1', 'h2', 'h3', 'h4', 'div', 'span'], 
                                      class_=re.compile(r'title|name|reaction|header', re.I))
                if not title_elem:
                    # Look for divs with specific background colors or styling
                    title_elem = soup.find('div', style=re.compile(r'background|color', re.I))
                if title_elem:
                    name_text = title_elem.get_text(strip=True)
                    # Filter out very long text (likely not a reaction name)
                    if name_text and len(name_text) < 100:
                        details['name'] = name_text
            
            # THIRD priority: look for reaction name in text patterns (common reaction types)
            if not details['name']:
                all_text = soup.get_text()
                # Look for common reaction type patterns
                name_patterns = [
                    r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:oxidation|reduction|coupling|synthesis|addition|substitution|elimination|cyclization|reaction))\b',
                    r'\b((?:oxidation|reduction|coupling|synthesis|addition|substitution|elimination|cyclization)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b',
                    r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+reaction)\b',
                ]
                for pattern in name_patterns:
                    name_match = re.search(pattern, all_text, re.I)
                    if name_match:
                        details['name'] = name_match.group(1).strip()
                        break
            
            # Extract from tables
            for table in soup.select('table'):
                rows = table.select('tr')
                for row in rows:
                    cells = row.find_all(['th', 'td'])[:2]
                    if len(cells) < 2:
                        continue
                    
                    key = (cells[0].get_text(' ', strip=True) or '').lower()
                    val = cells[1]
                    vals = [s.strip() for s in val.stripped_strings if s.strip()]
                    
                    # Extract reactant name (not SMILES)
                    if 'reactant' in key and 'name' in key and not 'smiles' in key:
                        if vals:
                            details['reactant'] = vals[0]
                    elif 'reactant' in key and 'smiles' in key:
                        details['reactant_smiles'].extend([v for v in vals if v != 'SMILES'])
                    # Extract solvent name
                    elif 'solvent' in key and not 'smiles' in key:
                        if vals:
                            details['solvent'] = vals[0]
                    elif 'reactant' in key and 'solvent' in key:
                        if vals:
                            details['solvent'] = vals[0]
                    elif 'product' in key and 'smiles' in key:
                        details['product_smiles'].extend([v for v in vals if v != 'SMILES'])
                    elif 'product' in key and ('name' in key or 'product' == key) and not 'smiles' in key:
                        if vals:
                            details['product'] = vals[0]
                        details['product_names'].extend(vals)
            
            # Extract from definition lists
            for dl in soup.select('dl'):
                items = list(dl.children)
                for i in range(0, len(items) - 1, 2):
                    dt = items[i]
                    dd = items[i + 1]
                    if getattr(dt, 'name', None) != 'dt' or getattr(dd, 'name', None) != 'dd':
                        continue
                    
                    key = (dt.get_text(' ', strip=True) or '').lower()
                    vals = [s.strip() for s in dd.stripped_strings if s.strip()]
                    
                    # Extract reactant name
                    if 'reactant' in key and 'name' in key and not 'smiles' in key:
                        if vals:
                            details['reactant'] = vals[0]
                    elif 'reactant' in key and 'smiles' in key:
                        details['reactant_smiles'].extend([v for v in vals if v != 'SMILES'])
                    # Extract solvent name
                    elif 'solvent' in key and not 'smiles' in key:
                        if vals:
                            details['solvent'] = vals[0]
                    elif 'reactant' in key and 'solvent' in key:
                        if vals:
                            details['solvent'] = vals[0]
                    elif 'product' in key and 'smiles' in key:
                        details['product_smiles'].extend([v for v in vals if v != 'SMILES'])
                    elif 'product' in key and ('name' in key or 'product' == key) and not 'smiles' in key:
                        if vals:
                            details['product'] = vals[0]
                        details['product_names'].extend(vals)
            
            # Remove duplicates
            details['reactant_smiles'] = sorted(set(details['reactant_smiles']))
            details['solvents'] = sorted(set(details['solvents']))
            details['product_smiles'] = sorted(set(details['product_smiles']))
            
            return details
            
        except Exception as e:
            print(f"Error scraping details page: {e}")
            return None
    
    def save_current_data(self):
        """Save data incrementally, merging with existing data"""
        # Always start with existing data (preserve all existing data)
        final_output = json.loads(json.dumps(self.existing_data)) if self.existing_data else {}  # Deep copy
        
        # If no new data to add, just save existing data to preserve it
        if not self.scraped_data['datasets']:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False)
            self.existing_data = final_output
            return
        
        for idx, dataset in enumerate(self.scraped_data['datasets']):
            # Extract DOI from dataset
            doi = dataset.get('dataset_id', '')
            # If dataset_id doesn't look like a DOI, try to extract from URL
            if not doi or doi.startswith('dataset_'):
                doi = self._extract_dataset_id_from_url(dataset.get('url', ''))
            
            if not doi or doi.startswith('dataset_'):
                continue
            
            # Split DOI into prefix and suffix
            # Format: "10.1021/jacsau.4c01276" -> prefix: "10.1021", suffix: "/jacsau.4c01276"
            doi_parts = doi.split('/', 1)
            if len(doi_parts) == 2:
                doi_prefix = doi_parts[0]  # e.g., "10.1021"
                doi_suffix = '/' + doi_parts[1]  # e.g., "/jacsau.4c01276"
            else:
                # Fallback: use whole DOI as prefix
                doi_prefix = doi
                doi_suffix = ''
            
            # Use "DOI {prefix}" as top-level key
            doi_key = f"DOI {doi_prefix}"
            
            if doi_key not in final_output:
                final_output[doi_key] = {}
            
            if doi_suffix not in final_output[doi_key]:
                final_output[doi_key][doi_suffix] = {}
            
            if 'reaction_details' not in dataset:
                continue
            
            # Add reactions (merge with existing, overwrite duplicates)
            for reaction in dataset['reaction_details']:
                reaction_id = reaction.get('reaction_id', '')
                if not reaction_id:
                    continue
                
                # Get the reaction name and separated SMILES
                reaction_name = reaction.get('name', '').strip()
                reactant_smiles = reaction.get('reactant_smiles', '')
                solvent_smiles = reaction.get('solvent_smiles', '')
                product_smiles = reaction.get('product_smiles', '')
                
                # Build the output structure with reaction name and properly separated SMILES
                # Only include name field if it's not empty
                reaction_data = {
                    "reactant_smiles": reactant_smiles,
                    "solvent_smiles": solvent_smiles,
                    "product_smiles": product_smiles
                }
                
                # Only add name if it's not empty
                if reaction_name:
                    reaction_data["name"] = reaction_name
                
                # Merge: if reaction already exists, update it; otherwise add new
                final_output[doi_key][doi_suffix][reaction_id] = reaction_data
        
        # Save to file (always preserve existing data)
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False)
            
            # Update existing_data for next save (so subsequent saves merge correctly)
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
        
        # Format dataset name: "Compound Name, Authors Year"
        compound_name = entry_info.get('compound_name', 'Unknown')
        authors = entry_info.get('authors', '')
        year = entry_info.get('year', '')
        
        # Filter out page header text from compound name - more aggressive
        # Remove everything before and including common header phrases
        header_patterns = [
            r'.*?you have reached the archives.*?',
            r'.*?chemical reaction database.*?',
            r'.*?archives of the.*?',
            r'^.*?2025\s+',  # Remove leading "2025" if it's from header
        ]
        for pattern in header_patterns:
            compound_name = re.sub(pattern, '', compound_name, flags=re.IGNORECASE).strip()
        
        # If compound name still looks like it contains header, try to find the actual name
        # Look for pattern: "Compound Name, Authors" after any header text
        if 'chemical reaction database' in compound_name.lower() or 'archives' in compound_name.lower():
            # Try to extract name after the header
            match = re.search(r'(?:database|archives).*?(\w+.*?,\s*[^,]+)', compound_name, re.IGNORECASE)
            if match:
                compound_name = match.group(1).strip()
            else:
                # Last resort: split by comma and take the first meaningful part
                parts = compound_name.split(',')
                for part in parts:
                    part = part.strip()
                    if part and 'database' not in part.lower() and 'archives' not in part.lower() and len(part) > 3:
                        compound_name = part
                        break
        
        # Print the reaction data URL instead of dataset name
        print(reaction_data_url)
        
        dataset_info, reactions = self.scrape_reaction_data_page(reaction_data_url, entry_info)
        if not dataset_info:
            return
        
        # Limit reactions
        reactions = reactions[:max_reactions_per_dataset]
        dataset_info['reaction_samples'] = reactions
        total_reactions = len(reactions)
        
        # Get details links if available
        details_links = dataset_info.get('details_links', [])
        reaction_data_url = dataset_info.get('url', '')
        
        # If we don't have enough details links, try to re-fetch them from the first page
        if len(details_links) < total_reactions:
            try:
                # Re-fetch details links from the reaction data page
                response = self._make_request_with_retry(reaction_data_url)
                if response and response.status_code == 200:
                    html = response.text
                    page_details_links = self._extract_details_links(html, reaction_data_url)
                    # Merge with existing, prioritizing new ones
                    for link in page_details_links:
                        if link not in details_links:
                            details_links.append(link)
                    # Re-sort to maintain order
                    details_links = self._sort_details_links(details_links, html, reaction_data_url)
            except Exception as e:
                pass
        
        for idx, reaction_smiles in enumerate(reactions):
            self.global_reaction_counter += 1
            print(f"Scraping Reaction-{idx + 1}")
            
            # Try to match with a details link (if available and index matches)
            details_url = None
            
            # First try: use the details link at the same index
            if idx < len(details_links):
                details_url = details_links[idx]
            else:
                # Second try: find the Details button with id="title-{idx}" by re-scraping
                details_url = self._find_details_url_for_reaction(reaction_data_url, idx)
            
            # Always try to extract name from details page if we have a URL
            reaction_detail = self.extract_reaction_details(
                reaction_smiles, 
                reaction_index=idx,
                details_url=details_url
            )
            
            # If name is still empty and we have a details URL, try again
            if not reaction_detail.get('name') and details_url:
                details_data = self.scrape_details_page(details_url)
                if details_data and details_data.get('name'):
                    reaction_detail['name'] = details_data.get('name')
            
            # Last resort: try to extract from the reaction data page directly
            if not reaction_detail.get('name') and reaction_data_url:
                name_from_page = self._extract_name_from_reaction_page(reaction_data_url, idx)
                if name_from_page:
                    reaction_detail['name'] = name_from_page
            
            dataset_info['reaction_details'].append(reaction_detail)
            
            # Save incrementally after each reaction
            # Temporary attach to scraped_data to persist progress
            self.scraped_data['datasets'].append(dataset_info)
            self.save_current_data()
            self.scraped_data['datasets'].pop()
            
            time.sleep(0.1)  # Be polite to the server
        
        # Final save with complete dataset
        self.scraped_data['datasets'].append(dataset_info)
        self.save_current_data()
        
        # Print separator line after dataset is complete
        print("-" * 70)
    
    def run_scrape(self, archive_url, max_datasets=None, max_reactions_per_dataset=100):
        """Run the complete scraping process"""
        # Clear the JSON file and start fresh
        with open(self.json_file, 'w', encoding='utf-8') as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        self.existing_data = {}
        
        # Reset accumulated data (working data for this run)
        self.scraped_data = {'datasets': []}
        
        # Step 1: Scrape archive page
        entries = self.scrape_archive_page(archive_url)
        if not entries:
            print("✗ No entries found in archive")
            return
        
        # Limit datasets if specified
        if max_datasets:
            entries = entries[:max_datasets]
        
        # Initialize global reaction counter
        self.global_reaction_counter = 0
        
        # Start scraping immediately - no counting pass to avoid delays
        dataset_idx = 0
        for idx, entry_info in enumerate(entries, 1):
            self.scrape_dataset(entry_info, max_reactions_per_dataset=max_reactions_per_dataset, 
                               dataset_idx=dataset_idx)
            dataset_idx += 1
            
            time.sleep(0.5)  # Delay between datasets


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


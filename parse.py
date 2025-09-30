import urllib3
import tldextract
from datetime import datetime
import urllib.parse
import urllib.robotparser
from bs4 import BeautifulSoup
import math
from storage import *


retries = urllib3.util.Retry(total=1, backoff_factor=0.2)
poolManager = urllib3.PoolManager(num_pools=50, maxsize=10, retries=retries)


def clean_url(url):
    # removes fragments and queries
    url = urllib.parse.urldefrag(url)[0]
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def get_links(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8'} # prioritize text/html or xhtml+xml    
        #get response
        try:
            response = poolManager.request('GET', url, headers=headers, timeout=3)
            if len(response.data) > (1*1024*1024):
                print(f"file larger than 1MB, skipping")
                return [len(response.data), "skipped:too_large"]
            content_type = response.headers.get('Content-Type', "")
        except urllib3.exceptions.ConnectTimeoutError as e:
            print(f"Took too long to connect \n")
            return [0, "timeout"]
        except urllib3.exceptions.MaxRetryError as e:
            print(f"Max retries exceeded \n")
            return [0, "max_retries_exceeded"]

        if response.status != 200:
            safe_dictadd("error", response.status, 1)
            return [0, f"{response.status}"]

        #Read the content of the response
        #handle different content types and find all links
        content_type = response.headers.get('Content-Type', "")
        if "text/html" in content_type or "application/xhtml+xml" in content_type:
            soup = BeautifulSoup(response.data, 'lxml') 
        else:
            #ignore everything else (dont parse)
            print(f"Skipping non-HTML/XML content {url}:")
            return ["non-HTML/XML content"]
        
        urls = soup.find_all('a', href=True)[:200] #find all links, keep the first 200

        for link in urls:
            # process to get url and domain in the correct format
            try:
                href = clean_url(link['href']) # remove fragments and queries
            except urllib.error.URLError as e:
                print(f"Error accessing URL: {e.reason}")
                return []
            except urllib.error.HTTPError as e:
                print(f"HTTP Error: {e.code} - {e.reason}")
                return []
            if href not in url_dict:
                if href.startswith('/'): # if relative link
                    link['href'] = urllib.parse.urljoin(url, href)  # combine base url with relative link
                    extract = tldextract.extract(urllib.parse.urlparse(link['href']).netloc)
                    link['domain'] = extract.domain+"."+extract.suffix # get domain
                    if extract.subdomain:
                        link['full_domain'] = extract.subdomain+"."+extract.domain+"."+extract.suffix
                    else:
                        link['full_domain'] = extract.domain+"."+extract.suffix
                elif href.startswith('http'):
                    link['href'] = href # url is full
                    extract = tldextract.extract(urllib.parse.urlparse(href).netloc)
                    link['domain'] = extract.domain+"."+extract.suffix # get domain
                    if extract.subdomain:
                        link['full_domain'] = extract.subdomain+"."+extract.domain+"."+extract.suffix
                    else:
                        link['full_domain'] = extract.domain+"."+extract.suffix
                else: # ignore other types of links 
                    link['href'] = "invalid"
            else: # ignore other types of links 
                link['href'] = "invalid"
        urls.append(len(response.data)) 
        urls.append(200)
        return urls
        
    except urllib.error.URLError as e:
        print(f"Error accessing URL: {e.reason}")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
def handle_robot(url):
    # handle robots.txt
    try:
        parsed_url = urllib.parse.urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"

        response = urllib.robotparser.RobotFileParser()
        response.set_url(robots_url)
        response.read()
        
        return response.can_fetch("*", url)
        
    except urllib.error.URLError as e:
        print(f"Error accessing URL: {e.reason}")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
        
def parse_pipeline(temp):
    #parse url from heapq and add new links to the heapq and respective dicts
    can_fetch = handle_robot(temp[1])
    current_url = temp[1]
    if current_url not in parsed_dict:
        #print(f"Processing: {temp}\n")
        iteration = url_dict[current_url]
        links = get_links(current_url) 
        safe_dictadd("parsed", current_url, [datetime.now().strftime("%H:%M:%S"), iteration, links[-2], links[-1]]) #change to add time, depth, size, return code
        if not links or len(links) <= 2:
            return
        for link in links[:-2]:
            if link['href'] != "invalid":
                domain = link['domain']
                full_domain = link['full_domain']
                if domain not in domain_dict:
                    safe_dictadd("domain", domain, 1)
                if full_domain not in full_domain_dict:
                    safe_dictadd("full_domain", full_domain, 1)
                heap_points = 1/(iteration*math.log(2*domain_dict[domain]+full_domain_dict[full_domain]+2))
                safe_heappush(-heap_points, link['href'])                
                safe_dictadd("url", link['href'], iteration + 1)
                safe_dictadd("domain", domain, domain_dict[domain]+1)
                safe_dictadd("full_domain", full_domain, full_domain_dict[full_domain]+1)

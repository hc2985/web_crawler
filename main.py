from collections import defaultdict
import concurrent
import urllib.error
import urllib.parse
import urllib.robotparser
from bs4 import BeautifulSoup
import heapq
import math
import threading
import urllib3
import tldextract   
import time

parsed_dict = {}
url_dict = {}
errors_dict = defaultdict(int)
priority_heapq = []
heapq.heapify(priority_heapq)
domain_dict = {}    
heaplock = threading.Lock()
dictlock = threading.Lock()


retries = urllib3.util.Retry(total=1, backoff_factor=0.2)
poolManager = urllib3.PoolManager(num_pools=50, maxsize=10, retries=retries)

def clean_url(url):
    # removes fragments and queries
    href = urllib.parse.quote(url, encoding='utf-8', safe=':/.?&=#-_')
    cleaned = urllib.parse.urlparse(href, allow_fragments=False)
    return urllib.parse.urlunparse(cleaned)
    
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
                return []
            content_type = response.headers.get('Content-Type', "")
        except urllib3.exceptions.ConnectTimeoutError as e:
            print(f"Took too long: {e}")
            return []
        except urllib3.exceptions.MaxRetryError as e:
            print(f"Max retries exceeded: {e}")
            return []
            
        if response.status != 200:
            safe_dictadd("error", response.status, 1)
            return []

        #Read the content of the response
        #handle different content types and find all links
        content_type = response.headers.get('Content-Type', "")
        if "text/html" in content_type or "application/xhtml+xml" in content_type:
            soup = BeautifulSoup(response.data, 'lxml') 
        else:
            #ignore everything else (dont parse)
            print(f"Skipping non-HTML/XML content {url}:")
            return []
        

        #print("Parsing")
        urls = soup.find_all('a', href=True)[:200]

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
                    link['domain'] = tldextract.extract(urllib.parse.urlparse(link['href']).netloc).domain # get domain
                elif href.startswith('http'):
                    link['href'] = href # url is full
                    link['domain'] = tldextract.extract(urllib.parse.urlparse(href).netloc).domain # get domain
                else: # ignore other types of links 
                    link['href'] = "invalid"
            else: # ignore other types of links 
                link['href'] = "invalid"
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
    if current_url not in parsed_dict and can_fetch:
        #print(f"Processing: {temp}\n")
        iteration  = url_dict[current_url]
        safe_dictadd("parsed", current_url, iteration)
        links = get_links(current_url) 
        
        if not links:
            return
        for link in links:
            if link['href'] != "invalid":
                domain = link['domain']
                if domain not in domain_dict:
                    safe_dictadd("domain", domain, 2)
                heap_points = 1/(iteration*math.log(domain_dict[domain]+1))
                safe_heappush(-heap_points, link['href'])                
                safe_dictadd("url", link['href'], iteration + 1)
                safe_dictadd("domain", domain, domain_dict[domain]+1)


# Below are functions for threading
def safe_dictadd(name, key, value):
    with dictlock:
        if name == "url":
            url_dict[key] = value
        elif name == "parsed":
            parsed_dict[key] = value
        elif name == "domain":
            domain_dict[key] = value
        elif name == "error":
            errors_dict[key] += 1
            
def safe_heappush(score, url):
    with heaplock:
        heapq.heappush(priority_heapq, (score, url))
        if len(priority_heapq) > 20000: # keep the best 10000 links if more than 20000 links
            priority_heapq[:] = heapq.nsmallest(10000, priority_heapq)
            heapq.heapify(priority_heapq)

def safe_heappop():
    with heaplock:
        if priority_heapq:
            return heapq.heappop(priority_heapq)
        return None

def worker():
    target=10000
    tid = threading.get_ident()
    while len(parsed_dict) < target:
        if len(parsed_dict)%100 == 0:
            print(f"Parsed {len(parsed_dict)}/{target}")
        # check if we have links waiting to be parsed, if so pop and parse it
        if priority_heapq:
            print(f"Thread {tid} processing {len(parsed_dict)}")
            temp = safe_heappop()
            if not temp:
                continue
            ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = ex.submit(parse_pipeline, temp)
            try:
                future.result(timeout=10)
            except concurrent.futures.TimeoutError:
                print(f"Timeout parsing {temp[1]}")
            except Exception as e:
                print(f"Error parsing {temp[1]}: {e}")
        else:
            break
            


def main():
    # seed or add links to queue and url_dict -> check if new in parsed_dict -> if new get links -> add new links to queue and url_dict and parsed in parsed_dict -> pop from queue and repeat
    seeds = []
    with open("seed.txt", "r") as f:
        for line in f:
            seeds.append(line.strip())
    
    for seed in seeds:
        heapq.heappush(priority_heapq, (0, seed))
        url_dict[seed] = 1
        
    threads = [threading.Thread(target=worker) for _ in range(10)]

    start_time = time.perf_counter()
    print("Starting crawl:")
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end_time = time.perf_counter()

    print("Writing output:")
    with open("output.txt", "w") as f:
        f.write(f"Crawling {len(parsed_dict)} pages finished in {end_time - start_time:.2f} seconds.\n")
        f.write(f"{len(parsed_dict)/(end_time - start_time):.2f} pages per second.\n\n")
        f.write("Errors encountered:\n")
        for error_code in errors_dict.keys():
            f.write(f"{error_code}: {errors_dict[error_code]}\n")
        f.write("\nParsed URLs:\n")
        for url in parsed_dict.keys():
            f.write(f"{url}\n")
        
    #for domain in domain_dict.keys():
        #print(f"{domain}: {domain_dict[domain]}")
    
    
if __name__ == "__main__":
    main()
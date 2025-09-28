import urllib.request
import urllib.error
import urllib.parse
import urllib.robotparser
from bs4 import BeautifulSoup
from collections import deque
import heapq
import math
import threading
import urllib3
import tldextract   
import time

parsed_dict = {}
url_dict = {}
priority_heapq = []
heapq.heapify(priority_heapq)
domain_dict = {}    
heaplock = threading.Lock()
dictlock = threading.Lock()
    

def get_links(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'} # prioritize text/html or xhtml+xml, xml and other files is lower priority
        retries = urllib3.util.Retry(total=10, backoff_factor=0.2)
        #get response
        try:
            response = urllib3.PoolManager(retries=retries).request('GET', url, headers=headers, timeout=2)
        except urllib3.exceptions.ConnectTimeoutError as e:
            print(f"Took too long: {e}")
        except urllib3.exceptions.MaxRetryError as e:
            print(f"Max retries exceeded: {e}")
            
        if response.status != 200:
            if response.status == 404:
                pass # later handle all errors
            return []

        #Read the content of the response
        #handle different content types
        content_type = response.headers.get('Content-Type', "")
        if "text/html" in content_type:
            print("HTML content")
            content = response.data
            soup = BeautifulSoup(content, 'html.parser')
        elif "application/xhtml+xml" in content_type:
            print("XHTML content")
            content = response.data
            soup = BeautifulSoup(content, 'lxml')
        elif "application/xml" in content_type or "text/xml" in content_type:
            print("XML content")
            content = response.data
            soup = BeautifulSoup(content, 'xml')
        else:
            #ignore everything else (dont parse)
            print(f"Skipping non-HTML/XML content {url}:")
            return []
        urls = soup.find_all('a', href=True) #find all links
        for link in urls:
            try:
                href = urllib.parse.quote(link['href'], encoding='utf-8', safe=':/.?&=#-_') # Get URL with encoding to handle special characters
            except urllib.error.URLError as e:
                print(f"Error accessing URL: {e.reason}")
            except urllib.error.HTTPError as e:
                print(f"HTTP Error: {e.code} - {e.reason}")
            if href.startswith('/'): # if relative link
                link['href'] = urllib.parse.urljoin(url, href)  # combine base url with relative link
                link['domain'] = tldextract.extract(urllib.parse.urlparse(link['href']).netloc).domain # get domain
            elif href.startswith('http'):
                link['href'] = href # url is full
                link['domain'] = tldextract.extract(urllib.parse.urlparse(href).netloc).domain # get domain
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
    try:
        parsed_url = urllib.parse.urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"

        response = urllib.robotparser.RobotFileParser()
        response.set_url(robots_url)
        response.read()
        
        can_fetch = response.can_fetch("*", url)
        
        if not can_fetch:
            print(f"Brr Brr, Robot.txt said no!: {url}")
        return can_fetch

    except urllib.error.URLError as e:
        print(f"Error accessing URL: {e.reason}")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
        
def parse_pipeline(temp):
    #current_url = heapq.heappop(priority_heapq)[1]  
    can_fetch = handle_robot(temp[1])
    current_url = temp[1]
    iteration  = url_dict[current_url]
    if current_url not in parsed_dict and can_fetch:
        #print(f"Processing: {temp}\n")
        safe_dictadd("parsed", current_url, iteration)
        links = get_links(current_url)
        if not links:
            return
        for link in links:
            if link['href'] not in url_dict and link['href'] != "invalid":
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
            
def safe_heappush(score, url):
    with heaplock:
        heapq.heappush(priority_heapq, (score, url))

def safe_heappop():
    with heaplock:
        if priority_heapq:
            return heapq.heappop(priority_heapq)
        return None

def worker():
    while len(parsed_dict) < 100:
        # check if we have links waiting to be parsed, if so pop and parse it
        temp = safe_heappop()
        if not temp:
            continue
        else:
            parse_pipeline(temp)
            

def main():
    # seed or add links to queue and url_dict -> check if new in parsed_dict -> if new get links -> add new links to queue and url_dict and parsed in parsed_dict -> pop from queue and repeat
    seeds = ["https://www.cheese.com/", "https://www.biggerbolderbaking.com/how-to-make-cream-cheese/", "https://www.seriouseats.com/cream-cheese-taste-test-8663390", "https://www.kraftheinz.com/philadelphia/products/00021000612239-original-cream-cheese",
             "https://www.cheeseprofessor.com/blog/cream-cheese-101", "https://www.hungryonion.org/t/we-taste-tested-8-supermarket-cream-cheeses-here-are-our-favorites/39335", "https://creamery.psu.edu/cheese/cream-cheese", "https://www.helpmebake.com/threads/what-is-cream-cheese.1270/",
             "https://cheesemaking.com/products/cream-cheese-recipe?srsltid=AfmBOopkU6PUy_ZVm8z9NFHbOpMESfYhaD7RO7GYFgO9e2q6vblK9mgE", "https://www.wisconsincheese.com/about-cheese/cream-cheese"]
    start_time = time.perf_counter()
    for seed in seeds:
        heapq.heappush(priority_heapq, (0, seed))
        url_dict[seed] = 1

    #while priority_heapq:
    #    parse_pipeline()
        
        
    threads = [threading.Thread(target=worker) for _ in range(20)]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
  
    end_time = time.perf_counter()
   
    print("\nParsed URLs:")
    with open("output.txt", "w") as f:
        f.write(f"Crawling {len(parsed_dict)} pages finished in {end_time - start_time:.2f} seconds.\n")
        f.write(f"{len(parsed_dict)/(end_time - start_time):.2f} pages per second.\n\n")
        f.write("\nParsed URLs:\n")
        for url in parsed_dict.keys():
            f.write(f"{url}\n")
        
    #for domain in domain_dict.keys():
        #print(f"{domain}: {domain_dict[domain]}")
    
    
if __name__ == "__main__":
    main()
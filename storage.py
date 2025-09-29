import heapq
from collections import defaultdict
import threading
import heapq


parsed_dict = {}
url_dict = {}
errors_dict = defaultdict(int)
priority_heapq = []
heapq.heapify(priority_heapq)
domain_dict = {}    
heaplock = threading.Lock()
dictlock = threading.Lock()

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
    
def get_num_parsed():
    return len(parsed_dict)


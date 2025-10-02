import concurrent
import threading   
import time
from storage import *
from parse import *
from query import get_seed_links

global target
target = 1000
global num_threads
num_threads = 16

def worker():
    tid = threading.get_ident()
    # give up on url if thread url takes too long to parse
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    while get_num_parsed() < target:
        # check if we have links waiting to be parsed, if so pop and parse it
        if priority_heapq:
            temp = safe_heappop()
            print(f"Thread {tid} processing {get_num_parsed()}")
            if not temp:
                continue
            future = ex.submit(parse_pipeline, temp)
            try:
                future.result(timeout=5)
            except concurrent.futures.TimeoutError:
                print(f"Timeout parsing {temp[1]}")
            except Exception as e:
                print(f"Unexpected error parsing {temp[1]}: {e}")
        else:
            break
    ex.shutdown(wait=False)
        
def main():
    # seed or add links to queue and url_dict -> check if new in parsed_dict -> if new get links -> add new links to queue and url_dict and parsed in parsed_dict -> pop from queue and repeat
    seeds = get_seed_links(input("type in query:"), num=num_threads)  
    for seed in seeds:
        heapq.heappush(priority_heapq, (0, seed))
        url_dict[seed] = [1, 10]
        
    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    start_time = time.perf_counter()
    print("Starting crawl:")
    for t in threads:
        t.start()
        time.sleep(0.1) # stagger thread starts  
    
    for t in threads:
        t.join()
    end_time = time.perf_counter()

    print("Writing output:")
    with open("output1.txt", "w", encoding="utf-8", errors="replace") as f: # replace with ? when encoding error
        f.write(f"Crawling {len(parsed_dict)} pages finished in {end_time - start_time:.2f} seconds.\n")
        f.write(f"{len(parsed_dict)/(end_time - start_time):.2f} pages per second.\n\n")
        f.write("http errors encountered:\n")
        for error_code in errors_dict.keys():
            f.write(f"{error_code}: {errors_dict[error_code]}\n")
        f.write("\nParsed URLs:\n")
        for url, (timestamp, depth, size, code, score) in parsed_dict.items():
            f.write(
                f"link:{url:<100} "
                f"time:{timestamp:<23} "
                f"depth:{depth:<5} "
                f"size:{size:<15} "
                f"status:{code:<25}"
                f"score:{score:<.6f}\n"
            )
    print("Complete.")
    
if __name__ == "__main__":
    main()
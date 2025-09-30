import concurrent
import threading   
import time
from storage import *
from parse import *

def worker():
    target=1000
    tid = threading.get_ident()
    while get_num_parsed() < target:
        if get_num_parsed() % 100 == 0:
            print(f"Parsed {get_num_parsed()}/{target}")
        # check if we have links waiting to be parsed, if so pop and parse it
        if priority_heapq:
            print(f"Thread {tid} processing {get_num_parsed()}")
            temp = safe_heappop()
            if not temp:
                continue
            # give up on url if thread url takes too long to parse
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
    with open("output1.txt", "w", encoding="utf-8", errors="replace") as f: # replace with ? when encoding error
        f.write(f"Crawling {len(parsed_dict)} pages finished in {end_time - start_time:.2f} seconds.\n")
        f.write(f"{len(parsed_dict)/(end_time - start_time):.2f} pages per second.\n\n")
        f.write("http errors encountered:\n")
        for error_code in errors_dict.keys():
            f.write(f"{error_code}: {errors_dict[error_code]}\n")
        f.write("\nParsed URLs:\n")
        for url, (timestamp, depth, size, code) in parsed_dict.items():
            f.write(
                f"link:{url:<100} "
                f"time:{timestamp:<23} "
                f"depth:{depth:<5} "
                f"size:{size:<15} "
                f"status:{code:<15}\n"
            )
    
if __name__ == "__main__":
    main()
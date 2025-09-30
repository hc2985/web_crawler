Features:
- Parses urls in a BFS-like manner while supporting domain diversity
- Uses function: 1/(depth * log(2*(superdomain+domain+1))) - higher score = higher priority
- Complies with Robots.txt
- Handles only html/xhtml content (check quirk 3)
- Ignores content size > 1MB
- Maintains top 10000 urls in the priority heap for storage management
- Processes first 200 urls per parse for storage management
- Handles up to 10 redirects
- Utilizes multi-threading to speed up crawling
- Proper logging of details after crawling

How to Run:
- Run main.py to start using crawler.
- edit the variable target in worker function to set target number of webpages parsed.
- edit file name directly below the line "seeds = []" to set seed input file
- edit file name directly below the line "print("Writing output:")" to set output file name

Statistics:
- Run 1: 10006 pages run, 1478.09 seconds, 6.77 pages per second
- Run 2: 10005 pages run, 2732.62 seconds, 3.66 pages per second

Known Quirks (trivial bugs):
- parses up to # of threads pages more than target pages parsed ex:(target: 10000, threads: 10, 10000 <= total_parsed <= 10010)
- links blocked by robots.txt not logged as crawler will not attempt to parse
- if websites mark file as html or xhtml and present a xml file, the parser detects and parses as xml instead of ignoring
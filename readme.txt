### trivial bugs

- parses up to # of threads pages more than target pages parsed ex:(target: 10000, threads: 10, 10000 <= total_parsed <= 10010)
- links blocked by robots.txt not logged as crawler will not attempt to parse
- 
File description:
main.py:     Defines thread tasks, spawns and runs threads, logs progress to output file.
parse.py:    Handles URL parsing. Includes link normalization, robots.txt checks,
             duplicate checks, priority calculation, and saving log data for each link parsed.
query.py:    Queries the seed links from duckduckgo given input from user.
storage.py:  Manages all datastructures with functions that prevent race conditions.

How to Run:
- No config file required
- Install requirements listed in requirements.txt 
- Run main.py to start using crawler
- Type in query when asked for a query string in the terminal. This will be used to fetch seed links

Parameters:
- Edit the global variable 'target' on top of 'main.py' to set target number of webpages parsed.
- Edit the global variable 'num_threads'  on top of 'main.py' to set number of threads and seed links.
- Set output file name at bottom of the main function on the bottom of 'main.py'.
- Type query into terminal to fetch seed links.



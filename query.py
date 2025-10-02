from ddgs import DDGS

ddgs = DDGS()

def get_seed_links(query, num=10):
    return [r["href"] for r in ddgs.text(query, max_results=num)]

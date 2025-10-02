import re
import matplotlib.pyplot as plt

# path to your file
filename = "output1.txt"

scores = []

with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        # look for scores from the output file
        match = re.search(r"score:([0-9.]+)", line)
        if match:
            scores.append(float(match.group(1)))

print(f"Extracted {len(scores)} scores")

# Plot line graph that shows scores over completed order
plt.plot(scores, marker='.')
plt.xlabel("crawl complete order")
plt.ylabel("Score")
plt.title("Scores Across Crawl Complete Order")
plt.show()

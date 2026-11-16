"""Small program to check duplicacy in the streaming input.

Reads every JSON-lines file in input/ and reports how many events are
duplicates (same event_id seen more than once).
"""
import glob
import json
from collections import Counter

counts = Counter()
total = 0
for path in sorted(glob.glob("input/*.json")):
    with open(path) as f:
        for line in f:
            if line.strip():
                counts[json.loads(line)["event_id"]] += 1
                total += 1

duplicates = {k: v for k, v in counts.items() if v > 1}
print(f"total events        : {total}")
print(f"unique events       : {len(counts)}")
print(f"duplicate events    : {total - len(counts)}")
for event_id, c in sorted(duplicates.items()):
    print(f"  {event_id} appears {c} times")

"""Writes one JSON-lines batch file into input/ (the streaming source).

Each run simulates a new batch of events arriving. About 15% of events are
re-sent duplicates (same event_id), so de-duplication can be demonstrated.

Usage: python generate_events.py [number_of_events]
"""
import json
import os
import random
import sys
import time
from datetime import datetime

os.makedirs("input", exist_ok=True)
n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
categories = ["electronics", "grocery", "clothing", "books", "toys"]

# Remember ids already sent, to be able to repeat some of them (duplicates)
seen_file = "input/.sent_ids.json"
sent = json.load(open(seen_file)) if os.path.exists(seen_file) else []

events = []
for _ in range(n):
    if sent and random.random() < 0.15:
        events.append(random.choice(sent))                 # duplicate
    else:
        e = {
            "event_id": f"e{len(sent) + 1:06d}",
            "category": random.choice(categories),
            "amount": round(random.uniform(5, 500), 2),
            "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        sent.append(e)
        events.append(e)

json.dump(sent, open(seen_file, "w"))
name = f"input/events_{int(time.time() * 1000)}.json"
with open(name, "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")
print(f"wrote {len(events)} events to {name}")

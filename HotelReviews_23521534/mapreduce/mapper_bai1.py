#!/usr/bin/env python3
import sys, os

STOPWORDS = set()
if os.path.exists("stopwords.txt"):
    with open("stopwords.txt", encoding="utf-8") as f:
        for line in f:
            w = line.strip()
            if w: STOPWORDS.add(w)

first = True
for line in sys.stdin:
    line = line.strip()
    if first:
        first = False
        if line.startswith("id;"): continue
    parts = line.split(";")
    if len(parts) < 5: continue
    rid, review, aspect, category, sentiment = parts[:5]
    if rid == "id": continue
    tokens = review.lower().split()
    filtered = [t.strip('.,!?;:"\'()') for t in tokens if t not in STOPWORDS and len(t) > 1]
    print(f"{rid}\t{' '.join(filtered)}\t{aspect}\t{category}\t{sentiment}")

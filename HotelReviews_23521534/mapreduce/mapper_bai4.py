#!/usr/bin/env python3
import sys
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 5: continue
    tokens_str, category, sentiment = parts[1], parts[3], parts[4]
    if sentiment not in ("positive","negative"): continue
    for word in tokens_str.split():
        clean = word.strip('.,!?;:"\'()')
        if clean and len(clean) > 1:
            print(f"{category}|{sentiment}|{clean}\t1")

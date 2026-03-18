#!/usr/bin/env python3
import sys
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 4: continue
    tokens_str, category = parts[1], parts[3]
    for word in tokens_str.split():
        clean = word.strip('.,!?;:"\'()')
        if clean and len(clean) > 1:
            print(f"{category}|{clean}\t1")

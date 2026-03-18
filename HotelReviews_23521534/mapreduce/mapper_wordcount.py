#!/usr/bin/env python3
import sys
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 2: continue
    for word in parts[1].split():
        clean = word.strip('.,!?;:"\'()')
        if clean and len(clean) > 1:
            print(f"{clean}\t1")

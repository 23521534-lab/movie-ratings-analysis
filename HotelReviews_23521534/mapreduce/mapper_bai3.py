#!/usr/bin/env python3
import sys
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 5: continue
    aspect, sentiment = parts[2].strip(), parts[4].strip()
    if aspect and sentiment in ("positive","negative","neutral"):
        print(f"{aspect}\t{sentiment}\t1")

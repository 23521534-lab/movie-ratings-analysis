#!/usr/bin/env python3
import sys
cur_key, cur_count = None, 0
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3: continue
    key = f"{parts[0]}\t{parts[1]}"
    count = int(parts[2])
    if key == cur_key:
        cur_count += count
    else:
        if cur_key: print(f"{cur_key}\t{cur_count}")
        cur_key, cur_count = key, count
if cur_key: print(f"{cur_key}\t{cur_count}")

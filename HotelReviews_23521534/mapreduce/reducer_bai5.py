#!/usr/bin/env python3
import sys
cur_key, cur_count = None, 0
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2: continue
    key, count = parts[0], int(parts[1])
    if key == cur_key:
        cur_count += count
    else:
        if cur_key: print(f"{cur_key}\t{cur_count}")
        cur_key, cur_count = key, count
if cur_key: print(f"{cur_key}\t{cur_count}")

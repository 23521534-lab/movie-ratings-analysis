#!/usr/bin/env python3
import sys
cur_word, cur_count = None, 0
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2: continue
    word, count = parts[0], int(parts[1])
    if word == cur_word:
        cur_count += count
    else:
        if cur_word: print(f"{cur_word}\t{cur_count}")
        cur_word, cur_count = word, count
if cur_word: print(f"{cur_word}\t{cur_count}")

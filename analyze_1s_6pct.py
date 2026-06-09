#!/usr/bin/env python3
import os
import csv

DIR = 'data/klines/1s'
THRESH = 0.06

if not os.path.isdir(DIR):
    print('No directory', DIR)
    raise SystemExit(1)

files = [f for f in os.listdir(DIR) if f.endswith('.csv')]
if not files:
    print('No 1s files found in', DIR)
    raise SystemExit(1)

summary = {}
for fn in sorted(files):
    path = os.path.join(DIR, fn)
    total = 0
    cnt_high = 0
    cnt_low = 0
    cnt_either = 0
    timestamps = []
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                o = float(row['open'])
                h = float(row['high'])
                l = float(row['low'])
            except Exception:
                continue
            if o == 0:
                continue
            total += 1
            high_move = (h - o) / o >= THRESH
            low_move = (o - l) / o >= THRESH
            if high_move:
                cnt_high += 1
                timestamps.append((row['timestamp_ms'], 'high', (h-o)/o))
            if low_move:
                cnt_low += 1
                timestamps.append((row['timestamp_ms'], 'low', (o-l)/o))
            if high_move or low_move:
                cnt_either += 1
    summary[fn] = {
        'total': total,
        'high': cnt_high,
        'low': cnt_low,
        'either': cnt_either,
        'pct_either': (cnt_either/total*100) if total else 0,
        'sample_ts': timestamps[:10],
    }

for fn, s in summary.items():
    print(fn)
    print(f"  total_seconds={s['total']}, high_count={s['high']}, low_count={s['low']}, either_count={s['either']}, either_pct={s['pct_either']:.3f}%")
    if s['sample_ts']:
        print('  sample_events (timestamp_ms,type,move):')
        for t in s['sample_ts']:
            print('   ', t)
print('Done')

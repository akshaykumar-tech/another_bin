#!/usr/bin/env python3
import os
import csv

EVENT_CSV = 'data/analysis/6pct_events/SAHARAUSDT_6pct_events.csv'
DATA_1S = 'data/klines/1s/SAHARAUSDT.csv'
WINDOW = 60 # seconds
PRE_THRESH = 0.02 # 2%

# load 1s data into dict
ts_map = {}
with open(DATA_1S,'r') as f:
    reader = csv.DictReader(f)
    for r in reader:
        ts_map[int(r['timestamp_ms'])] = r

events = []
with open(EVENT_CSV,'r') as f:
    reader = csv.DictReader(f)
    for r in reader:
        events.append({'timestamp_ms': int(r['timestamp_ms']), 'type': r['type']})

matched = []
for ev in events:
    e_ts = ev['timestamp_ms']
    ev_type = ev['type']
    start = e_ts - WINDOW*1000
    found = False
    found_ts = None
    for s in range(start, e_ts, 1000):
        row = ts_map.get(s)
        if not row:
            continue
        o = float(row['open'])
        h = float(row['high'])
        l = float(row['low'])
        if o == 0:
            continue
        if ev_type == 'high':
            if (h - o)/o >= PRE_THRESH:
                found = True
                found_ts = s
                break
        else:
            if (o - l)/o >= PRE_THRESH:
                found = True
                found_ts = s
                break
    matched.append({'event_ts': e_ts, 'event_type': ev_type, 'pre_found': found, 'pre_ts': found_ts})

count_matched = sum(1 for m in matched if m['pre_found'])
print(f'total_events={len(matched)}, matched_pre_signals={count_matched}')
for m in matched:
    print(m)

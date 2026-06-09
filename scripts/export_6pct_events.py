#!/usr/bin/env python3
import os
import csv
from datetime import datetime

DIR = 'data/klines/1s'
OUT_DIR = 'data/analysis/6pct_events'
THRESH = 0.06

os.makedirs(OUT_DIR, exist_ok=True)
combined_path = os.path.join(OUT_DIR, 'all_events.csv')

fieldnames = ['symbol','timestamp_ms','timestamp_utc','type','move_frac','open','high','low','close','volume']
with open(combined_path, 'w', newline='') as combined_f:
    writer_all = csv.DictWriter(combined_f, fieldnames=fieldnames)
    writer_all.writeheader()

    for fn in sorted(os.listdir(DIR)):
        if not fn.endswith('.csv'):
            continue
        symbol = fn.replace('.csv','')
        infile = os.path.join(DIR, fn)
        outfile = os.path.join(OUT_DIR, f'{symbol}_6pct_events.csv')
        found = 0
        with open(infile, 'r') as f, open(outfile, 'w', newline='') as out_f:
            reader = csv.DictReader(f)
            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                try:
                    o = float(row['open'])
                    h = float(row['high'])
                    l = float(row['low'])
                    c = float(row.get('close', o))
                    vol = row.get('volume', '')
                except Exception:
                    continue
                if o == 0:
                    continue
                high_move_frac = (h - o) / o
                low_move_frac = (o - l) / o
                ts = int(row['timestamp_ms'])
                ts_utc = datetime.utcfromtimestamp(ts/1000.0).isoformat()
                if high_move_frac >= THRESH:
                    rec = {'symbol': symbol, 'timestamp_ms': ts, 'timestamp_utc': ts_utc, 'type':'high', 'move_frac': f'{high_move_frac:.8f}', 'open':row['open'], 'high':row['high'], 'low':row['low'], 'close':row.get('close',''), 'volume':vol}
                    writer.writerow(rec)
                    writer_all.writerow(rec)
                    found += 1
                if low_move_frac >= THRESH:
                    rec = {'symbol': symbol, 'timestamp_ms': ts, 'timestamp_utc': ts_utc, 'type':'low', 'move_frac': f'{low_move_frac:.8f}', 'open':row['open'], 'high':row['high'], 'low':row['low'], 'close':row.get('close',''), 'volume':vol}
                    writer.writerow(rec)
                    writer_all.writerow(rec)
                    found += 1
        print(f'Wrote {found} events to {outfile}')

print('Combined events:', combined_path)

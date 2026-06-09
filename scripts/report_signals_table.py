#!/usr/bin/env python3
import os
import csv
from datetime import datetime

EVENTS_DIR = 'data/analysis/6pct_events'
DATA_1S_DIR = 'data/klines/1s'
OUT = 'data/analysis/6pct_events/signals_report.csv'
WINDOW = 60
PRE_THRESH = 0.02

os.makedirs(os.path.dirname(OUT), exist_ok=True)

rows_out = []
for fn in sorted(os.listdir(EVENTS_DIR)):
    if not fn.endswith('_6pct_events.csv'):
        continue
    symbol = fn.replace('_6pct_events.csv','')
    events_path = os.path.join(EVENTS_DIR, fn)
    data_path = os.path.join(DATA_1S_DIR, f'{symbol}.csv')
    if not os.path.exists(data_path):
        continue
    # load 1s data into dict
    ts_map = {}
    with open(data_path,'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            ts_map[int(r['timestamp_ms'])] = r
    # read events
    with open(events_path,'r') as f:
        reader = csv.DictReader(f)
        for ev in reader:
            e_ts = int(ev['timestamp_ms'])
            e_type = ev['type']
            # find pre-signal
            start = e_ts - WINDOW*1000
            signal_found = False
            signal_ts = None
            entry_price = ''
            for s in range(start, e_ts, 1000):
                r = ts_map.get(s)
                if not r:
                    continue
                o = float(r['open'])
                h = float(r['high'])
                l = float(r['low'])
                if o == 0:
                    continue
                if e_type == 'high':
                    if (h - o)/o >= PRE_THRESH:
                        signal_found = True
                        signal_ts = s
                        entry_price = o
                        break
                else:
                    if (o - l)/o >= PRE_THRESH:
                        signal_found = True
                        signal_ts = s
                        entry_price = o
                        break
            # get event peak price
            # event file included open/high/low/close/volume
            ev_open = float(ev.get('open',0) or 0)
            ev_high = float(ev.get('high',0) or 0)
            ev_low = float(ev.get('low',0) or 0)
            if e_type == 'high':
                peak = ev_high
                move_pct = ((peak - entry_price)/entry_price*100) if signal_found and entry_price else ''
            else:
                peak = ev_low
                move_pct = ((entry_price - peak)/entry_price*100) if signal_found and entry_price else ''
            rows_out.append({
                'symbol': symbol,
                'event_ts_ms': e_ts,
                'event_ts_utc': datetime.utcfromtimestamp(e_ts/1000).isoformat(),
                'event_type': e_type,
                'signal_found': int(signal_found),
                'signal_ts_ms': signal_ts or '',
                'signal_ts_utc': datetime.utcfromtimestamp(signal_ts/1000).isoformat() if signal_ts else '',
                'entry_price': f"{entry_price:.8f}" if entry_price!='' else '',
                'peak_price': f"{peak:.8f}" if peak else '',
                'move_pct': f"{move_pct:.6f}" if move_pct!='' else '',
                'exit_ts_ms': e_ts,
                'exit_ts_utc': datetime.utcfromtimestamp(e_ts/1000).isoformat(),
            })

# write CSV
keys = ['symbol','event_ts_utc','event_ts_ms','event_type','signal_found','signal_ts_utc','signal_ts_ms','entry_price','peak_price','move_pct','exit_ts_utc','exit_ts_ms']
with open(OUT,'w',newline='') as f:
    writer = csv.DictWriter(f, fieldnames=keys)
    writer.writeheader()
    for r in rows_out:
        writer.writerow(r)

print('Wrote report to', OUT)
# print simple table
for r in rows_out:
    print(r['symbol'], r['event_ts_utc'], 'type='+r['event_type'], 'signal_found='+str(r['signal_found']), 'signal_ts='+r['signal_ts_utc'], 'entry='+r['entry_price'], 'move_pct='+r['move_pct'], 'exit='+r['exit_ts_utc'])

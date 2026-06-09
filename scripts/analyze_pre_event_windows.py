#!/usr/bin/env python3
import os
import csv
import random
from datetime import datetime
import math

DATA_DIR = 'data/klines/1s'
EVENTS_DIR = 'data/analysis/6pct_events'
OUT_DIR = 'data/analysis/pre_event_features'
WINDOW = 60  # seconds
NEG_MULT = 5  # number of negative windows per positive
THRESH_SEARCH = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06]

os.makedirs(OUT_DIR, exist_ok=True)

# load 1s data into dict of {symbol: {ts: row_dict}}
print('Loading 1s data...')
data = {}
for fn in os.listdir(DATA_DIR):
    if not fn.endswith('.csv'):
        continue
    symbol = fn.replace('.csv','')
    path = os.path.join(DATA_DIR, fn)
    ts_map = {}
    with open(path,'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            ts = int(r['timestamp_ms'])
            ts_map[ts] = r
    data[symbol] = ts_map

# for each symbol with events, extract event timestamps
print('Loading events...')
event_files = [f for f in os.listdir(EVENTS_DIR) if f.endswith('_6pct_events.csv')]
all_features = []
for ef in event_files:
    symbol = ef.split('_6pct_events.csv')[0]
    if symbol not in data:
        continue
    with open(os.path.join(EVENTS_DIR, ef),'r') as f:
        reader = csv.DictReader(f)
        events = [int(row['timestamp_ms']) for row in reader]
    if not events:
        continue
    ts_keys = sorted(data[symbol].keys())
    ts_min, ts_max = ts_keys[0], ts_keys[-1]

    # positive windows
    pos_feats = []
    for e_ts in events:
        start = e_ts - WINDOW*1000
        # collect WINDOW rows at 1s resolution
        rows = []
        ok = True
        for s in range(start, e_ts, 1000):
            if s in data[symbol]:
                rows.append(data[symbol][s])
            else:
                ok = False
                break
        if not ok or len(rows) < WINDOW:
            continue
        # compute features
        opens = [float(r['open']) for r in rows]
        highs = [float(r['high']) for r in rows]
        lows = [float(r['low']) for r in rows]
        vols = [float(r['volume']) for r in rows]
        returns = [(highs[i]-opens[i])/opens[i] for i in range(len(rows))]
        neg_returns = [(opens[i]-lows[i])/opens[i] for i in range(len(rows))]
        feat = {
            'symbol': symbol,
            'event_ts': e_ts,
            'start_ts': start,
            'price_change_60s': (opens[-1]-opens[0])/opens[0],
            'max_return_60s': max(returns),
            'max_neg_return_60s': max(neg_returns),
            'std_return': (sum((x-(sum(returns)/len(returns)))**2 for x in returns)/len(returns))**0.5,
            'vol_sum_60s': sum(vols),
            'vol_max_10s': max([sum(vols[i:i+10]) for i in range(max(1,len(vols)-9))]),
            'nonzero_secs': sum(1 for v in vols if v>0),
        }
        pos_feats.append(feat)

    # negative windows: sample random starts, avoid within WINDOW of events
    neg_feats = []
    attempts = 0
    needed = len(pos_feats)*NEG_MULT
    while len(neg_feats) < needed and attempts < needed*10:
        attempts += 1
        # pick random ts aligned to second within data range
        s_ts = random.choice(ts_keys)
        if s_ts + WINDOW*1000 > ts_max:
            continue
        # exclude if within WINDOW of any event
        if any(abs(s_ts - e) <= WINDOW*1000 for e in events):
            continue
        rows = []
        ok = True
        for s in range(s_ts, s_ts+WINDOW*1000, 1000):
            if s in data[symbol]:
                rows.append(data[symbol][s])
            else:
                ok = False
                break
        if not ok or len(rows) < WINDOW:
            continue
        opens = [float(r['open']) for r in rows]
        highs = [float(r['high']) for r in rows]
        lows = [float(r['low']) for r in rows]
        vols = [float(r['volume']) for r in rows]
        returns = [(highs[i]-opens[i])/opens[i] for i in range(len(rows))]
        neg_returns = [(opens[i]-lows[i])/opens[i] for i in range(len(rows))]
        feat = {
            'symbol': symbol,
            'event_ts': None,
            'start_ts': s_ts,
            'price_change_60s': (opens[-1]-opens[0])/opens[0],
            'max_return_60s': max(returns),
            'max_neg_return_60s': max(neg_returns),
            'std_return': (sum((x-(sum(returns)/len(returns)))**2 for x in returns)/len(returns))**0.5,
            'vol_sum_60s': sum(vols),
            'vol_max_10s': max([sum(vols[i:i+10]) for i in range(max(1,len(vols)-9))]),
            'nonzero_secs': sum(1 for v in vols if v>0),
        }
        neg_feats.append(feat)

    # write pos and neg CSVs
    out_pos = os.path.join(OUT_DIR, f'{symbol}_pos_features.csv')
    out_neg = os.path.join(OUT_DIR, f'{symbol}_neg_features.csv')
    keys = ['symbol','event_ts','start_ts','price_change_60s','max_return_60s','max_neg_return_60s','std_return','vol_sum_60s','vol_max_10s','nonzero_secs']
    with open(out_pos,'w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=keys)
        writer.writeheader()
        for r in pos_feats:
            writer.writerow(r)
    with open(out_neg,'w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=keys)
        writer.writeheader()
        for r in neg_feats:
            writer.writerow(r)

    print(f'{symbol}: pos={len(pos_feats)}, neg={len(neg_feats)} written')

    all_features.extend([(1, r) for r in pos_feats])
    all_features.extend([(0, r) for r in neg_feats])

# basic gate search: try simple thresholds on vol_sum_60s and max_return_60s
print('\nSearching simple gates...')
if not all_features:
    print('no features')
    raise SystemExit(0)

# convert to lists
labels = [lab for lab,_ in all_features]
feats = [f for _,f in all_features]

# examine ranges
vols = [f['vol_sum_60s'] for f in feats]
mxr = [f['max_return_60s'] for f in feats]
mxn = [f['max_neg_return_60s'] for f in feats]

print('samples:', len(feats))
print('vol_sum_60s range:', min(vols), max(vols))
print('max_return_60s range:', min(mxr), max(mxr))
print('max_neg_return_60s range:', min(mxn), max(mxn))

# try gates: vol_sum_60s > V and max_return_60s > R (or max_neg_return_60s > R)
best = None
for V in [0, 10, 100, 1000, 10000, 100000, 1000000]:
    for R in [0.02,0.03,0.04,0.05,0.06,0.08,0.1]:
        tp=fp=tn=fnc=0
        for i,f in enumerate(feats):
            pred = (f['vol_sum_60s']>V) and (f['max_return_60s']>R or f['max_neg_return_60s']>R)
            if labels[i]==1 and pred: tp+=1
            if labels[i]==1 and not pred: fnc+=1
            if labels[i]==0 and pred: fp+=1
            if labels[i]==0 and not pred: tn+=1
        tpr = tp/(tp+fnc) if (tp+fnc)>0 else 0
        fpr = fp/(fp+tn) if (fp+tn)>0 else 0
        score = tpr - fpr
        if best is None or score>best[0]:
            best = (score, V, R, tp, fp, tn, fnc, tpr, fpr)

print('Best gate (maximize TPR-FPR): score,V,R,tp,fp,tn,fn,tpr,fpr')
print(best)

# save combined features CSV
comb_out = os.path.join(OUT_DIR,'combined_pre_event_features.csv')
keys_comb = ['label'] + keys
with open(comb_out,'w',newline='') as f:
    writer = csv.DictWriter(f,fieldnames=keys_comb)
    writer.writeheader()
    for lab, r in all_features:
        rr = {'label': lab}
        rr.update({k: r[k] for k in keys if k in r})
        writer.writerow(rr)
print('Wrote combined features to', comb_out)
print('Done')

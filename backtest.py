import json
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime
from collections import defaultdict

# Safe JSON deserializer to handle empty or malformed messages
def safe_json_deserializer(m):
    try:
        if not m:
            return None
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"[WARN] Skipping malformed message: {e}")
        return None

# Kafka consumer setup
consumer = KafkaConsumer(
    'mock_l1_stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=safe_json_deserializer,
    auto_offset_reset='earliest'
)

# Order parameters
TOTAL_SHARES = 5000
STEP_SIZE = 100  # Allocation step size

def parse_time(ts):
    if '.' in ts:
        base, frac = ts.split('.')
        frac = frac.rstrip('Z')
        if len(frac) > 6:
            frac = frac[:6]
        return datetime.strptime(f"{base}.{frac}", "%Y-%m-%dT%H:%M:%S.%f")
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

def allocate(order_size, venues, lambda_over, lambda_under, theta_queue, step=100):
    n = len(venues)
    splits = [[]]
    for i in range(n):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[i]['ask_sz_00'])
            q = 0
            while q <= max_v:
                new_splits.append(alloc + [q])
                q += step
            if q - step < max_v:
                new_splits.append(alloc + [max_v])
        splits = new_splits
    best_cost = float('inf')
    best_split = None
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta_queue)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    return best_split, best_cost

def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta_queue):
    executed = 0
    cash_spent = 0
    for i, qty in enumerate(split):
        exe = min(qty, venues[i]['ask_sz_00'])
        executed += exe
        cash_spent += exe * venues[i]['ask_px_00']
        if qty > exe:
            cash_spent -= (qty - exe) * 0
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_penalty = theta_queue * (underfill + overfill)
    cost_penalty = lambda_under * underfill + lambda_over * overfill
    return cash_spent + risk_penalty + cost_penalty

def best_ask_execution(remaining, venues):
    best_price = min(v['ask_px_00'] for v in venues)
    total_size = sum(v['ask_sz_00'] for v in venues if v['ask_px_00'] == best_price)
    fill = min(remaining, total_size)
    cash = fill * best_price
    return fill, cash

def twap_execution(remaining, timestamps, current_idx, venues, total_chunks=60):
    chunks = [TOTAL_SHARES // total_chunks] * total_chunks
    remainder = TOTAL_SHARES % total_chunks
    for i in range(remainder):
        chunks[i] += 1
    total_seconds = (timestamps[-1] - timestamps[0]).total_seconds()
    interval = total_seconds / total_chunks
    current_time = timestamps[current_idx]
    due_chunks = []
    for i in range(total_chunks):
        chunk_time = timestamps[0] + i * interval
        if chunk_time <= current_time:
            due_chunks.append(chunks[i])
    total_fill = 0
    total_cash = 0
    for chunk in due_chunks:
        fill, cash = best_ask_execution(min(chunk, remaining), venues)
        total_fill += fill
        total_cash += cash
        remaining -= fill
        if remaining <= 0:
            break
    return total_fill, total_cash

def vwap_execution(remaining, venues):
    best_price = min(v['ask_px_00'] for v in venues)
    eligible_venues = [v for v in venues if v['ask_px_00'] == best_price]
    total_volume = sum(v['ask_sz_00'] for v in eligible_venues)
    if total_volume == 0:
        return 0, 0
    total_fill = 0
    total_cash = 0
    for venue in eligible_venues:
        portion = venue['ask_sz_00'] / total_volume
        to_fill = min(venue['ask_sz_00'], int(portion * remaining))
        total_fill += to_fill
        total_cash += to_fill * venue['ask_px_00']
        remaining -= to_fill
        if remaining <= 0:
            break
    return total_fill, total_cash

def run_backtest():
    snapshots = defaultdict(list)
    timestamps = []

    print("Consuming market data from Kafka...")
    for message in consumer:
        snapshot = message.value
        if snapshot is None:
            continue  # skip empty or bad messages
        ts = parse_time(snapshot['ts_event'])
        snapshots[ts].append(snapshot)
        if ts not in timestamps:
            timestamps.append(ts)

    timestamps.sort()
    print(f"Loaded {len(timestamps)} snapshots")

    param_grid = {
        'lambda_over': np.linspace(0.1, 1.0, 5),
        'lambda_under': np.linspace(0.1, 1.0, 5),
        'theta_queue': np.linspace(0.1, 1.0, 5)
    }

    best_cost = float('inf')
    best_params = None
    results = {}

    print("Running parameter optimization...")
    for lo in param_grid['lambda_over']:
        for lu in param_grid['lambda_under']:
            for tq in param_grid['theta_queue']:
                remaining = TOTAL_SHARES
                cash = 0
                fills = []
                for ts in timestamps:
                    if remaining <= 0:
                        break
                    venues = [{
                        'ask_px_00': v['ask_px_00'],
                        'ask_sz_00': v['ask_sz_00']
                    } for v in snapshots[ts]]
                    allocation, _ = allocate(remaining, venues, lo, lu, tq, STEP_SIZE)
                    for i, venue in enumerate(venues):
                        qty = allocation[i]
                        fill = min(qty, venue['ask_sz_00'])
                        cash += fill * venue['ask_px_00']
                        remaining -= fill
                        fills.append(fill)
                if cash < best_cost:
                    best_cost = cash
                    best_params = (lo, lu, tq)
                    results[(lo, lu, tq)] = cash

    print("Running optimized strategy...")
    lo_opt, lu_opt, tq_opt = best_params
    remaining = TOTAL_SHARES
    cash_opt = 0
    for ts in timestamps:
        if remaining <= 0:
            break
        venues = [{
            'ask_px_00': v['ask_px_00'],
            'ask_sz_00': v['ask_sz_00']
        } for v in snapshots[ts]]
        allocation, _ = allocate(remaining, venues, lo_opt, lu_opt, tq_opt, STEP_SIZE)
        for i, venue in enumerate(venues):
            qty = allocation[i]
            fill = min(qty, venue['ask_sz_00'])
            cash_opt += fill * venue['ask_px_00']
            remaining -= fill

    print("Running baseline strategies...")
    remaining = TOTAL_SHARES
    cash_best_ask = 0
    for ts in timestamps:
        if remaining <= 0:
            break
        venues = [v for v in snapshots[ts]]
        fill, cash = best_ask_execution(remaining, venues)
        cash_best_ask += cash
        remaining -= fill

    remaining = TOTAL_SHARES
    cash_twap = 0
    for idx, ts in enumerate(timestamps):
        if remaining <= 0:
            break
        venues = [v for v in snapshots[ts]]
        fill, cash = twap_execution(remaining, timestamps, idx, venues)
        cash_twap += cash
        remaining -= fill

    remaining = TOTAL_SHARES
    cash_vwap = 0
    for ts in timestamps:
        if remaining <= 0:
            break
        venues = [v for v in snapshots[ts]]
        fill, cash = vwap_execution(remaining, venues)
        cash_vwap += cash
        remaining -= fill

    avg_fill_opt = cash_opt / TOTAL_SHARES
    savings_best_ask = (cash_best_ask - cash_opt) / cash_opt * 10000
    savings_twap = (cash_twap - cash_opt) / cash_opt * 10000
    savings_vwap = (cash_vwap - cash_opt) / cash_opt * 10000

    return {
        "best_parameters": {
            "lambda_over": float(lo_opt),
            "lambda_under": float(lu_opt),
            "theta_queue": float(tq_opt)
        },
        "optimized": {
            "total_cash": float(cash_opt),
            "avg_fill_px": float(avg_fill_opt)
        },
        "baselines": {
            "best_ask": {
                "total_cash": float(cash_best_ask),
                "avg_fill_px": float(cash_best_ask / TOTAL_SHARES)
            },
            "twap": {
                "total_cash": float(cash_twap),
                "avg_fill_px": float(cash_twap / TOTAL_SHARES)
            },
            "vwap": {
                "total_cash": float(cash_vwap),
                "avg_fill_px": float(cash_vwap / TOTAL_SHARES)
            }
        },
        "savings_vs_baselines_bps": {
            "best_ask": float(savings_best_ask),
            "twap": float(savings_twap),
            "vwap": float(savings_vwap)
        }
    }

if __name__ == "__main__":
    results = run_backtest()
    print(json.dumps(results, indent=2))

import json
import numpy as np
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime, timedelta
from collections import defaultdict
import time

# Safe JSON deserializer
def safe_json_deserializer(m):
    try:
        if not m:
            return None
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"[WARN] Skipping malformed message: {e}")
        return None

# Kafka consumer
consumer = KafkaConsumer(
    'mock_l1_stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=safe_json_deserializer,
    auto_offset_reset='earliest'
)

TOTAL_SHARES = 5000
STEP_SIZE = 100

def parse_time(ts):
    if '.' in ts:
        base, frac = ts.split('.')
        frac = frac.rstrip('Z')
        if len(frac) > 6:
            frac = frac[:6]
        return datetime.strptime(f"{base}.{frac}", "%Y-%m-%dT%H:%M:%S.%f")
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

def allocate(order_size, venues, lambda_over, lambda_under, theta_queue, step=100, slippage_coeff=0.01):
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
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta_queue, slippage_coeff)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    return best_split, best_cost

def greedy_allocate(order_size, venues, lambda_over, lambda_under, theta_queue, step=100, slippage_coeff=0.01):
    """Penalty-aware greedy allocation that properly considers parameter impacts"""
    # Create a cost matrix that considers penalties
    cost_matrix = []
    for i, venue in enumerate(venues):
        for fill in range(step, min(venue['ask_sz_00'], order_size) + 1, step):
            # Calculate execution cost
            slippage = slippage_coeff * (fill / venue['ask_sz_00'])
            price_impact = venue['ask_px_00'] * (1 + slippage)
            cash_cost = fill * price_impact
            
            # Calculate penalty impact - KEY FIX
            remaining_after = order_size - fill
            penalty = (theta_queue + lambda_under + lambda_over) * remaining_after
            
            # Total cost with penalty impact
            total_cost = cash_cost + penalty
            cost_per_share = total_cost / fill
            
            cost_matrix.append((cost_per_share, total_cost, i, fill))
    
    # Sort by cost per share
    cost_matrix.sort(key=lambda x: x[0])
    
    remaining = order_size
    allocation = [0] * len(venues)
    total_cost = 0
    
    for _, cost_val, i, fill in cost_matrix:
        if remaining <= 0:
            break
            
        # Only use if venue has capacity
        available = venues[i]['ask_sz_00'] - allocation[i]
        if available <= 0:
            continue
            
        # Take the fill that fits
        actual_fill = min(fill, remaining, available)
        allocation[i] += actual_fill
        remaining -= actual_fill
        total_cost += cost_val * (actual_fill / fill)  # Prorate cost
        
    # Fill any remaining with best available
    if remaining > 0:
        best_venue = min(range(len(venues)), 
                        key=lambda i: venues[i]['ask_px_00'] if venues[i]['ask_sz_00'] > allocation[i] else float('inf'))
        available = venues[best_venue]['ask_sz_00'] - allocation[best_venue]
        if available > 0:
            fill = min(remaining, available)
            allocation[best_venue] += fill
            remaining -= fill
            slippage = slippage_coeff * (fill / venues[best_venue]['ask_sz_00'])
            price_impact = venues[best_venue]['ask_px_00'] * (1 + slippage)
            total_cost += fill * price_impact
    
    # Add final penalty for any remaining shares
    if remaining > 0:
        penalty = (theta_queue + lambda_under + lambda_over) * remaining
        total_cost += penalty
        
    return allocation, total_cost

def compute_cost(allocation, venues, order_size, lambda_over, lambda_under, theta_queue, slippage_coeff=0.01):
    """Correct cost calculation without exponential scaling"""
    executed = 0
    cash_spent = 0
    
    # Calculate base execution costs
    for i, qty in enumerate(allocation):
        fill = min(qty, venues[i]['ask_sz_00'])
        if fill <= 0:
            continue
        slippage = slippage_coeff * (fill / venues[i]['ask_sz_00'])
        effective_price = venues[i]['ask_px_00'] * (1 + slippage)
        cash_spent += fill * effective_price
        executed += fill
    
    # Calculate penalties (in dollars)
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    
    penalty = (
        theta_queue * (underfill + overfill) + 
        lambda_under * underfill + 
        lambda_over * overfill
    )
    
    # Return cash spent plus penalties (both in dollars)
    return cash_spent + penalty


def best_ask_execution(remaining, venues):
    best_price = min(v['ask_px_00'] for v in venues)
    total_size = sum(v['ask_sz_00'] for v in venues if v['ask_px_00'] == best_price)
    fill = min(remaining, total_size)
    cash = fill * best_price
    return fill, cash
def twap_execution(remaining, timestamps, current_idx, venues, total_chunks=60):
    """Execute using TWAP strategy with proper datetime handling"""
    if not timestamps or remaining <= 0:
        return 0, 0
    
    # Calculate chunks
    chunks = [TOTAL_SHARES // total_chunks] * total_chunks
    for i in range(TOTAL_SHARES % total_chunks):
        chunks[i] += 1
    
    # Calculate time intervals in seconds
    total_seconds = (timestamps[-1] - timestamps[0]).total_seconds()
    interval_seconds = total_seconds / total_chunks
    
    current_time = timestamps[current_idx]
    elapsed_seconds = (current_time - timestamps[0]).total_seconds()
    
    # Determine how many chunks we should have executed by now
    chunks_to_execute = min(int(elapsed_seconds / interval_seconds) + 1, total_chunks)
    
    total_fill = 0
    total_cash = 0
    
    # Execute the due chunks
    for i in range(chunks_to_execute):
        if remaining <= 0:
            break
        # Get the chunk size (protect against index error)
        chunk = chunks[i] if i < len(chunks) else 0
        fill, cash = best_ask_execution(min(chunk, remaining), venues)
        total_fill += fill
        total_cash += cash
        remaining -= fill
    
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
    no_message_timeout = 5
    start_time = time.time()

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=500)
            if not msg_pack:
                if time.time() - start_time > no_message_timeout:
                    break
                continue
            start_time = time.time()
            for tp, messages in msg_pack.items():
                for message in messages:
                    snapshot = message.value
                    if snapshot is None:
                        continue
                    ts = parse_time(snapshot['ts_event'])
                    snapshots[ts].append(snapshot)
                    if ts not in timestamps:
                        timestamps.append(ts)
    except KafkaError as e:
        print(f"[ERROR] Kafka error: {e}")
    finally:
        consumer.close()

    timestamps.sort()
    print(f"\nLoaded {len(timestamps)} snapshots")

    # Impactful parameter ranges (50-500)
    param_grid = {
        'lambda_over': np.linspace(1, 10.0, 5),
        'lambda_under': np.linspace(1, 10.0, 5),
        'theta_queue': np.linspace(1, 10.0, 5)
    }

    slippage_coeff = 0.01
    best_cost = float('inf')
    best_params = None
    all_results = []

    print("\nRunning parameter optimization with impactful penalties...")
    print(f"Testing {len(param_grid['lambda_over'])*len(param_grid['lambda_under'])*len(param_grid['theta_queue'])} combinations")

    for lo in param_grid['lambda_over']:
        for lu in param_grid['lambda_under']:
            for tq in param_grid['theta_queue']:
                remaining = TOTAL_SHARES
                total_cost = 0
                
                for ts in timestamps:
                    if remaining <= 0:
                        break
                        
                    venues = [{
                        'ask_px_00': v['ask_px_00'],
                        'ask_sz_00': v['ask_sz_00'],
                        'maker_rebate': 0.0002,
                        'taker_fee': 0.0005
                    } for v in snapshots[ts]]
                    
                    allocation, alloc_cost = greedy_allocate(
                        remaining, venues, lo, lu, tq, STEP_SIZE, slippage_coeff
                    )
                    
                    total_cost += alloc_cost
                    
                    # Update remaining based on actual fills
                    for i, venue in enumerate(venues):
                        fill = min(allocation[i], venue['ask_sz_00'])
                        remaining -= fill
                
                result = {
                    'params': (float(lo), float(lu), float(tq)),
                    'cost': float(total_cost),
                    'filled': TOTAL_SHARES - remaining
                }
                all_results.append(result)
                
                if remaining == 0 and total_cost < best_cost:
                    best_cost = total_cost
                    best_params = (lo, lu, tq)
                    print(f"New best: λo={lo:.1f} λu={lu:.1f} θq={tq:.1f} | Cost: ${total_cost:,.2f}")

    print("\nTop performing parameter combinations:")
    sorted_results = sorted(all_results, key=lambda x: x['cost'])
    for res in sorted_results[:5]:
        p = res['params']
        print(f"λo={p[0]:.1f} λu={p[1]:.1f} θq={p[2]:.1f} | Cost: ${res['cost']:,.2f} | Filled: {res['filled']}/{TOTAL_SHARES}")

    if not best_params:
        print("\nWarning: No parameters fully filled the order")
        best_filled = max(res['filled'] for res in all_results)
        best_params = next(res['params'] for res in all_results if res['filled'] == best_filled)
        print(f"Using best partial fill: {best_params} | Filled: {best_filled}/{TOTAL_SHARES}")

    print("\nRunning optimized strategy with best parameters...")
    lo_opt, lu_opt, tq_opt = best_params
    remaining = TOTAL_SHARES
    optimized_cost = 0
    
    for ts in timestamps:
        if remaining <= 0:
            break
            
        venues = [{
            'ask_px_00': v['ask_px_00'],
            'ask_sz_00': v['ask_sz_00'],
            'maker_rebate': 0.0002,
            'taker_fee': 0.0005
        } for v in snapshots[ts]]
        
        allocation, alloc_cost = greedy_allocate(
            remaining, venues, lo_opt, lu_opt, tq_opt, STEP_SIZE, slippage_coeff
        )
        
        optimized_cost += alloc_cost
        
        for i, venue in enumerate(venues):
            fill = min(allocation[i], venue['ask_sz_00'])
            remaining -= fill

    print("Running baseline strategies...")
    
    # Best Ask Execution
    remaining = TOTAL_SHARES
    cash_best_ask = 0
    for ts in timestamps:
        if remaining <= 0:
            break
        venues = [v for v in snapshots[ts]]
        fill, cash = best_ask_execution(remaining, venues)
        cash_best_ask += cash
        remaining -= fill

    # TWAP Execution
    remaining = TOTAL_SHARES
    cash_twap = 0
    for idx, ts in enumerate(timestamps):
        if remaining <= 0:
            break
        venues = [v for v in snapshots[ts]]
        fill, cash = twap_execution(remaining, timestamps, idx, venues)
        cash_twap += cash
        remaining -= fill

    # VWAP Execution
    remaining = TOTAL_SHARES
    cash_vwap = 0
    for ts in timestamps:
        if remaining <= 0:
            break
        venues = [v for v in snapshots[ts]]
        fill, cash = vwap_execution(remaining, venues)
        cash_vwap += cash
        remaining -= fill

    # Calculate metrics
    avg_fill_opt = optimized_cost / TOTAL_SHARES if TOTAL_SHARES > 0 else 0
    savings_best_ask = (cash_best_ask - optimized_cost) / cash_best_ask * 10000 if cash_best_ask > 0 else 0
    savings_twap = (cash_twap - optimized_cost) / cash_twap * 10000 if cash_twap > 0 else 0
    savings_vwap = (cash_vwap - optimized_cost) / cash_vwap * 10000 if cash_vwap > 0 else 0

    results = {
        "best_parameters": {
            "lambda_over": float(lo_opt),
            "lambda_under": float(lu_opt),
            "theta_queue": float(tq_opt)
        },
        "optimized": {
            "total_cash": float(optimized_cost),
            "avg_fill_px": float(avg_fill_opt),
            "shares_filled": TOTAL_SHARES - remaining
        },
        "baselines": {
            "best_ask": {
                "total_cash": float(cash_best_ask),
                "avg_fill_px": float(cash_best_ask / TOTAL_SHARES) if TOTAL_SHARES > 0 else 0
            },
            "twap": {
                "total_cash": float(cash_twap),
                "avg_fill_px": float(cash_twap / TOTAL_SHARES) if TOTAL_SHARES > 0 else 0
            },
            "vwap": {
                "total_cash": float(cash_vwap),
                "avg_fill_px": float(cash_vwap / TOTAL_SHARES) if TOTAL_SHARES > 0 else 0
            }
        },
        "savings_vs_baselines_bps": {
            "best_ask": float(savings_best_ask),
            "twap": float(savings_twap),
            "vwap": float(savings_vwap)
        },
        "parameter_space_analysis": sorted_results[:10]
    }

    print("\nFinal Results:")
    print(f"Optimized Strategy Cost: ${results['optimized']['total_cash']:,.2f}")
    print(f"Average Fill Price: {results['optimized']['avg_fill_px']:.4f}")
    print(f"Shares Filled: {results['optimized']['shares_filled']}/{TOTAL_SHARES}")
    print("\nSavings vs Baselines (bps):")
    print(f"Best Ask: {results['savings_vs_baselines_bps']['best_ask']:.2f}")
    print(f"TWAP: {results['savings_vs_baselines_bps']['twap']:.2f}")
    print(f"VWAP: {results['savings_vs_baselines_bps']['vwap']:.2f}")

    return results

if __name__ == "__main__":
    results = run_backtest()
    print(json.dumps(results, indent=2))

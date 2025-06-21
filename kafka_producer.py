import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime

def parse_time(ts):
    """Parse nanosecond timestamps to datetime objects"""
    if '.' in ts:
        base, frac = ts.split('.')
        frac = frac.rstrip('Z')  # Remove trailing Z
        if len(frac) > 6:
            frac = frac[:6]  # Truncate to microseconds
        return datetime.strptime(f"{base}.{frac}", "%Y-%m-%dT%H:%M:%S.%f")
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

# Load and filter data
df = pd.read_csv('l1_day.csv')
df = df[(df['ts_recv'] >= '2024-08-01T13:36:32') & 
        (df['ts_recv'] <= '2024-08-01T13:45:14')]

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stream processing
last_ts = None
for _, row in df.iterrows():
    # Create market snapshot
    snapshot = {
        "ts_event": row['ts_event'],
        "publisher_id": row['publisher_id'],
        "ask_px_00": row['ask_px_00'],
        "ask_sz_00": row['ask_sz_00']
    }
    
    # Real-time pacing
    current_ts = parse_time(row['ts_recv'])
    if last_ts:
        delta = (current_ts - last_ts).total_seconds()
        time.sleep(delta)
    
    producer.send('mock_l1_stream', snapshot)
    last_ts = current_ts

producer.flush()
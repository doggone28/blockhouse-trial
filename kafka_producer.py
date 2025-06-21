import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_time(ts):
    """Parse nanosecond timestamps to datetime objects"""
    if '.' in ts:
        base, frac = ts.split('.')
        frac = frac.rstrip('Z')
        if len(frac) > 6:
            frac = frac[:6]
        return datetime.strptime(f"{base}.{frac}", "%Y-%m-%dT%H:%M:%S.%f")
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

def on_send_error(excp):
    logger.error("Failed to send message: %s", excp)
    # Implement retry logic here if needed

def main():
    try:
        # Load and filter data
        df = pd.read_csv('l1_day.csv')
        df = df[(df['ts_recv'] >= '2024-08-01T13:36:32') & 
                (df['ts_recv'] <= '2024-08-01T13:45:14')]
        
        logger.info("Loaded %d rows", len(df))
        
        # Kafka configuration
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            max_block_ms=5000
        )
        
        if not producer.bootstrap_connected():
            raise ConnectionError("Failed to connect to Kafka")
        
        last_ts = None
        for idx, row in df.iterrows():
            try:
                # Validate data
                float(row['ask_px_00'])
                int(row['ask_sz_00'])
                
                snapshot = {
                    "ts_event": row['ts_event'],
                    "publisher_id": row['publisher_id'],
                    "ask_px_00": row['ask_px_00'],
                    "ask_sz_00": row['ask_sz_00']
                }
                
                # Pacing logic
                current_ts = parse_time(row['ts_recv'])
                delta = (current_ts - last_ts).total_seconds() if last_ts else 0
                time.sleep(min(delta, 1.0))  # Cap sleep time
                
                # Send with callback
                producer.send(
                    'mock_l1_stream',
                    value=snapshot
                ).add_errback(on_send_error)
                
                if idx % 100 == 0:
                    logger.info("Sent %d messages", idx+1)
                    
                last_ts = current_ts
                
            except Exception as e:
                logger.error("Error processing row %d: %s", idx, e)
                continue
                
    finally:
        producer.flush(timeout=10)
        producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    main()
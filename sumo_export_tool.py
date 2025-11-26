import requests
import time
import json
import gzip
import os
import sqlite3
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ==========================================
# CONFIGURATION
# ==========================================

# CREDS
SUMO_ACCESS_ID = 'YOUR_ACCESS_ID_HERE'
SUMO_ACCESS_KEY = 'YOUR_ACCESS_KEY_HERE'
SUMO_DEPLOYMENT = 'us1' # e.g., 'us1', 'us2', 'eu', 'au', 'de', 'jp'

# DATA RANGE
# Format: YYYY-MM-DDTHH:MM:SS
START_TIME = '2025-05-20T00:00:00'
END_TIME = '2025-11-26T00:00:00'

# SETTINGS
OUTPUT_DIR = './sumo_logs'
CHUNK_HOURS = 2          # Size of each time slice. 2-4 hours is usually a sweet spot for large volumes.
MAX_WORKERS = 10          # Parallel downloads. Increase if bandwidth/CPU allows (Sumo limits apply).
PAGE_LIMIT = 10000       # Max messages per API page request.

# ==========================================

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sumo_export.log"),
        # Removed StreamHandler to keep the progress bar clean
    ]
)

class SumoDownloader:
    def __init__(self):
        self.base_url = f'https://api.{SUMO_DEPLOYMENT}.sumologic.com/api/v1'
        self.session = self._create_session()
        self.setup_database()
        
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

    def _create_session(self):
        """Creates a session with retry logic for resilience."""
        session = requests.Session()
        session.auth = (SUMO_ACCESS_ID, SUMO_ACCESS_KEY)
        
        # Retry on 429 (Rate Limit), 500, 502, 503, 504
        retries = Retry(
            total=10,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def setup_database(self):
        """Sets up a simple SQLite DB to track processed chunks."""
        self.conn = sqlite3.connect('export_history.db', check_same_thread=False)
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                filename TEXT,
                record_count INTEGER,
                PRIMARY KEY (start_time, end_time)
            )
        ''')
        self.conn.commit()

    def mark_chunk_complete(self, start, end, filename, count):
        c = self.conn.cursor()
        c.execute('INSERT OR REPLACE INTO chunks VALUES (?, ?, ?, ?, ?)', 
                  (start, end, 'COMPLETED', filename, count))
        self.conn.commit()

    def is_chunk_complete(self, start, end):
        c = self.conn.cursor()
        c.execute('SELECT status FROM chunks WHERE start_time=? AND end_time=? AND status="COMPLETED"', (start, end))
        return c.fetchone() is not None

    def create_search_job(self, start_iso, end_iso):
        """Creates a search job on Sumo Logic."""
        payload = {
            'query': '*',  # Pull EVERYTHING
            'from': start_iso,
            'to': end_iso,
            'timeZone': 'UTC',
            'byReceiptTime': True # More reliable for complete exports
        }
        
        try:
            r = self.session.post(f'{self.base_url}/search/jobs', json=payload)
            r.raise_for_status()
            return r.json()['id']
        except Exception as e:
            logging.error(f"Failed to create job for {start_iso}: {e}")
            raise

    def wait_for_job(self, job_id):
        """Polls status until DONE GATHERING RESULTS."""
        while True:
            r = self.session.get(f'{self.base_url}/search/jobs/{job_id}')
            r.raise_for_status()
            status = r.json()
            state = status['state']
            
            if state == 'DONE GATHERING RESULTS':
                return status.get('messageCount', 0)
            elif state in ['CANCELLED', 'FAILED']:
                raise Exception(f"Job {job_id} failed with state: {state}")
            
            # Simple adaptive sleep
            time.sleep(2)

    def fetch_messages(self, job_id, total_count, output_file):
        """Pages through results and writes to GZIP file."""
        offset = 0
        written_count = 0
        
        with gzip.open(output_file, 'wt', encoding='utf-8') as f:
            while offset < total_count:
                params = {
                    'offset': offset,
                    'limit': PAGE_LIMIT
                }
                
                r = self.session.get(f'{self.base_url}/search/jobs/{job_id}/messages', params=params)
                r.raise_for_status()
                data = r.json()
                messages = data.get('messages', [])
                
                if not messages:
                    break
                
                for msg in messages:
                    # Write as JSON Lines
                    json.dump(msg, f)
                    f.write('\n')
                
                written_count += len(messages)
                offset += len(messages)
                
                # Check message count sanity to prevent infinite loops
                if len(messages) == 0: 
                    break

        return written_count

    def process_chunk(self, start, end):
        """Worker function to handle a single time chunk."""
        start_iso = start.strftime('%Y-%m-%dT%H:%M:%S')
        end_iso = end.strftime('%Y-%m-%dT%H:%M:%S')

        if self.is_chunk_complete(start_iso, end_iso):
            logging.info(f"Skipping completed chunk: {start_iso} to {end_iso}")
            return

        logging.info(f"Starting chunk: {start_iso} to {end_iso}")
        
        filename = f"logs_{start.strftime('%Y%m%d_%H%M')}_{end.strftime('%H%M')}.json.gz"
        filepath = os.path.join(OUTPUT_DIR, filename)

        try:
            # 1. Start Job
            job_id = self.create_search_job(start_iso, end_iso)
            
            # 2. Wait
            total_count = self.wait_for_job(job_id)
            logging.info(f"Job {job_id} ready. Found {total_count} messages. Downloading...")

            # 3. Download
            if total_count > 0:
                saved_count = self.fetch_messages(job_id, total_count, filepath)
            else:
                saved_count = 0
                # Create empty file just to mark presence
                with gzip.open(filepath, 'wt') as f:
                    pass

            # 4. Mark Done
            self.mark_chunk_complete(start_iso, end_iso, filename, saved_count)
            logging.info(f"Finished chunk {start_iso}. Saved {saved_count} records.")

        except Exception as e:
            logging.error(f"Error processing chunk {start_iso}: {e}")
            # Do not mark as complete so it retries next time
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass

    def run(self):
        logging.info("Generating time chunks...")
        current = datetime.strptime(START_TIME, '%Y-%m-%dT%H:%M:%S')
        end = datetime.strptime(END_TIME, '%Y-%m-%dT%H:%M:%S')
        
        chunks = []
        while current < end:
            next_step = current + timedelta(hours=CHUNK_HOURS)
            if next_step > end:
                next_step = end
            chunks.append((current, next_step))
            current = next_step
        
        logging.info(f"Generated {len(chunks)} chunks to process.")

        # Use ThreadPool to process chunks in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.process_chunk, start, end) for start, end in chunks]
            
            # Wrap with tqdm for progress bar
            for f in tqdm(as_completed(futures), total=len(chunks), desc="Downloading Logs", unit="chunk"):
                try:
                    f.result()
                except Exception as e:
                    logging.error(f"Worker exception: {e}")

if __name__ == '__main__':
    print("Starting Sumo Logic Export Manager...")
    print("Press Ctrl+C to stop. You can restart the script to resume.")
    
    downloader = SumoDownloader()
    downloader.run()


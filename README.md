# Sumo Logic Bulk Export Tool

A Python script designed for robust, high-volume log exports (TB+) from Sumo Logic. It slices large time ranges into small chunks and processes them in parallel to maximize speed and reliability.

## Features
- **Parallel Downloading**: Uses multiple workers to download faster.
- **Resume Capability**: Tracks progress in a local DB (`export_history.db`). If stopped, simply restart to resume.
- **Smart Chunking**: Splits time ranges into 2-hour blocks to avoid API timeouts.
- **Compression**: Saves logs directly to `.json.gz` to save disk space.

## Setup

1. **Install Dependencies**:
   ```bash
   pip install requests tqdm
   ```

2. **Configure Variables**:
   Open `sumo_export_tool.py` and edit the top section:
   - `SUMO_ACCESS_ID` & `SUMO_ACCESS_KEY`: Your API credentials.
   - `SUMO_DEPLOYMENT`: Your pod (e.g., `us1`, `us2`, `eu`).
   - `START_TIME` & `END_TIME`: Date range (Format: `YYYY-MM-DDTHH:MM:SS`).

3. **Performance Tuning** (Optional):
   - `MAX_WORKERS`: Default is `10`. Increase to `20+` if you have fast internet/high limits. Decrease to `4` if hitting errors.
   - `CHUNK_HOURS`: Default `2`. Keep small (1-4) for stability.

## Usage

```bash
python sumo_export_tool.py
```

### Note on Progress Bar
The progress bar updates only when a chunk is **fully completed**. It may appear "frozen" for several minutes while workers are searching and downloading data in the background. Check `sumo_export.log` for real-time detailed status.

# queuectl – CLI Job Queue with Workers & DLQ

`queuectl` is a minimal, production-style background job queue implemented as a CLI tool.

It supports:

- Enqueuing shell command jobs
- Multiple worker processes
- Automatic retries with exponential backoff
- Dead Letter Queue (DLQ) for permanently failed jobs
- Persistent storage using SQLite
- Basic configuration via CLI

---

## Tech Stack

- **Language**: Python 3
- **Storage**: SQLite (via `sqlite3`)
- **CLI**: `argparse`
- **Concurrency**: `multiprocessing`
- **Execution**: `subprocess.run`

---

## Setup

```bash
git clone https://github.com/<your-username>/queuectl.git
cd queuectl

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

python queuectl.py --help

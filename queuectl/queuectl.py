#!/usr/bin/env python3
import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from multiprocessing import Process

DB_PATH = os.environ.get("QUEUECTL_DB", "queuectl.db")


# ========== DB UTILITIES ==========
def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def get_conn():
    # check_same_thread=False so multiple processes can use it
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            run_at TEXT NOT NULL,
            last_error TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workers (
            id TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            last_heartbeat TEXT NOT NULL
        )
        """
    )

    # defaults
    cur.execute(
        "INSERT OR IGNORE INTO config(key, value) VALUES (?, ?)",
        ("max_retries", "3"),
    )
    cur.execute(
        "INSERT OR IGNORE INTO config(key, value) VALUES (?, ?)",
        ("backoff_base", "2"),
    )
    cur.execute(
        "INSERT OR IGNORE INTO config(key, value) VALUES (?, ?)",
        ("stop_flag", "0"),
    )

    conn.commit()
    conn.close()


def get_config(key, default=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    if row is None:
        return default
    return row["value"]


def set_config(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO config(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )
    conn.commit()
    conn.close()


# ========= ENQUEUE =========
def cmd_enqueue(args):
    init_db()
    try:
        job_data = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    job_id = job_data.get("id") or str(uuid.uuid4())
    command = job_data.get("command")
    if not command:
        print("Job must have 'command'", file=sys.stderr)
        sys.exit(1)

    max_retries_cfg = int(get_config("max_retries", 3))
    max_retries = int(job_data.get("max_retries", max_retries_cfg))

    created_at = job_data.get("created_at") or now_iso()
    updated_at = job_data.get("updated_at") or created_at
    run_at = job_data.get("run_at") or created_at

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO jobs(id, command, state, attempts, max_retries,
                             created_at, updated_at, run_at, last_error)
            VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, NULL)
            """,
            (job_id, command, max_retries, created_at, updated_at, run_at),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        print(f"Job with id '{job_id}' already exists", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    print(f"Enqueued job {job_id} with command: {command}")


# ========= WORKER LOGIC =========
def claim_next_job(conn):
    """
    Atomically pick the next eligible pending job and mark it 'processing'.
    Returns row or None.
    """
    cur = conn.cursor()
    # manual transaction boundary
    cur.execute("BEGIN IMMEDIATE")
    now = now_iso()
    cur.execute(
        """
        SELECT * FROM jobs
        WHERE state = 'pending'
          AND run_at <= ?
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (now,),
    )
    row = cur.fetchone()
    if row is None:
        conn.commit()
        return None

    cur.execute(
        """
        UPDATE jobs
        SET state = 'processing',
            updated_at = ?
        WHERE id = ?
        """,
        (now, row["id"]),
    )
    conn.commit()
    return row


def handle_job_result(conn, job_row, success, error_msg=None):
    cur = conn.cursor()
    now = now_iso()
    attempts = job_row["attempts"]
    max_retries = job_row["max_retries"]

    if success:
        cur.execute(
            """
            UPDATE jobs
            SET state = 'completed',
                updated_at = ?,
                last_error = NULL
            WHERE id = ?
            """,
            (now, job_row["id"]),
        )
    else:
        attempts += 1
        backoff_base = int(get_config("backoff_base", 2))
        if attempts > max_retries:
            # move to DLQ: dead
            cur.execute(
                """
                UPDATE jobs
                SET state = 'dead',
                    attempts = ?,
                    updated_at = ?,
                    last_error = ?
                WHERE id = ?
                """,
                (attempts, now, error_msg, job_row["id"]),
            )
        else:
            delay = backoff_base ** attempts
            run_at_dt = datetime.utcnow() + timedelta(seconds=delay)
            run_at = run_at_dt.isoformat() + "Z"
            cur.execute(
                """
                UPDATE jobs
                SET state = 'pending',
                    attempts = ?,
                    run_at = ?,
                    updated_at = ?,
                    last_error = ?
                WHERE id = ?
                """,
                (attempts, run_at, now, error_msg, job_row["id"]),
            )

    conn.commit()


def worker_loop(worker_id):
    conn = get_conn()
    cur = conn.cursor()
    pid = os.getpid()
    started_at = now_iso()

    # register worker
    cur.execute(
        """
        INSERT INTO workers(id, pid, started_at, last_heartbeat)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            pid = excluded.pid,
            started_at = excluded.started_at,
            last_heartbeat = excluded.last_heartbeat
        """,
        (worker_id, pid, started_at, started_at),
    )
    conn.commit()

    try:
        while True:
            # heartbeat
            cur.execute(
                "UPDATE workers SET last_heartbeat = ? WHERE id = ?",
                (now_iso(), worker_id),
            )
            conn.commit()

            # check stop flag
            if get_config("stop_flag", "0") == "1":
                print(f"[worker {worker_id}] Stop flag set, exiting.")
                break

            job = claim_next_job(conn)
            if job is None:
                time.sleep(1)
                continue

            print(
                f"[worker {worker_id}] Processing job {job['id']} "
                f"(attempts={job['attempts']})"
            )

            try:
                result = subprocess.run(
                    job["command"],
                    shell=True,
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    print(
                        f"[worker {worker_id}] Job {job['id']} completed successfully."
                    )
                    handle_job_result(conn, job, True)
                else:
                    err = (
                        f"Exit code {result.returncode}. "
                        f"stderr={result.stderr.strip()}"
                    )
                    print(
                        f"[worker {worker_id}] Job {job['id']} failed: {err}",
                        file=sys.stderr,
                    )
                    handle_job_result(conn, job, False, err)
            except Exception as e:
                err = f"Exception while running command: {e}"
                print(
                    f"[worker {worker_id}] Job {job['id']} errored: {err}",
                    file=sys.stderr,
                )
                handle_job_result(conn, job, False, err)

    finally:
        # deregister worker
        cur.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
        conn.commit()
        conn.close()
        print(f"[worker {worker_id}] Shutdown complete.")


def cmd_worker_start(args):
    init_db()
    set_config("stop_flag", "0")  # allow workers to run
    count = args.count
    procs = []

    for _ in range(count):
        worker_id = str(uuid.uuid4())
        p = Process(target=worker_loop, args=(worker_id,))
        p.start()
        procs.append(p)
        print(f"Started worker {worker_id} with PID {p.pid}")

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("Received KeyboardInterrupt, signaling workers to stop...")
        set_config("stop_flag", "1")
        for p in procs:
            p.join()


def cmd_worker_stop(_args):
    init_db()
    set_config("stop_flag", "1")
    print("Stop flag set. Workers will exit after completing current job.")


# ========= STATUS / LIST / DLQ =========
def cmd_status(_args):
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    # job counts by state
    cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
    rows = cur.fetchall()
    print("Job counts by state:")
    for r in rows:
        print(f"  {r['state']}: {r['cnt']}")

    # active workers (heartbeat within last 10s)
    cutoff = datetime.utcnow() - timedelta(seconds=10)
    cutoff_iso = cutoff.isoformat() + "Z"
    cur.execute(
        """
        SELECT COUNT(*) as cnt FROM workers
        WHERE last_heartbeat >= ?
        """,
        (cutoff_iso,),
    )
    wcnt = cur.fetchone()["cnt"]
    print(f"\nActive workers (heartbeat <= 10s): {wcnt}")

    conn.close()


def cmd_list(args):
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    if args.state:
        cur.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY created_at",
            (args.state,),
        )
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at")

    rows = cur.fetchall()
    for r in rows:
        print(
            f"{r['id']} | {r['state']} | attempts={r['attempts']} "
            f"| max_retries={r['max_retries']} | run_at={r['run_at']} "
            f"| cmd={r['command']}"
        )

    conn.close()


def cmd_dlq_list(_args):
    init_db()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM jobs WHERE state = 'dead' ORDER BY created_at"
    )
    rows = cur.fetchall()
    if not rows:
        print("DLQ is empty.")
        return
    for r in rows:
        print(
            f"{r['id']} | attempts={r['attempts']} | cmd={r['command']} "
            f"| last_error={r['last_error']}"
        )
    conn.close()


def cmd_dlq_retry(args):
    init_db()
    job_id = args.job_id
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM jobs WHERE id = ? AND state = 'dead'", (job_id,))
    row = cur.fetchone()
    if row is None:
        print(f"No dead job found with id '{job_id}'", file=sys.stderr)
        sys.exit(1)

    now = now_iso()
    cur.execute(
        """
        UPDATE jobs
        SET state = 'pending',
            attempts = 0,
            run_at = ?,
            updated_at = ?,
            last_error = NULL
        WHERE id = ?
        """,
        (now, now, job_id),
    )
    conn.commit()
    conn.close()
    print(f"Moved job {job_id} from DLQ back to pending.")


# ========= CONFIG =========
def cmd_config_set(args):
    init_db()
    set_config(args.key, args.value)
    print(f"Config '{args.key}' set to '{args.value}'")


def cmd_config_get(args):
    init_db()
    val = get_config(args.key, None)
    if val is None:
        print(f"(no value set for '{args.key}')")
    else:
        print(f"{args.key} = {val}")


# ========= ARGPARSE / MAIN =========
def build_parser():
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="Simple background job queue with workers and DLQ",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # enqueue
    p_enq = subparsers.add_parser(
        "enqueue", help="Enqueue a new job with JSON spec"
    )
    p_enq.add_argument(
        "job_json",
        help="Job JSON, e.g. '{\"id\":\"job1\",\"command\":\"sleep 2\"}'",
    )
    p_enq.set_defaults(func=cmd_enqueue)

    # worker
    p_worker = subparsers.add_parser("worker", help="Manage workers")
    worker_sub = p_worker.add_subparsers(dest="worker_cmd", required=True)

    p_ws = worker_sub.add_parser("start", help="Start worker processes")
    p_ws.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of workers to start (default: 1)",
    )
    p_ws.set_defaults(func=cmd_worker_start)

    p_wstop = worker_sub.add_parser(
        "stop", help="Signal workers to stop gracefully"
    )
    p_wstop.set_defaults(func=cmd_worker_stop)

    # status
    p_status = subparsers.add_parser(
        "status", help="Show job states and active workers"
    )
    p_status.set_defaults(func=cmd_status)

    # list
    p_list = subparsers.add_parser(
        "list", help="List jobs (optionally by state)"
    )
    p_list.add_argument(
        "--state",
        help="Filter by job state "
        "(pending|processing|completed|failed|dead)",
    )
    p_list.set_defaults(func=cmd_list)

    # dlq
    p_dlq = subparsers.add_parser("dlq", help="Dead Letter Queue operations")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_cmd", required=True)

    p_dlq_list = dlq_sub.add_parser("list", help="List DLQ jobs")
    p_dlq_list.set_defaults(func=cmd_dlq_list)

    p_dlq_retry = dlq_sub.add_parser("retry", help="Retry a DLQ job by id")
    p_dlq_retry.add_argument("job_id", help="Job id to retry")
    p_dlq_retry.set_defaults(func=cmd_dlq_retry)

    # config
    p_cfg = subparsers.add_parser("config", help="Config management")
    cfg_sub = p_cfg.add_subparsers(dest="cfg_cmd", required=True)

    p_cfg_set = cfg_sub.add_parser("set", help="Set config key")
    p_cfg_set.add_argument("key")
    p_cfg_set.add_argument("value")
    p_cfg_set.set_defaults(func=cmd_config_set)

    p_cfg_get = cfg_sub.add_parser("get", help="Get config key")
    p_cfg_get.add_argument("key")
    p_cfg_get.set_defaults(func=cmd_config_get)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

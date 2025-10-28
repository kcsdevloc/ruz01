#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Local-only bulk RÚZ runner (enumerate UJs once, then fan out to statements & reports)

Features
- Global UJ sweep using /api/uctovne-jednotky with pagination (no per-ICO list spam)
- Concurrency with a global token-bucket rate limiter (stable RPS)
- Chunked NDJSON.gz (or .zst if zstandard is available) to avoid millions of tiny files
- Observability: periodic counters, errors, RPS, simple ETA
- Graceful exit (Ctrl+C), Pause (Ctrl+P), Resume (Ctrl+O)

Usage (example):
  python ruz_bulk_local.py --years 5 --base-dir "m:\\pg_ts\\api\\raw\\ruz_bulk" --concurrency 64 --rps 60
"""

import argparse
import concurrent.futures as cf
import gzip
import io
import json
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional zstandard
try:
    import zstandard as zstd  # pip install zstandard
    HAS_ZSTD = True
except Exception:
    HAS_ZSTD = False

BASE_URL = "https://www.registeruz.sk/cruz-public"

# ---------------- Control flags & counters ----------------
stop_event = threading.Event()
pause_event = threading.Event()  # set => paused

counters_lock = threading.Lock()
counters = {
    "uj_enqueued": 0,
    "uj_done": 0,
    "stmt_done": 0,
    "rept_done": 0,
    "req_ok": 0,
    "req_err": 0,
    "rps_window": 0,  # incremented per OK/ERR; reset by reporter to compute RPS
}

start_time = time.time()

# --- Globals for fast shutdown ---
q_uj_global = None            # set in main() after queue is created
worker_count_global = 0       # set in main() after workers are started
session_global = None         # set in main() after session is created


# ---------------- Token-bucket limiter ----------------
class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate = float(rate_per_sec)
        self.capacity = int(capacity)
        self.tokens = float(capacity)
        self.lock = threading.Lock()
        self.last = time.time()

    def acquire(self, n: float = 1.0):
        while not stop_event.is_set():
            while pause_event.is_set() and not stop_event.is_set():
                time.sleep(0.05)
            with self.lock:
                now = time.time()
                elapsed = now - self.last
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
            time.sleep(0.005)


# ---------------- HTTP session ----------------
def setup_session(timeout: int) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=7,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=256, pool_maxsize=256)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"Accept": "application/json"})
    s.request_timeout = timeout
    return s


def GET(session: requests.Session, limiter: TokenBucket, path: str, params: Optional[dict] = None) -> dict:
    # Pause-aware
    while pause_event.is_set() and not stop_event.is_set():
        time.sleep(0.05)
    if stop_event.is_set():
        raise KeyboardInterrupt

    limiter.acquire(1.0)
    url = f"{BASE_URL}{path}"
    try:
        resp = session.get(url, params=params, timeout=getattr(session, "request_timeout", 30))
        if resp.status_code >= 400:
            with counters_lock:
                counters["req_err"] += 1
                counters["rps_window"] += 1
            # raise with context (trim body)
            raise RuntimeError(f"HTTP {resp.status_code} {url} params={params} body={resp.text[:300]}")
        try:
            data = resp.json()
        except Exception as e:
            with counters_lock:
                counters["req_err"] += 1
                counters["rps_window"] += 1
            raise RuntimeError(f"Bad JSON {url} params={params}: {e} body={resp.text[:300]}")
        with counters_lock:
            counters["req_ok"] += 1
            counters["rps_window"] += 1
        return data
    except Exception:
        # brief jittered sleep to smooth spikes
        time.sleep(0.1 + 0.2 * (time.time() % 1))
        raise


# ---------------- Chunked NDJSON writers ----------------
class NDJSONWriter:
    """
    Append-only chunked writer:
      base_dir/<type>/part-00001.ndjson.<codec>
    Rotation by approximate size (bytes).
    """
    def __init__(self, out_dir: str, typ: str, rotate_mb: int = 128, codec: str = "gzip"):
        self.typ = typ
        self.dir = os.path.join(out_dir, typ)
        os.makedirs(self.dir, exist_ok=True)
        self.rotate_bytes = int(rotate_mb * 1024 * 1024)
        self.codec = "zstd" if (codec == "zstd" and HAS_ZSTD) else "gzip"
        self.part_idx = 0
        self.bytes = 0
        self.fh = None
        self._open_new()

    def _open_new(self):
        if self.fh:
            try:
                self.fh.close()
            except Exception:
                pass
        self.part_idx += 1
        ext = "ndjson.zst" if self.codec == "zstd" else "ndjson.gz"
        path = os.path.join(self.dir, f"part-{self.part_idx:05d}.{ext}")
        raw = open(path, "wb")
        if self.codec == "zstd":
            cctx = zstd.ZstdCompressor(level=10, write_content_size=False)
            self.fh = io.BufferedWriter(cctx.stream_writer(raw))
        else:
            self.fh = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6)
        self.bytes = 0

    def write_obj(self, obj: dict):
        b = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
        self.fh.write(b)
        self.bytes += len(b)
        if self.bytes >= self.rotate_bytes:
            self._open_new()

    def close(self):
        if self.fh:
            try:
                self.fh.close()
            except Exception:
                pass
            self.fh = None


# ---------------- API helpers ----------------
def list_uj_ids(session: requests.Session, limiter: TokenBucket, since: str = "2000-01-01"):
    """Yield all UJ ids via the paged list API."""
    params = {
        "zmenene-od": since,
        "max-zaznamov": 10000,
    }
    last_id = None
    while not stop_event.is_set():
        if last_id is not None:
            params["pokracovat-za-id"] = last_id
        data = GET(session, limiter, "/api/uctovne-jednotky", params=params)
        chunk = data.get("id", []) if isinstance(data, dict) else []
        if not chunk:
            break
        for uj in chunk:
            yield int(uj)
        if not data.get("existujeDalsieId", False):
            break
        last_id = int(chunk[-1])


def get_uj_detail(session: requests.Session, limiter: TokenBucket, uj_id: int) -> dict:
    return GET(session, limiter, "/api/uctovna-jednotka", params={"id": uj_id})


def get_statement_detail(session: requests.Session, limiter: TokenBucket, st_id: int) -> dict:
    return GET(session, limiter, "/api/uctovna-zavierka", params={"id": st_id})


def get_report_detail(session: requests.Session, limiter: TokenBucket, rep_id: int) -> dict:
    return GET(session, limiter, "/api/uctovny-vykaz", params={"id": rep_id})


# ---------------- Processing logic ----------------
def year_from(st: dict) -> Optional[int]:
    d = st.get("obdobieDo") or ""
    try:
        return int(d.split("-")[0])
    except Exception:
        return None


def select_statements(statements: List[dict], last_n_years: int) -> List[dict]:
    """Non-consolidated; best per year (prefer typ == 'Riadna'); last N years by okres-do year."""
    # Filter indiv
    indiv = [s for s in statements if not s.get("konsolidovana", False)]
    # group by year
    bucket: Dict[int, List[dict]] = {}
    for s in indiv:
        y = year_from(s)
        if y is None:
            continue
        bucket.setdefault(y, []).append(s)
    # pick best per year
    chosen = []
    for y, items in bucket.items():
        items_sorted = sorted(
            items,
            key=lambda s: (
                0 if (s.get("typ") == "Riadna") else 1,
                s.get("datumPodania") or "",
                s.get("id", 0),
            ),
        )
        chosen.append(items_sorted[0])
    # last N years only
    chosen_sorted = sorted(chosen, key=lambda s: year_from(s) or 0, reverse=True)
    return chosen_sorted[:last_n_years]


def worker(
    session: requests.Session,
    limiter: TokenBucket,
    q_uj: queue.Queue,
    years: int,
    w_uj: NDJSONWriter,
    w_stmt: NDJSONWriter,
    w_rep: NDJSONWriter,
    allow_statement,          # callable(payload)->bool
    allow_report,             # callable(payload)->bool
):
    while not stop_event.is_set():
        try:
            uj_id = q_uj.get(timeout=0.3)
        except queue.Empty:
            continue
        if uj_id is None:
            q_uj.task_done()
            return
        try:
            uj = get_uj_detail(session, limiter, uj_id)
            ico = uj.get("ico")
            # write UJ record
            w_uj.write_obj({"type": "uj", "uj_id": uj_id, "ico": ico, "payload": uj})

            st_ids = sorted(set(uj.get("idUctovnychZavierok") or []))
            statements: List[dict] = []
            for st_id in st_ids:
                if stop_event.is_set():
                    break
                try:
                    st = get_statement_detail(session, limiter, st_id)
                    statements.append(st)
                except Exception as e:
                    with counters_lock:
                        counters["req_err"] += 1
                    # continue to next statement
                    continue

            selected = select_statements(statements, years)

            for st in selected:
                if allow_statement(st):
                    w_stmt.write_obj({"type": "statement", "uj_id": uj_id, "ico": ico, "payload": st})
                    with counters_lock:
                        counters["stmt_done"] += 1
                    rep_ids = st.get("idUctovnychVykazov") or []
                    for rep_id in rep_ids:
                        if stop_event.is_set():
                            break
                        try:
                            rep = get_report_detail(session, limiter, rep_id)
                            if allow_report(rep):
                                w_rep.write_obj({"type": "report", "uj_id": uj_id, "ico": ico, "statement_id": st.get("id"), "payload": rep})
                                with counters_lock:
                                    counters["rept_done"] += 1
                        except Exception:
                            with counters_lock:
                                counters["req_err"] += 1
                            continue

            with counters_lock:
                counters["uj_done"] += 1
        finally:
            q_uj.task_done()


# ---------------- Reporter & key listener ----------------
def fmt_eta(done: int, rate: float) -> str:
    if done <= 0 or rate <= 0:
        return "ETA: n/a"
    # not a true ETA (we don't know total), but show average time per UJ
    sec_per = 1.0 / rate
    return f"avg/req ~{sec_per:.2f}s"


def reporter(period_sec: int):
    while not stop_event.is_set():
        time.sleep(period_sec)
        with counters_lock:
            c = counters.copy()
            rps = c["rps_window"] / period_sec
            counters["rps_window"] = 0  # reset for next window
        elapsed = time.time() - start_time
        msg = (
            f"[{time.strftime('%H:%M:%S')}] "
            f"UJ {c['uj_done']}/{c['uj_enqueued']} | "
            f"Stmt {c['stmt_done']} | "
            f"Rep {c['rept_done']} | "
            f"Req OK {c['req_ok']} / ERR {c['req_err']} | "
            f"RPS {rps:.1f} | Elapsed {int(elapsed/60)}m"
        )
        print(msg, flush=True)


def key_listener():
    try:
        import msvcrt  # Windows
        print("[Keys] Ctrl+P = pause, Ctrl+O = resume, Ctrl+C = graceful stop", flush=True)
        while not stop_event.is_set():
            if msvcrt.kbhit():
                ch = msvcrt.getch()
                if not ch:
                    continue
                b = ch.lower()
                # Ctrl+P (16) or 'p'
                if b in (b"\x10", b"p"):
                    if not pause_event.is_set():
                        pause_event.set()
                        print("[PAUSE] Paused. Press Ctrl+O (or 'o') to continue.", flush=True)
                # Ctrl+O (15) or 'o'
                elif b in (b"\x0f", b"o"):
                    if pause_event.is_set():
                        pause_event.clear()
                        print("[PAUSE] Resumed.", flush=True)
            time.sleep(0.05)
    except Exception:
        # Non-Windows: skip hotkeys (Ctrl+C still works)
        return


def sigint_handler(sig, frame):
    print("\n[STOP] Ctrl+C received. Cancelling now...", flush=True)
    stop_event.set()
    pause_event.clear()

    # Close HTTP session so in-flight requests error out quickly
    try:
        if session_global is not None:
            session_global.close()
    except Exception:
        pass

    # Drain queue and inject sentinels so workers exit promptly
    q = q_uj_global
    if q is not None:
        try:
            while True:
                item = q.get_nowait()
                q.task_done()
        except queue.Empty:
            pass
        # push one sentinel per worker
        for _ in range(worker_count_global):
            try:
                q.put_nowait(None)
            except queue.Full:
                # drop one pending task and retry inserting sentinel
                try:
                    dropped = q.get_nowait()
                    q.task_done()
                    q.put_nowait(None)
                except Exception:
                    pass

# ---------------- Main ----------------
def main():
    parser = argparse.ArgumentParser(description="Local-only bulk RÚZ fetcher with chunked NDJSON output.")
    parser.add_argument("--base-dir", type=str, required=True, help="Base output directory.")
    parser.add_argument("--years", type=int, default=5, help="How many last calendar years to fetch per UJ.")
    parser.add_argument("--concurrency", type=int, default=64, help="Worker threads.")
    parser.add_argument("--rps", type=float, default=60.0, help="Global requests per second cap (token bucket).")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--report-every", type=int, default=60, help="Progress report period in seconds.")
    parser.add_argument("--since", type=str, default="2000-01-01", help="zmenene-od for UJ enumeration.")
    parser.add_argument("--rotate-mb", type=int, default=128, help="Chunk rotation size (MB).")
    parser.add_argument("--codec", type=str, default=("zstd" if HAS_ZSTD else "gzip"), choices=["zstd", "gzip"],
                        help="Compression codec for chunks.")
    parser.add_argument("--max-uj", type=int, default=0, help="Process at most this many UJs (0 = no limit).")
    parser.add_argument("--shard", type=int, default=0, help="0-based shard id (this worker)")
    parser.add_argument("--of", type=int, default=1, help="total number of shards")
    # NEW: inclusive year window for statements/reports (defaults from env MIN_YEAR/MAX_YEAR if present)
    parser.add_argument("--min-year", type=int, default=(int(os.getenv("MIN_YEAR")) if os.getenv("MIN_YEAR") else None),
                        help="inclusive lower bound year for statements/reports")
    parser.add_argument("--max-year", type=int, default=(int(os.getenv("MAX_YEAR")) if os.getenv("MAX_YEAR") else None),
                        help="inclusive upper bound year for statements/reports")
    args = parser.parse_args()
    if args.of <= 0 or not (0 <= args.shard < args.of):
        print(f"--shard must be in [0..{args.of-1}] and --of > 0", file=sys.stderr)
        sys.exit(2)

    os.makedirs(args.base_dir, exist_ok=True)

    signal.signal(signal.SIGINT, sigint_handler)

    session = setup_session(args.timeout)
    limiter = TokenBucket(rate_per_sec=args.rps, capacity=int(max(args.rps, 1)))

    # expose session to the signal handler
    global session_global
    session_global = session

    # Writers (chunked)
    w_uj   = NDJSONWriter(args.base_dir, "uj",        rotate_mb=args.rotate_mb, codec=args.codec)
    w_stmt = NDJSONWriter(args.base_dir, "statement", rotate_mb=args.rotate_mb, codec=args.codec)
    w_rep  = NDJSONWriter(args.base_dir, "report",    rotate_mb=args.rotate_mb, codec=args.codec)

    # Queues & threads
    q_uj: queue.Queue = queue.Queue(maxsize=args.concurrency * 4)

    # Reporter & keys
    threading.Thread(target=reporter, args=(args.report_every,), daemon=True).start()
    threading.Thread(target=key_listener, daemon=True).start()

    # Year-window predicates (gate writes)
    def allow_statement(payload: dict) -> bool:
        if args.min_year is None and args.max_year is None:
            return True
        y = year_from(payload or {})
        if y is None:
            return False
        if args.min_year is not None and y < args.min_year:
            return False
        if args.max_year is not None and y > args.max_year:
            return False
        return True

    def allow_report(payload: dict) -> bool:
        if args.min_year is None and args.max_year is None:
            return True
        ts = ((payload or {}).get("obsah") or {}).get("titulnaStrana") or {}
        d = ts.get("obdobieDo")
        try:
            y = int(str(d).split("-")[0]) if d else None
        except Exception:
            y = None
        if y is None:
            return False
        if args.min_year is not None and y < args.min_year:
            return False
        if args.max_year is not None and y > args.max_year:
            return False
        return True

    # Workers
    workers: List[threading.Thread] = []
    for _ in range(args.concurrency):
        t = threading.Thread(
            target=worker,
            args=(session, limiter, q_uj, args.years, w_uj, w_stmt, w_rep, allow_statement, allow_report),
            daemon=True
        )
        t.start()
        workers.append(t)

    # expose queue and worker count to the signal handler
    global q_uj_global, worker_count_global
    q_uj_global = q_uj
    worker_count_global = len(workers)

    # Enumerate UJs and enqueue
    try:
        enq = 0
        for uj_id in list_uj_ids(session, limiter, since=args.since):
            if stop_event.is_set():
                break

            # ----- deterministic sharding: only take UJs that belong to this shard -----
            if (uj_id % args.of) != args.shard:
                continue

            # pause-aware
            while pause_event.is_set() and not stop_event.is_set():
                time.sleep(0.05)

            # enqueue
            try:
                q_uj.put(uj_id, timeout=0.5)
            except queue.Full:
                if stop_event.is_set():
                    break
                q_uj.put(uj_id)  # block until there is room

            enq += 1
            if enq % 10000 == 0:
                with counters_lock:
                    counters["uj_enqueued"] = enq

            if args.max_uj > 0 and enq >= args.max_uj:
                break

        with counters_lock:
            counters["uj_enqueued"] = enq

    finally:
        # Ensure workers are told to exit (sentinels). If Ctrl+C already did it, this is harmless.
        for _ in workers:
            try:
                q_uj.put_nowait(None)
            except queue.Full:
                pass

        # Bounded wait for queue to drain
        deadline = time.time() + 10  # seconds
        while True:
            if getattr(q_uj, "unfinished_tasks", 0) == 0:
                break
            if time.time() > deadline or stop_event.is_set():
                print("[STOP] Forcing shutdown (some in-flight tasks may be dropped).", flush=True)
                break
            time.sleep(0.1)

        # Close writers
        try:
            w_uj.close()
            w_stmt.close()
            w_rep.close()
        except Exception:
            pass

        # Wait workers (briefly)
        for t in workers:
            t.join(timeout=1.0)

        # Close session on clean exit too (harmless if already closed by Ctrl+C)
        try:
            session.close()
        except Exception:
            pass

    # Final line
    with counters_lock:
        c = counters.copy()
    elapsed = time.time() - start_time
    print(
        f"[DONE] UJ {c['uj_done']}/{c['uj_enqueued']} | Stmt {c['stmt_done']} | Rep {c['rept_done']} | "
        f"Req OK {c['req_ok']} / ERR {c['req_err']} | Elapsed {int(elapsed/60)}m",
        flush=True,
    )


if __name__ == "__main__":
    main()

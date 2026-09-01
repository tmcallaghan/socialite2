#!/usr/bin/env python3
"""socialite2 - a Python port of the Socialite social-media benchmark.

A single-script benchmark tool for MongoDB API compatible databases (MongoDB,
Amazon DocumentDB, Azure DocumentDB, Open Source DocumentDB). It can both load
Socialite-shaped data (``--load``) and execute the Socialite workload
(``--run``) against any MongoDB-compatible endpoint.

Architecture (inspired by py-mongo-sysbench / python-bench02):
  * ``multiprocessing.Process`` workers, each with its own ``MongoClient``.
  * A shared ``multiprocessing.Manager().Queue()`` carries performance
    messages from workers to a reporter thread in the main process.
  * The reporter drains the queue, prints periodic throughput/latency stats
    and, on completion, a per-operation percentile summary.
  * Per-worker interval-based (fixed-window) rate limiting.

Work distributions (uniform / zipfian / latest) are faithful pure-Python ports
of the YCSB generators. The zipfian keyspace selector uses a *scrambled*
zipfian (FNV-1a hash of the zipfian draw) so the hot key set is scattered
across the keyspace rather than clustered at the low indices.

Schema (matches Socialite exactly):
  users     : {_id: "u<i>"}
  content   : {_id: ObjectId, _a: author, _m: message}   idx {_a:1,_id:1}
  followers : {_id: ObjectId, _f: owner,  _t: peer}       idx {_f:1,_t:1} unique, {_t:1,_f:1}
  following : {_id: ObjectId, _f: follower,_t: followed}  idx {_f:1,_t:1} unique
"""

import argparse
import csv
import math
import multiprocessing as mp
import os  # noqa: F401
import queue
import random
import string
import threading
import time
from collections import deque

try:
    from pymongo import MongoClient, InsertOne, ASCENDING, DESCENDING
    from pymongo.errors import BulkWriteError, DuplicateKeyError
    from bson import ObjectId  # noqa: F401
except ImportError:  # allow importing this module (e.g. for the distribution
    # unit tests) without pymongo installed. The DB code paths require it.
    MongoClient = None
    InsertOne = None
    ASCENDING = 1
    DESCENDING = -1
    BulkWriteError = Exception
    DuplicateKeyError = Exception
    ObjectId = None


# ===========================================================================
# Numerical core: work-distribution generators (YCSB ports)
# ===========================================================================
ZIPFIAN_CONSTANT = 0.99
SCRAMBLED_ITEM_COUNT = 10_000_000_000
SCRAMBLED_ZETAN = 26.46902820178302  # precomputed zeta(1e10, 0.99) - DO NOT recompute
FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325
FNV_PRIME_64 = 1099511628211
MASK64 = (1 << 64) - 1


def fnvhash64(v):
    """FNV-1a 64-bit hash with Java signed-long semantics (final abs)."""
    h = FNV_OFFSET_BASIS_64
    v &= MASK64
    for _ in range(8):
        h = ((h ^ (v & 0xff)) * FNV_PRIME_64) & MASK64
        v >>= 8
    if h >= (1 << 63):
        h -= (1 << 64)
    return abs(h)


class ZipfianGenerator:
    """Faithful port of YCSB ZipfianGenerator (base=0)."""

    def __init__(self, items, zipfian_constant=ZIPFIAN_CONSTANT, zetan=None, rng=None):
        self.items = items
        self.base = 0
        self.theta = zipfian_constant
        self._rng = rng or random.Random()

        self.zeta2theta = 1.0 / (1 ** self.theta) + 1.0 / (2 ** self.theta)
        self.alpha = 1.0 / (1.0 - self.theta)

        if zetan is not None:
            self.zetan = zetan
        else:
            self.zetan = self._zetastatic(items, self.theta)

        self.countforzeta = items
        self.eta = (1 - (2.0 / items) ** (1 - self.theta)) / (1 - self.zeta2theta / self.zetan)

    @staticmethod
    def _zetastatic(n, theta):
        s = 0.0
        for i in range(n):
            s += 1.0 / ((i + 1) ** theta)
        return s

    def next_long(self, itemcount):
        if itemcount != self.countforzeta:
            if itemcount > self.countforzeta:
                # incrementally extend zetan (used by the "latest" distribution
                # as the counter grows)
                for i in range(self.countforzeta, itemcount):
                    self.zetan += 1.0 / ((i + 1) ** self.theta)
                self.countforzeta = itemcount
                # recompute eta with self.items in the (2.0/self.items) term (YCSB)
                self.eta = (1 - (2.0 / self.items) ** (1 - self.theta)) / (1 - self.zeta2theta / self.zetan)
            # (item-count-decrease branch intentionally omitted)

        u = self._rng.random()
        uz = u * self.zetan

        if uz < 1.0:
            return self.base
        if uz < 1.0 + (0.5 ** self.theta):
            return self.base + 1
        return self.base + int(itemcount * ((self.eta * u - self.eta + 1) ** self.alpha))

    def next_value(self):
        return self.next_long(self.items)


class ScrambledZipfianGenerator:
    """Zipfian frequency distribution with the hot set scattered via FNV hash."""

    def __init__(self, items, rng=None):
        self.itemcount = items
        self.min = 0
        self.gen = ZipfianGenerator(
            SCRAMBLED_ITEM_COUNT, ZIPFIAN_CONSTANT, zetan=SCRAMBLED_ZETAN, rng=rng
        )

    def next_value(self):
        ret = self.gen.next_value()
        return self.min + (fnvhash64(ret) % self.itemcount)


class CounterGenerator:
    """Monotonic counter; basis for the "latest" distribution."""

    def __init__(self, start):
        self._c = start

    def next_value(self):
        v = self._c
        self._c += 1
        return v

    def last_value(self):
        return self._c - 1


class SkewedLatestGenerator:
    """"latest" distribution: newest counter values are hottest."""

    def __init__(self, counter, rng=None):
        self.counter = counter
        self.zipf = ZipfianGenerator(max(1, counter.last_value() + 1), rng=rng)

    def next_value(self):
        mx = self.counter.last_value()
        if mx < 0:
            return 0
        return mx - self.zipf.next_long(mx + 1)


def user_id(i):
    return f"u{i}"


def random_message(rng):
    """Random lowercase message, length in [10, 140] (matches Socialite)."""
    return ''.join(rng.choice(string.ascii_lowercase) for _ in range(rng.randint(10, 140)))


def make_selector(kind, n, rng):
    """Return a zero-arg callable yielding an int index in [0, n)."""
    if kind == "uniform":
        return lambda: rng.randint(0, n - 1)
    if kind == "zipfian":
        gen = ScrambledZipfianGenerator(n, rng=rng)
        return gen.next_value
    if kind == "latest":
        z = ZipfianGenerator(n, rng=rng)
        return lambda: max(0, min(n - 1, (n - 1) - z.next_long(n)))
    raise ValueError(f"unknown selector kind: {kind!r}")


# ===========================================================================
# CLI
# ===========================================================================
def build_parser():
    """Build and return the argparse parser for socialite2."""
    parser = argparse.ArgumentParser(
        prog="socialite2.py",
        description=(
            "socialite2 - a Python port of the Socialite social-media benchmark "
            "for MongoDB API compatible databases (MongoDB, Amazon DocumentDB, "
            "Azure DocumentDB, Open Source DocumentDB). One script handles both "
            "data loading (--load) and benchmark execution (--run)."
        ),
        epilog=(
            "Examples:\n"
            "  # Load 100k users with a zipfian follow graph into a local MongoDB\n"
            "  python socialite2.py --load --users 100000 --drop\n"
            "\n"
            "  # Run the benchmark for 5 minutes with 8 workers, capped at 5000 ops/sec\n"
            "  python socialite2.py --run --processes 8 --run-seconds 300 --rate-limit 5000\n"
            "\n"
            "  # Run a fixed number of operations against a remote endpoint\n"
            "  python socialite2.py --run --uri mongodb://host:27017 --num-operations 1000000\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    mode = parser.add_argument_group("mode")
    mode.add_argument("--load", action="store_true",
                      help="Load data into the target collections.")
    mode.add_argument("--run", action="store_true",
                      help="Execute the Socialite benchmark workload.")

    common = parser.add_argument_group("common")
    common.add_argument("--uri", type=str, default="mongodb://localhost:27017",
                        help="MongoDB connection URI (default: %(default)s)")
    common.add_argument("--database", type=str, default="socialite",
                        help="Target database name (default: %(default)s)")
    common.add_argument("--processes", type=int, default=4,
                        help="Number of worker processes (default: %(default)s)")
    common.add_argument("--run-seconds", type=int, default=0,
                        help="Duration to run in seconds; 0 means unbounded (default: %(default)s)")
    common.add_argument("--num-operations", type=int, default=0,
                        help="Total operations across all workers; 0 means unbounded (default: %(default)s)")
    common.add_argument("--rate-limit", type=int, default=9999999,
                        help="Target ops/sec across ALL workers (default: %(default)s = unlimited)")
    common.add_argument("--file-name", type=str, default="socialite2",
                        help="Base name for output .csv file (default: %(default)s)")
    common.add_argument("--report-interval", type=int, default=10,
                        help="Reporting interval in seconds (default: %(default)s)")
    common.add_argument("--num-intervals-average", type=int, default=10,
                        help="Recent intervals to average for smoothed throughput (default: %(default)s)")
    common.add_argument("--seed", type=int, default=42,
                        help="Base random seed; each worker derives its own (default: %(default)s)")
    common.add_argument("--compression", type=str, default="default", choices=["default","none"], help="Collection compression (default: %(default)s)")
    common.add_argument("--worker-report-seconds", type=int, default=5, help="Number of seconds between workers queueing reporting metrics (default: %(default)s)")

    load = parser.add_argument_group("load")
    load.add_argument("--drop", action="store_true",
                      help="Drop existing collections before loading.")
    load.add_argument("--users", type=int, default=1000,
                      help="Number of users to load (default: %(default)s)")
    load.add_argument("--max-follows", type=int, default=100,
                      help="Maximum follows per user (default: %(default)s)")
    load.add_argument("--messages-per-user", type=int, default=100,
                      help="Messages (content) per user to load (default: %(default)s)")
    load.add_argument("--load-batch-size", type=int, default=100,
                      help="Batch size for bulk inserts during load (default: %(default)s)")
    load.add_argument("--follow-distribution", type=str, default="uniform",
                      choices=["uniform", "zipfian", "latest"],
                      help="Distribution used to select follow targets during load (default: %(default)s)")

    run = parser.add_argument_group("run")
    run.add_argument("--user-distribution", type=str, default="uniform",
                     choices=["uniform", "zipfian", "latest"],
                     help="Distribution used to pick the acting user (default: %(default)s)")
    run.add_argument("--content-distribution", type=str, default="latest",
                     choices=["uniform", "zipfian", "latest"],
                     help="Distribution used to pick the author when sending content (default: %(default)s)")
    run.add_argument("--follow-pct", type=float, default=0.10,
                     help="Fraction of ops that are follow (default: %(default)s)")
    run.add_argument("--unfollow-pct", type=float, default=0.05,
                     help="Fraction of ops that are unfollow (default: %(default)s)")
    run.add_argument("--read-timeline-pct", type=float, default=0.30,
                     help="Fraction of ops that read a timeline (default: %(default)s)")
    run.add_argument("--scroll-timeline-pct", type=float, default=0.15,
                     help="Fraction of ops that scroll a timeline (default: %(default)s)")
    run.add_argument("--send-content-pct", type=float, default=0.30,
                     help="Fraction of ops that send content (default: %(default)s)")
    run.add_argument("--fof-agg-pct", type=float, default=0.05,
                     help="Fraction of ops that run friends-of-friends via aggregation (default: %(default)s)")
    run.add_argument("--fof-query-pct", type=float, default=0.05,
                     help="Fraction of ops that run friends-of-friends via query (default: %(default)s)")
    run.add_argument("--timeline-limit", type=int, default=50,
                     help="Maximum documents returned for a timeline read (default: %(default)s)")
    run.add_argument("--fanout-limit", type=int, default=50,
                     help="Maximum fanout for timeline / friends-of-friends queries (default: %(default)s)")

    return parser


def parse_args(argv=None):
    """Parse command-line arguments and return the namespace."""
    return build_parser().parse_args(argv)


# ===========================================================================
# Rate limiting
# ===========================================================================
class RateLimiter:
    """Interval fixed-window rate limiter (port of py-mongo-sysbench)."""

    UNLIMITED_THRESHOLD = 9999999

    def __init__(self, rate_per_worker, interval_seconds=2):
        self.rate_per_worker = rate_per_worker
        self.interval_seconds = interval_seconds
        self.unlimited = (rate_per_worker <= 0 or
                          rate_per_worker >= self.UNLIMITED_THRESHOLD)
        self.max_per_interval = rate_per_worker * interval_seconds
        self._window_end = None
        self._count = 0

    def throttle(self):
        """Call once per operation; sleeps if the window budget is exhausted."""
        if self.unlimited or self.max_per_interval <= 0:
            return
        now = time.time()
        if self._window_end is None or now > self._window_end:
            self._window_end = now + self.interval_seconds
            self._count = 0
        self._count += 1
        if self._count >= self.max_per_interval:
            sleep_for = self._window_end - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._window_end = time.time() + self.interval_seconds
            self._count = 0


# ===========================================================================
# Percentiles & sampling
# ===========================================================================
def percentiles(sorted_values, pcts=(50, 95, 99)):
    """Return {pct: value} for an ALREADY-sorted list using linear interpolation."""
    result = {}
    n = len(sorted_values)
    if n == 0:
        return {p: 0.0 for p in pcts}
    if n == 1:
        return {p: float(sorted_values[0]) for p in pcts}
    for p in pcts:
        rank = (p / 100.0) * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        value = sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac
        result[p] = float(value)
    return result


def reservoir_add(sample_list, value, cap=10000, rng=None, counter=None):
    """Bounded latency sampling. Reservoir when rng given, else FIFO keep-recent."""
    if len(sample_list) < cap:
        sample_list.append(value)
        return
    if rng is not None:
        n = counter if counter is not None else (cap + 1)
        j = rng.randint(0, n - 1)
        if j < cap:
            sample_list[j] = value
    else:
        sample_list.pop(0)
        sample_list.append(value)


def csv_writer_for(file_name):
    """Open (truncating) a CSV file for the given base name; return (fh, writer)."""
    path = file_name if file_name.endswith(".csv") else file_name + ".csv"
    fh = open(path, "w", newline="")
    return fh, csv.writer(fh)


# ===========================================================================
# Reporter (runs in a thread in the main process)
# ===========================================================================
# module-level holder so the nested interval snapshot works simply
prev_load_inserts_holder = [0]


def perf_q_empty(perf_q):
    try:
        return perf_q.empty()
    except (NotImplementedError, OSError):
        return False


def _format_hms(seconds):
    """Format a duration in seconds as HH:MM:SS (or --:--:-- if unknown)."""
    if seconds is None or seconds != seconds or seconds < 0 or seconds == float("inf"):
        return "--:--:--"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _expected_load_total(phase, args):
    """Approximate number of inserts expected for a load phase (for % and ETA)."""
    if phase == "users":
        return args.users
    if phase == "follow":
        # ~max_follows/2 follows per user on average, each -> 2 inserts.
        return int(args.users * args.max_follows)
    if phase == "content":
        return args.users * args.messages_per_user
    return 0


def reporter(perf_q, num_workers, args):
    """Drain the performance queue, print periodic stats, and a final summary."""
    merged_sample_cap = 200000

    total_counts = {}
    total_sum_ms = {}
    merged_samples = {}
    total_errors = {}

    load_total_inserts = 0
    load_total_errors = 0
    load_phase_inserts = {}
    load_phase_errors = {}

    saw_opbatch = False
    saw_loadprogress = False

    completed = 0
    start_time = time.time()
    last_report = start_time

    prev_interval_counts = {}
    prev_interval_sum_ms = {}
    prev_load_inserts_holder[0] = 0
    interval_throughput = deque(maxlen=max(1, args.num_intervals_average))

    fh, writer = csv_writer_for(args.file_name)
    header_written = [False]

    def write_header_if_needed(kind):
        if header_written[0]:
            return
        if kind == "run":
            writer.writerow(["record_type", "elapsed_sec", "op", "interval_ops",
                             "interval_ops_per_sec", "interval_mean_ms",
                             "cumulative_count", "overall_ops_per_sec"])
        else:
            writer.writerow(["record_type", "elapsed_sec", "phase",
                             "interval_inserts", "interval_inserts_per_sec",
                             "cumulative_inserts", "overall_inserts_per_sec",
                             "cumulative_errors"])
        header_written[0] = True
        fh.flush()

    def apply_opbatch(msg):
        nonlocal saw_opbatch
        saw_opbatch = True
        for op, n in (msg.get("counts", {}) or {}).items():
            total_counts[op] = total_counts.get(op, 0) + n
        for op, s in (msg.get("sum_ms", {}) or {}).items():
            total_sum_ms[op] = total_sum_ms.get(op, 0.0) + s
        for op, lst in (msg.get("samples", {}) or {}).items():
            bucket = merged_samples.setdefault(op, [])
            if lst:
                room = merged_sample_cap - len(bucket)
                if room > 0:
                    bucket.extend(lst[:room])
        for op, e in (msg.get("errors", {}) or {}).items():
            total_errors[op] = total_errors.get(op, 0) + e

    def apply_loadprogress(msg):
        nonlocal saw_loadprogress, load_total_inserts, load_total_errors
        saw_loadprogress = True
        phase = msg.get("phase", "load")
        inserts = msg.get("inserts", 0) or 0
        errs = msg.get("errors", 0) or 0
        load_total_inserts += inserts
        load_total_errors += errs
        load_phase_inserts[phase] = load_phase_inserts.get(phase, 0) + inserts
        load_phase_errors[phase] = load_phase_errors.get(phase, 0) + errs

    def drain_available():
        nonlocal completed
        numMessages = 0
        #print("drain_available() start")
        while True:
            try:
                msg = perf_q.get(timeout=0.5)
                numMessages += 1
                #print("approximate queue size = {}".format(perf_q.qsize()))
                #print("-----------------------------------------------------------------------------------------------------------------------")
                #print("{}".format(msg))
            except queue.Empty:
                #print("drain_available() done - queue is empty - processed {} messages".format(numMessages))
                return
            except (EOFError, OSError):
                #print("drain_available() done - hit EOFError or OSError - processed {} messages".format(numMessages))
                return
            name = msg.get("name")
            if name == "opBatch":
                apply_opbatch(msg)
            elif name == "loadProgress":
                apply_loadprogress(msg)
            elif name == "processCompleted":
                completed += 1
            if completed >= num_workers and perf_q_empty(perf_q):
                #print("drain_available() done - queue is empty - all workers done - processed {} messages".format(numMessages))
                return

    def emit_interval(now):
        elapsed = now - start_time
        interval_secs = max(1e-9, now - last_report)
        if saw_opbatch or not saw_loadprogress:
            write_header_if_needed("run")
            overall_interval_ops = 0
            parts = []
            for op in sorted(total_counts.keys()):
                cur = total_counts.get(op, 0)
                prev = prev_interval_counts.get(op, 0)
                dcount = cur - prev
                overall_interval_ops += dcount
                dsum = total_sum_ms.get(op, 0.0) - prev_interval_sum_ms.get(op, 0.0)
                mean_ms = (dsum / dcount) if dcount > 0 else 0.0
                ops_sec = dcount / interval_secs
                parts.append(f"{op}={ops_sec:.0f}/s({mean_ms:.2f}ms)")
                writer.writerow(["run_interval", f"{elapsed:.1f}", op, dcount,
                                 f"{ops_sec:.2f}", f"{mean_ms:.4f}", cur, ""])
            overall_ops_sec = overall_interval_ops / interval_secs
            interval_throughput.append(overall_ops_sec)
            smoothed = sum(interval_throughput) / len(interval_throughput)
            print(f"[{elapsed:7.1f}s] ops/sec={overall_ops_sec:8.0f} "
                  f"(avg{len(interval_throughput)}={smoothed:8.0f}) | " + " ".join(parts))
            writer.writerow(["run_interval_total", f"{elapsed:.1f}", "ALL",
                             overall_interval_ops, f"{overall_ops_sec:.2f}", "",
                             sum(total_counts.values()), f"{smoothed:.2f}"])
            fh.flush()
        else:
            write_header_if_needed("load")
            prev = prev_load_inserts_holder[0]
            dins = load_total_inserts - prev
            ins_sec = dins / interval_secs
            overall_ins_sec = load_total_inserts / max(1e-9, elapsed)
            interval_throughput.append(ins_sec)
            smoothed = sum(interval_throughput) / len(interval_throughput)
            phase = next(iter(load_phase_inserts), "load")
            expected_total = _expected_load_total(phase, args)
            if expected_total > 0:
                pct = min(100.0, load_total_inserts / expected_total * 100.0)
                remaining = max(0, expected_total - load_total_inserts)
                eta_secs = (remaining / smoothed) if smoothed > 0 else None
            else:
                pct = 0.0
                eta_secs = None
            print(f"[{elapsed:7.1f}s] inserts={load_total_inserts:10d} "
                  f"inserts/sec={ins_sec:8.0f} (avg{len(interval_throughput):<5}={smoothed:8.0f}) "
                  f"{pct:5.1f}% complete ETA {_format_hms(eta_secs)}")
            writer.writerow(["load_interval", f"{elapsed:.1f}", "load", dins,
                             f"{ins_sec:.2f}", load_total_inserts,
                             f"{overall_ins_sec:.2f}", ""])
            fh.flush()

    # Main loop
    while completed < num_workers:
        drain_available()
        now = time.time()
        if now - last_report >= args.report_interval and completed < num_workers:
            emit_interval(now)
            prev_interval_counts.clear()
            prev_interval_counts.update(total_counts)
            prev_interval_sum_ms.clear()
            prev_interval_sum_ms.update(total_sum_ms)
            prev_load_inserts_holder[0] = load_total_inserts
            last_report = now

    # Final drain of any straggler messages
    try:
        while True:
            msg = perf_q.get_nowait()
            name = msg.get("name")
            if name == "opBatch":
                apply_opbatch(msg)
            elif name == "loadProgress":
                apply_loadprogress(msg)
    except queue.Empty:
        pass
    except (EOFError, OSError):
        pass

    total_elapsed = max(1e-9, time.time() - start_time)

    if saw_opbatch or not saw_loadprogress:
        _print_run_summary(total_counts, total_sum_ms, merged_samples,
                           total_errors, total_elapsed, writer, fh)
    else:
        _print_load_summary(load_total_inserts, load_phase_inserts,
                            load_phase_errors, load_total_errors,
                            total_elapsed, writer, fh)

    try:
        fh.flush()
        fh.close()
    except Exception:
        pass


def _print_run_summary(total_counts, total_sum_ms, merged_samples,
                       total_errors, total_elapsed, writer, fh):
    print("\n" + "=" * 105)
    print("FINAL SUMMARY (run phase)")
    print("=" * 105)
    header = (f"{'operation':<25}{'count':>12}{'ops/sec':>12}{'mean_ms':>10}"
              f"{'p50':>10}{'p95':>10}{'p99':>10}{'errors':>10}")
    print(header)
    print("-" * 105)
    writer.writerow([])
    writer.writerow(["FINAL_SUMMARY_RUN"])
    writer.writerow(["operation", "count", "ops_per_sec", "mean_ms",
                     "p50_ms", "p95_ms", "p99_ms", "errors"])

    grand_count = 0
    grand_sum = 0.0
    grand_errors = 0
    for op in sorted(total_counts.keys()):
        count = total_counts.get(op, 0)
        ssum = total_sum_ms.get(op, 0.0)
        errs = total_errors.get(op, 0)
        mean_ms = (ssum / count) if count > 0 else 0.0
        ops_sec = count / total_elapsed
        pct = percentiles(sorted(merged_samples.get(op, [])))
        grand_count += count
        grand_sum += ssum
        grand_errors += errs
        print(f"{op:<25}{count:>12}{ops_sec:>12.1f}{mean_ms:>10.2f}"
              f"{pct[50]:>10.2f}{pct[95]:>10.2f}{pct[99]:>10.2f}{errs:>10}")
        writer.writerow([op, count, f"{ops_sec:.2f}", f"{mean_ms:.4f}",
                         f"{pct[50]:.4f}", f"{pct[95]:.4f}", f"{pct[99]:.4f}", errs])

    print("-" * 105)
    overall_mean = (grand_sum / grand_count) if grand_count > 0 else 0.0
    overall_ops_sec = grand_count / total_elapsed
    print(f"{'TOTAL':<25}{grand_count:>12}{overall_ops_sec:>12.1f}"
          f"{overall_mean:>10.2f}{'':>10}{'':>10}{'':>10}{grand_errors:>10}")
    print(f"\nElapsed: {total_elapsed:.1f}s   Total ops: {grand_count}   "
          f"Overall: {overall_ops_sec:.1f} ops/sec   Errors: {grand_errors}")
    writer.writerow(["TOTAL", grand_count, f"{overall_ops_sec:.2f}",
                     f"{overall_mean:.4f}", "", "", "", grand_errors])
    fh.flush()


def _print_load_summary(load_total_inserts, load_phase_inserts,
                        load_phase_errors, load_total_errors,
                        total_elapsed, writer, fh):
    print("\n" + "=" * 80)
    print("FINAL SUMMARY (load phase)")
    print("=" * 80)
    ins_sec = load_total_inserts / total_elapsed
    print(f"Total inserts : {load_total_inserts}")
    print(f"Elapsed       : {total_elapsed:.1f}s")
    print(f"Inserts/sec   : {ins_sec:.1f}")
    print(f"Total errors  : {load_total_errors}")
    writer.writerow([])
    writer.writerow(["FINAL_SUMMARY_LOAD"])
    writer.writerow(["phase", "inserts", "errors"])
    if load_phase_inserts:
        print("-" * 80)
        print(f"{'phase':<24}{'inserts':>16}{'errors':>12}")
        for phase in sorted(load_phase_inserts.keys()):
            ins = load_phase_inserts.get(phase, 0)
            errs = load_phase_errors.get(phase, 0)
            print(f"{phase:<24}{ins:>16}{errs:>12}")
            writer.writerow([phase, ins, errs])
    writer.writerow(["TOTAL", load_total_inserts, load_total_errors])
    writer.writerow(["inserts_per_sec", f"{ins_sec:.2f}"])
    fh.flush()


# ===========================================================================
# DB setup + load workers
# ===========================================================================
def setup_load(args):
    """Connect, optionally drop collections, and create the Socialite indexes."""
    client = MongoClient(args.uri)
    try:
        db = client[args.database]

        if args.drop:
            for name in ("users", "content", "followers", "following"):
                db[name].drop()
                
        # disable compression if requested and Amazon DocumentDB
        if args.compression == 'none':
            for name in ("users", "content", "followers", "following"):
                print(f"[setup] creating collection {name} with compression disabled")
                db.create_collection(name=name,storageEngine={"documentDB":{"compression":{"enable":False}}})

        # Wrap each index creation so re-running without --drop does not crash.
        try:
            db.content.create_index([("_a", ASCENDING), ("_id", ASCENDING)])
        except Exception as e:
            print(f"[setup] content index skipped: {e}")
        try:
            db.followers.create_index([("_f", ASCENDING), ("_t", ASCENDING)], unique=True)
        except Exception as e:
            print(f"[setup] followers unique index skipped: {e}")
        try:
            db.following.create_index([("_f", ASCENDING), ("_t", ASCENDING)], unique=True)
        except Exception as e:
            print(f"[setup] following unique index skipped: {e}")
        try:
            db.followers.create_index([("_t", ASCENDING), ("_f", ASCENDING)])
        except Exception as e:
            print(f"[setup] followers reverse index skipped: {e}")
    finally:
        client.close()
    return None


def partition_range(worker_id, num_workers, total):
    """Return a contiguous half-open [start, end) slice for this worker."""
    start = worker_id * total // num_workers
    end = (worker_id + 1) * total // num_workers
    return start, end


def _count_dupes(bwe):
    """Count duplicate-key (11000) errors in a BulkWriteError (tolerated)."""
    dupes = 0
    other = 0
    details = getattr(bwe, "details", None) or {}
    for err in details.get("writeErrors", []):
        if err.get("code") == 11000:
            dupes += 1
        else:
            other += 1
    return dupes, other


def _flush(coll, ops):
    """Flush a batch of write ops with ordered=False. Returns (inserted, errors)."""
    if not ops:
        return 0, 0
    try:
        coll.bulk_write(ops, ordered=False)
        return len(ops), 0
    except BulkWriteError as bwe:
        dupes, other = _count_dupes(bwe)
        inserted = len(ops) - dupes - other
        return inserted, other
    except DuplicateKeyError:
        return len(ops) - 1, 0


def load_worker(worker_id, phase, args, perf_q):
    """Entry point for a load worker process (spawn-safe: own MongoClient)."""
    client = MongoClient(args.uri)
    try:
        db = client[args.database]
        rng = random.Random(args.seed + worker_id)
        start, end = partition_range(worker_id, args.processes, args.users)

        last_report = time.perf_counter()
        inserts_since = 0
        errors_since = 0

        def maybe_report(force=False):
            nonlocal last_report, inserts_since, errors_since
            now = time.perf_counter()
            if force or (now - last_report) >= args.worker_report_seconds:
                perf_q.put({
                    "name": "loadProgress",
                    "worker": worker_id,
                    "phase": phase,
                    "inserts": inserts_since,
                    "errors": errors_since,
                })
                last_report = now
                inserts_since = 0
                errors_since = 0

        if phase == "users":
            ops = []
            for i in range(start, end):
                ops.append(InsertOne({"_id": user_id(i)}))
                if len(ops) >= args.load_batch_size:
                    ins, err = _flush(db.users, ops)
                    inserts_since += ins
                    errors_since += err
                    ops = []
                    maybe_report()
            ins, err = _flush(db.users, ops)
            inserts_since += ins
            errors_since += err

        elif phase == "follow":
            sel = make_selector(args.follow_distribution, args.users, rng)
            follower_ops = []
            following_ops = []
            for i in range(start, end):
                me = user_id(i)
                k = rng.randint(0, args.max_follows)
                picked = set()
                attempts = 0
                max_attempts = k * 5 + 10
                while len(picked) < k and attempts < max_attempts:
                    attempts += 1
                    t = sel()
                    if t == i or t in picked:
                        continue
                    picked.add(t)
                    target = user_id(t)
                    follower_ops.append(InsertOne({"_f": me, "_t": target}))
                    following_ops.append(InsertOne({"_f": me, "_t": target}))

                if len(follower_ops) >= args.load_batch_size:
                    ins, err = _flush(db.followers, follower_ops)
                    inserts_since += ins
                    errors_since += err
                    follower_ops = []
                if len(following_ops) >= args.load_batch_size:
                    ins, err = _flush(db.following, following_ops)
                    inserts_since += ins
                    errors_since += err
                    following_ops = []
                maybe_report()

            ins, err = _flush(db.followers, follower_ops)
            inserts_since += ins
            errors_since += err
            ins, err = _flush(db.following, following_ops)
            inserts_since += ins
            errors_since += err

        elif phase == "content":
            ops = []
            for i in range(start, end):
                author = user_id(i)
                for _ in range(args.messages_per_user):
                    ops.append(InsertOne({"_a": author, "_m": random_message(rng)}))
                    if len(ops) >= args.load_batch_size:
                        ins, err = _flush(db.content, ops)
                        inserts_since += ins
                        errors_since += err
                        ops = []
                        maybe_report()
            ins, err = _flush(db.content, ops)
            inserts_since += ins
            errors_since += err

        maybe_report(force=True)
        perf_q.put({"name": "processCompleted", "worker": worker_id})
    finally:
        client.close()


def report_collection_info(args):
    """Print a formatted table of collStats for the loaded collections."""
    client = MongoClient(args.uri)
    GbDivisor = 1024*1024*1024
    try:
        db = client[args.database]

        header = (
            f"{'collection':<12} {'count':>12} {'avgObjSize':>12} "
            f"{'sizeGb':>14} {'storageSizeGb':>14} {'compRatio':>10} "
            f"{'totalIdxSizeGb':>14}"
        )
        print("\n" + header)
        print("-" * len(header))

        for name in ("users", "content", "followers", "following"):
            try:
                stats = db.command("collStats", name)
            except Exception as e:
                print(f"{name:<12} (unavailable: {e})")
                continue

            count = stats.get("numDocs", stats.get("count", 0)) or 0
            avg_obj = int(stats.get("avgObjSize", 0)) or 0
            size = stats.get("size", 0) or 0
            storage = stats.get("storageSize", 0) or 0
            total_idx = stats.get("totalIndexSize", 0) or 0
            comp_ratio = (size / storage) if storage > 0 else 1.0

            print(
                f"{name:<12} {count:>12} {avg_obj:>12} "
                f"{size/GbDivisor:>14.2f} {storage/GbDivisor:>14.2f} {comp_ratio:>10.2f} "
                f"{total_idx/GbDivisor:>14.2f}"
            )
    finally:
        client.close()


# ===========================================================================
# Workload operations
# ===========================================================================
OP_FOLLOW = "follow"
OP_UNFOLLOW = "unfollow"
OP_READ_TIMELINE = "read_timeline"
OP_SCROLL_TIMELINE = "scroll_timeline"
OP_SEND_CONTENT = "send_content"
OP_FOF_AGG = "friends_of_friends_agg"
OP_FOF_QUERY = "friends_of_friends_query"
OP_NAMES = [OP_FOLLOW, OP_UNFOLLOW, OP_READ_TIMELINE, OP_SCROLL_TIMELINE,
            OP_SEND_CONTENT, OP_FOF_AGG, OP_FOF_QUERY]


def op_send_content(ctx):
    author = ctx.pick_author()
    ctx.db.content.insert_one({"_a": author, "_m": ctx.random_message()})
    return OP_SEND_CONTENT


def op_follow(ctx):
    me = ctx.pick_user()
    target = ctx.pick_user()
    if target == me:
        return OP_FOLLOW
    try:
        ctx.db.following.insert_one({"_f": me, "_t": target})
    except DuplicateKeyError:
        pass
    try:
        ctx.db.followers.insert_one({"_f": me, "_t": target})
    except DuplicateKeyError:
        pass
    return OP_FOLLOW


def op_unfollow(ctx):
    me = ctx.pick_user()
    doc = ctx.db.following.find_one({"_f": me})
    if doc is None:
        return OP_UNFOLLOW
    target = doc["_t"]
    ctx.db.following.delete_one({"_f": me, "_t": target})
    ctx.db.followers.delete_one({"_f": me, "_t": target})
    return OP_UNFOLLOW


def op_read_timeline(ctx):
    u = ctx.pick_user()
    followees = [d["_t"] for d in ctx.db.following.find({"_f": u}, {"_t": 1, "_id": 0}).limit(ctx.fanout_limit)]
    if not followees:
        return OP_READ_TIMELINE
    list(ctx.db.content.find({"_a": {"$in": followees}}).sort([("_id", DESCENDING)]).limit(ctx.timeline_limit))
    return OP_READ_TIMELINE


def op_scroll_timeline(ctx):
    u = ctx.pick_user()
    followees = [d["_t"] for d in ctx.db.following.find({"_f": u}, {"_t": 1, "_id": 0}).limit(ctx.fanout_limit)]
    if not followees:
        return OP_SCROLL_TIMELINE
    newest = list(ctx.db.content.find({"_a": {"$in": followees}}).sort([("_id", DESCENDING)]).limit(1))
    if not newest:
        return OP_SCROLL_TIMELINE
    anchor = newest[0]["_id"]
    list(ctx.db.content.find({"_a": {"$in": followees}, "_id": {"$lt": anchor}}).sort([("_id", DESCENDING)]).limit(ctx.timeline_limit))
    return OP_SCROLL_TIMELINE


def op_fof_query(ctx):
    u = ctx.pick_user()
    direct = [d["_t"] for d in ctx.db.following.find({"_f": u}, {"_t": 1, "_id": 0}).limit(ctx.fanout_limit)]
    if not direct:
        return OP_FOF_QUERY
    fof = list(ctx.db.following.find({"_f": {"$in": direct}}, {"_t": 1, "_id": 0}).limit(1000))
    exclude = set(direct)
    exclude.add(u)
    _ = {d["_t"] for d in fof if d["_t"] not in exclude}
    return OP_FOF_QUERY


def op_fof_agg(ctx):
    u = ctx.pick_user()
    pipeline = [
        {"$match": {"_f": u}},
        {"$limit": ctx.fanout_limit},
        {"$lookup": {"from": "following", "localField": "_t", "foreignField": "_f", "as": "fof"}},
        {"$unwind": "$fof"},
        {"$group": {"_id": "$fof._t"}},
        {"$limit": 1000},
    ]
    list(ctx.db.following.aggregate(pipeline))
    return OP_FOF_AGG


OP_FUNCS = {
    OP_FOLLOW: op_follow,
    OP_UNFOLLOW: op_unfollow,
    OP_READ_TIMELINE: op_read_timeline,
    OP_SCROLL_TIMELINE: op_scroll_timeline,
    OP_SEND_CONTENT: op_send_content,
    OP_FOF_AGG: op_fof_agg,
    OP_FOF_QUERY: op_fof_query,
}


# ===========================================================================
# Run worker
# ===========================================================================
class RunContext:
    """Per-worker state handed to each op function."""

    def __init__(self, db, rng, user_sel, author_sel, args):
        self.db = db
        self.rng = rng
        self._user_sel = user_sel
        self._author_sel = author_sel
        self.fanout_limit = args.fanout_limit
        self.timeline_limit = args.timeline_limit

    def pick_user(self):
        return user_id(self._user_sel())

    def pick_author(self):
        return user_id(self._author_sel())

    def random_message(self):
        return random_message(self.rng)


def build_op_picker(args):
    """Return a bucket list of op names weighted by the *_pct arguments."""
    weights = {
        OP_FOLLOW: args.follow_pct,
        OP_UNFOLLOW: args.unfollow_pct,
        OP_READ_TIMELINE: args.read_timeline_pct,
        OP_SCROLL_TIMELINE: args.scroll_timeline_pct,
        OP_SEND_CONTENT: args.send_content_pct,
        OP_FOF_AGG: args.fof_agg_pct,
        OP_FOF_QUERY: args.fof_query_pct,
    }
    total = sum(max(0.0, w) for w in weights.values())
    if total <= 0:
        return list(OP_NAMES)  # equal probability
    bucket = []
    for op in OP_NAMES:
        n = int(round(max(0.0, weights[op]) / total * 100))
        bucket.extend([op] * n)
    if not bucket:
        return list(OP_NAMES)
    return bucket


def run_worker(worker_id, args, perf_q):
    """Entry point for a benchmark run worker process (spawn-safe)."""
    client = MongoClient(args.uri)
    try:
        db = client[args.database]
        rng = random.Random(args.seed + worker_id)

        n_users = 0
        try:
            n_users = db.users.estimated_document_count()
        except Exception:
            n_users = 0
        if not n_users or n_users <= 0:
            n_users = max(1, args.users)

        user_sel = make_selector(args.user_distribution, n_users, rng)
        author_sel = make_selector(args.content_distribution, n_users, rng)
        ctx = RunContext(db, rng, user_sel, author_sel, args)

        picker = build_op_picker(args)
        limiter = RateLimiter(args.rate_limit // max(1, args.processes))

        per_worker_ops = 0
        if args.num_operations > 0:
            per_worker_ops = math.ceil(args.num_operations / args.processes)
        deadline = None
        if args.run_seconds > 0:
            deadline = time.perf_counter() + args.run_seconds

        # local per-batch accumulators (flushed to the queue every ~2s)
        counts = {}
        sums = {}
        samples = {}
        errors = {}
        seen = {}  # per-op total seen (for reservoir sampling)

        def reset_batch():
            counts.clear()
            sums.clear()
            samples.clear()
            errors.clear()

        def flush_batch():
            if not counts and not errors:
                return
            perf_q.put({
                "name": "opBatch",
                "worker": worker_id,
                "counts": dict(counts),
                "sum_ms": dict(sums),
                "samples": {op: list(lst) for op, lst in samples.items()},
                "errors": dict(errors),
            })
            reset_batch()

        n_done = 0
        last_flush = time.perf_counter()

        while True:
            if per_worker_ops and n_done >= per_worker_ops:
                break
            if deadline is not None and time.perf_counter() >= deadline:
                break

            op_name = rng.choice(picker)
            fn = OP_FUNCS[op_name]

            limiter.throttle()
            t0 = time.perf_counter()
            ok = True
            try:
                fn(ctx)
            except Exception:
                ok = False
            dt_ms = (time.perf_counter() - t0) * 1000.0

            counts[op_name] = counts.get(op_name, 0) + 1
            n_done += 1
            if ok:
                sums[op_name] = sums.get(op_name, 0.0) + dt_ms
                seen[op_name] = seen.get(op_name, 0) + 1
                reservoir_add(samples.setdefault(op_name, []), dt_ms,
                              cap=1000, rng=rng, counter=seen[op_name])
            else:
                errors[op_name] = errors.get(op_name, 0) + 1

            now = time.perf_counter()
            if now - last_flush >= args.worker_report_seconds:
                flush_batch()
                last_flush = now

        flush_batch()
        perf_q.put({"name": "processCompleted", "worker": worker_id})
    finally:
        client.close()


# ===========================================================================
# Orchestration
# ===========================================================================
def _run_phase(target, phase_args, args, num_workers):
    """Spawn workers + reporter for one phase and wait for completion."""
    perf_q = mp.Manager().Queue()
    rep = threading.Thread(target=reporter, args=(perf_q, num_workers, args))
    rep.start()
    procs = []
    for w in range(num_workers):
        p = mp.Process(target=target, args=(w,) + phase_args(w, perf_q))
        procs.append(p)
        p.start()
    for p in procs:
        p.join()
    rep.join()


def run_load(args):
    print(f"socialite2 load: uri={args.uri} db={args.database} "
          f"users={args.users} max-follows={args.max_follows} "
          f"messages/user={args.messages_per_user} processes={args.processes} "
          f"follow-dist={args.follow_distribution}")
    print("Creating collections and indexes ...")
    setup_load(args)

    for phase in ("users", "follow", "content"):
        print(f"\n=== load phase: {phase} ===")
        _run_phase(
            load_worker,
            lambda w, q, _phase=phase: (_phase, args, q),
            args,
            args.processes,
        )

    report_collection_info(args)
    print(f"\nLoad complete. Metrics written to {args.file_name}.csv")


def run_benchmark(args):
    if args.run_seconds <= 0 and args.num_operations <= 0:
        raise SystemExit("error: --run requires --run-seconds or --num-operations (> 0)")

    print(f"socialite2 run: uri={args.uri} db={args.database} "
          f"processes={args.processes} "
          f"{'seconds=' + str(args.run_seconds) if args.run_seconds > 0 else 'ops=' + str(args.num_operations)} "
          f"rate-limit={args.rate_limit} user-dist={args.user_distribution} "
          f"content-dist={args.content_distribution}")

    _run_phase(
        run_worker,
        lambda w, q: (args, q),
        args,
        args.processes,
    )

    report_collection_info(args)
    print(f"\nRun complete. Metrics written to {args.file_name}.csv")


def main(argv=None):
    args = parse_args(argv)
    if not args.load and not args.run:
        raise SystemExit("error: specify exactly one of --load or --run")
    if args.load and args.run:
        raise SystemExit("error: --load and --run are mutually exclusive; run them separately")
    if args.processes < 1:
        raise SystemExit("error: --processes must be >= 1")

    if MongoClient is None:
        raise SystemExit("error: pymongo is not installed. Run: pip install pymongo")

    if args.load:
        run_load(args)
    else:
        run_benchmark(args)


if __name__ == "__main__":
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass
    main()

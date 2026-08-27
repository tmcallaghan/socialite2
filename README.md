# socialite2

socialite2 is a Python implementation of the [Socialite](https://github.com/mongodb-labs/socialite/tree/master/src/main/java/com/mongodb/socialite) social media benchmark for MongoDB API compatible databases. It is a single-script benchmark tool that can load data and execute the benchmark against any MongoDB-compatible endpoint (MongoDB, Amazon DocumentDB, Azure DocumentDB, and [Open Source DocumentDB](https://github.com/documentdb/documentdb)).

Everything lives in one file, [`socialite2.py`](socialite2.py). It handles both data loading (`--load`) and benchmark execution (`--run`), using `multiprocessing` workers, a shared performance queue, and a reporter thread — the same architecture as [py-mongo-sysbench](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench).

## Requirements

- Python 3.8+
- `pymongo` (the only runtime dependency)
- `pytest` (only to run the unit tests)

```bash
pip install pymongo
```

## Quick start

Load a small dataset into a local MongoDB and run the benchmark for a minute:

```bash
# 1. Load: 1000 users, a zipfian follow graph (<=100 follows each), 100 posts each
python socialite2.py --load --drop --users 1000 --maxfollows 100 --messages-per-user 100 \
    --uri mongodb://localhost:27017

# 2. Run: 8 workers for 60 seconds
python socialite2.py --run --processes 8 --run-seconds 60 \
    --uri mongodb://localhost:27017
```

## Schema

socialite2 reproduces the Socialite schema exactly (short field keys and all), so it can even point at data produced by the original Java tool.

| Collection  | `_id`                       | Fields                          | Indexes |
|-------------|-----------------------------|---------------------------------|---------|
| `users`     | userId string (`u0`, `u1`…) | `_d` (optional data)            | `_id` |
| `content`   | `ObjectId` (encodes post time) | `_a` author, `_m` message    | `{_a:1, _id:1}` |
| `followers` | `ObjectId`                  | `_f` owner, `_t` peer           | `{_f:1, _t:1}` unique, `{_t:1, _f:1}` |
| `following` | `ObjectId`                  | `_f` follower, `_t` followed    | `{_f:1, _t:1}` unique |

This build uses the **fanout-on-read** feed model: posts live only in `content`, and a timeline is computed at read time by querying the recent content of the users you follow. Post recency is derived from `ObjectId` ordering (no separate date field).

## Workload

`--run` executes all seven Socialite operations, chosen by weighted random selection (weights are the `*-pct` flags below, normalized):

| Operation                  | What it does |
|----------------------------|--------------|
| `follow`                   | Insert a follow edge into `followers` + `following` |
| `unfollow`                 | Delete an existing follow edge from both collections |
| `send_content`             | Insert a new post into `content` |
| `read_timeline`            | Fetch the newest posts from the users you follow |
| `scroll_timeline`          | Anchored pagination — the page of posts older than a recent anchor |
| `friends_of_friends_query` | Two-hop follow expansion via plain queries |
| `friends_of_friends_agg`   | Two-hop follow expansion via a `$lookup` aggregation |

## Work distributions

Both load and run can skew which keys are touched. Choose with `--follow-distribution`, `--user-distribution`, and `--content-distribution`:

- **`uniform`** — every key equally likely.
- **`zipfian`** — a small subset of keys is very hot. Implemented as a *scrambled* Zipfian (faithful pure-Python port of YCSB's `ScrambledZipfianGenerator`): a Zipfian value is drawn and then FNV-1a hashed across the keyspace, so the hot set is **scattered** rather than clustered at the low indices. This makes hotness independent of key/storage locality — the correct behavior for a database benchmark.
- **`latest`** — the most recently created keys are hottest (YCSB `SkewedLatestGenerator`).

## Output

Per-interval throughput/latency lines are printed to stdout and appended to `<file-name>.csv` (default `socialite2.csv`). At the end, socialite2 prints a per-operation summary with count, ops/sec, mean, and **p50 / p95 / p99** latency, followed by `collStats` for every collection (document counts, sizes, compression ratio, index size).

## Common options

Run `python socialite2.py --help` for the full list. Highlights:

| Flag | Default | Meaning |
|------|---------|---------|
| `--uri` | `mongodb://localhost:27017` | Connection string |
| `--database` | `socialite` | Target database |
| `--processes` | `4` | Worker processes |
| `--run-seconds` / `--num-operations` | `0` | Stop after N seconds or N total ops (set one for `--run`) |
| `--rate-limit` | unlimited | Target ops/sec across all workers |
| `--seed` | `42` | Base RNG seed (each worker derives its own; runs are reproducible) |
| `--users` / `--maxfollows` / `--messages-per-user` | `1000` / `100` / `100` | Load sizing |
| `--follow-pct`, `--read-timeline-pct`, … | see `--help` | Operation mix for `--run` |

## Tests

The distribution generators (the most correctness-sensitive part) have a statistical unit-test suite that needs no database:

```bash
pip install pytest
pytest test_socialite2.py -v
```

## References

- [Socialite](https://github.com/mongodb-labs/socialite) — the original Java benchmark
- [py-mongo-sysbench](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench) and [python-bench02](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/python-bench02) — architectural inspiration
- [YCSB generators](https://github.com/brianfrankcooper/YCSB/tree/master/core/src/main/java/site/ycsb/generator) — the reference Zipfian / scrambled / latest implementations

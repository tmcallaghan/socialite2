# CLAUDE.md - socialite2

## Project Overview

socialite2 is a Python implementation of the [Socialite](https://github.com/mongodb-labs/socialite) social media benchmark for MongoDB API compatible databases. It is a single-script benchmark tool that can load data and execute the benchmark against any MongoDB-compatible endpoint (MongoDB, Amazon DocumentDB, Azure DocumentDB, and [Open Source DocumentDB](https://github.com/documentdb/documentdb)).

## Goals

1. **Single script** - one Python file (`socialite2.py`) handles both data loading (`--load`) and benchmark execution (`--run`)
2. **Inspired by py-mongo-sysbench and python-bench02** - follow the architectural patterns from [py-mongo-sysbench](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench) and [python-bench02](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/python-bench02) - argparse CLI, multiprocessing workers, a shared performance queue, and a reporter thread
3. **Start with the existing Socialite** - implement the Socialite workload and benchmark at [Socialite](https://github.com/mongodb-labs/socialite)
4. **Python multiprocessing for concurrency** - use `multiprocessing.Process` for workers and `multiprocessing.Manager().Queue()` for performance reporting, not threading
5. **Keep it simple** - minimal dependencies (pymongo only), no plugin architecture, no abstract classes

## Architecture

### Single-file layout

All code lives in `socialite2.py`. No packages, no modules, no config files beyond CLI args.

### Two modes

- `--load` - bulk-insert documents into the target collections using batched `insert_many` / `bulk_write`. Each worker owns a key range partition.
- `--run` - execute the Socialite workload using the loaded data. Each worker runs operations independently, choosing operation type by weighted random selection.

### Concurrency model (from py-mongo-sysbench)

- Workers are `multiprocessing.Process` instances, each with its own pymongo `MongoClient`
- A shared `multiprocessing.Manager().Queue()` carries performance messages from workers to a reporter
- A reporter thread in the main process drains the queue and prints periodic throughput/latency stats
- Detailed and well formatted post run metrics including percentile based latency, throughput, and collection and index sizing
- Rate limiting is per-worker using a token-bucket or interval-based approach

### Schema

Match the Socialite schema exactly.

### Work distributions

Implement three techniques for distributing the data load and benchmark execution
- **uniform** - all data equally likely
- **zipfian** - most accesses go to a small subset of data
- **latest** - newest data is most popular

## CLI Interface

Follow the py-mongo-sysbench pattern for argument structure, create an argument for every variable each with sensible defaults.

## Key Design Decisions

- **pymongo is the only external dependency.** No numpy, no special stats libraries.
- **Zipfian distribution** must be implemented in pure Python.
- **Each worker creates its own MongoClient** - pymongo clients are not fork-safe.
- **Performance reporting** uses a queue + reporter thread pattern, not shared counters, to avoid lock contention.
- **Output** goes to stdout and optionally to a CSV file, matching py-mongo-sysbench's dual-output approach.
- **No ORM or abstraction layer** - direct pymongo calls.

## Coding Conventions

- Python 3.8+ (f-strings, walrus operator OK)
- No type hints required but welcome
- Functions over classes where possible
- `argparse` for CLI
- `multiprocessing` for parallelism (not `threading`, not `concurrent.futures`)
- Keep functions short and focused
- Use `random.Random` instances per-worker (seeded) to avoid contention on the global random state

## Dependencies

- Python 3.8+
- pymongo

## Reference Materials

- existing Socialite benchmark - https://github.com/mongodb-labs/socialite
- python py-mongo-sysbench - https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench
- python bench02 - https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/python-bench02

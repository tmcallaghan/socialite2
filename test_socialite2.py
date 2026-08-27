"""Pytest suite for the numerical core of socialite2.

These tests validate the YCSB-derived random generators and distribution
selectors that live in the assembled ``socialite2.py`` module. All tests are
pure stdlib + pytest (no live database). Because the generators are random,
the assertions use statistical tolerances chosen to be robust to RNG noise
while still confirming the intended distribution shape.
"""

import random
from collections import Counter

from socialite2 import (
    ZipfianGenerator,
    ScrambledZipfianGenerator,
    CounterGenerator,
    SkewedLatestGenerator,
    fnvhash64,
    make_selector,
    random_message,
    user_id,
)


def test_zipfian_hot_low_indices():
    z = ZipfianGenerator(1000, rng=random.Random(1))
    freq = Counter(z.next_value() for _ in range(100_000))
    assert freq[0] > freq[100] > freq[500]


def test_scrambled_scatters_hot_set():
    s = ScrambledZipfianGenerator(1000, rng=random.Random(2))
    draws = [s.next_value() for _ in range(100_000)]
    assert all(0 <= d < 1000 for d in draws)
    freq = Counter(draws)
    hottest, _ = freq.most_common(1)[0]
    assert hottest not in {0, 1, 2}


def test_scrambled_is_skewed():
    # ScrambledZipfian draws over the full YCSB domain [0, 1e10] (zetan=26.469),
    # so the ~10 hottest source values carry ~11% of the mass. Folded into 1000
    # buckets, the top-10 buckets therefore hold well above the uniform share
    # (10/1000 = 1%) but nowhere near 20% -- this is canonical YCSB behavior.
    s = ScrambledZipfianGenerator(1000, rng=random.Random(2))
    n = 100_000
    draws = [s.next_value() for _ in range(n)]
    freq = Counter(draws)
    top10 = sum(c for _, c in freq.most_common(10))
    uniform_top10 = 10 / 1000 * n  # == 1000
    assert top10 > 5 * uniform_top10   # clearly skewed, >> uniform
    assert top10 < 0.20 * n            # but not clustered like a raw 1000-item zipfian


def test_latest_favors_newest():
    sel = make_selector("latest", 1000, random.Random(3))
    draws = [sel() for _ in range(100_000)]
    mean_idx = sum(draws) / len(draws)
    assert mean_idx > 700
    top_decile = sum(1 for d in draws if 900 <= d < 1000)
    assert top_decile > 0.5 * len(draws)


def test_uniform_flat():
    sel = make_selector("uniform", 100, random.Random(4))
    freq = Counter(sel() for _ in range(100_000))
    expected = 1000
    for bucket in range(100):
        assert abs(freq[bucket] - expected) < 0.20 * expected


def test_fnvhash64_range_and_stable():
    values = [0, 1, 2, 42, 12345, 9_999_999_999, 2 ** 40 + 7]
    for x in values:
        h = fnvhash64(x)
        assert isinstance(h, int)
        assert h >= 0
        assert fnvhash64(x) == h  # stable across repeated calls
    hashes = {fnvhash64(x) for x in values}
    assert len(hashes) > 1  # different inputs generally differ


def test_counter():
    c = CounterGenerator(5)
    assert c.next_value() == 5
    assert c.next_value() == 6
    assert c.next_value() == 7
    assert c.last_value() == 7


def test_random_message_length():
    rng = random.Random(7)
    for _ in range(1000):
        m = random_message(rng)
        assert 10 <= len(m) <= 140
        assert all(ch.islower() and ch.isalpha() for ch in m)


def test_user_id():
    assert user_id(0) == "u0"
    assert user_id(42) == "u42"


def test_skewed_latest_favors_newest():
    counter = CounterGenerator(0)
    for _ in range(1000):
        counter.next_value()
    gen = SkewedLatestGenerator(counter, rng=random.Random(9))
    draws = [gen.next_value() for _ in range(50_000)]
    mx = counter.last_value()
    assert all(0 <= d <= mx for d in draws)
    mean_idx = sum(draws) / len(draws)
    assert mean_idx > 0.7 * mx

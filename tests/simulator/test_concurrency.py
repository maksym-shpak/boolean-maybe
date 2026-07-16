"""Concurrency tests: same-key serialization, different-key overlap, and the
fixed 32 active-handler bound.
"""

from __future__ import annotations

import http.client
import json
import threading
import time

from boolean_maybe.simulator.waiting import Waiter


def _submit(sim, key: str, payload: dict[str, object]) -> tuple[int, dict[str, object]]:
    body = json.dumps(payload).encode("utf-8")
    status, parsed, _ = sim.request(
        "POST",
        "/jobs",
        body=body,
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
    )
    assert parsed is not None
    return status, parsed


# -- Same-key serialization: exactly one winner among concurrent equivalents --


def test_same_key_concurrent_equivalent_submissions_have_one_winner(
    make_simulator,
) -> None:
    for iteration in range(20):
        sim = make_simulator()
        key = f"race-{iteration}"
        results: list[tuple[int, dict[str, object]]] = []
        results_lock = threading.Lock()
        barrier = threading.Barrier(16)

        def worker(sim=sim, key=key) -> None:
            barrier.wait(timeout=5)
            status, parsed = _submit(sim, key, {"work": "same"})
            with results_lock:
                results.append((status, parsed))

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert len(results) == 16
        created = [r for r in results if r[1]["replayed"] is False]
        replayed = [r for r in results if r[1]["replayed"] is True]
        assert len(created) == 1
        assert len(replayed) == 15
        remote_ids = {r[1]["remote_request_id"] for r in results}
        digests = {r[1]["payload_digest"] for r in results}
        assert len(remote_ids) == 1
        assert len(digests) == 1


# -- Different keys proceed concurrently without mixing ordinals/records ----


def test_different_keys_concurrent_no_cross_contamination(make_simulator) -> None:
    sim = make_simulator()
    key_count = 25
    results: dict[str, dict[str, object]] = {}
    results_lock = threading.Lock()
    barrier = threading.Barrier(key_count)

    def worker(index: int) -> None:
        key = f"distinct-{index}"
        barrier.wait(timeout=5)
        _, parsed = _submit(sim, key, {"work": index})
        with results_lock:
            results[key] = parsed

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(key_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert len(results) == key_count
    digests = {parsed["payload_digest"] for parsed in results.values()}
    assert len(digests) == key_count  # every distinct payload has its own digest
    for index in range(key_count):
        key = f"distinct-{index}"
        assert results[key]["idempotency_key"] == key
        assert results[key]["replayed"] is False


# -- Fixed 32 active-handler bound --------------------------------------------


class BlockingWaiter(Waiter):
    """Blocks every call until released, tracking peak concurrent waiters."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self._release_event = threading.Event()

    def release(self) -> None:
        self._release_event.set()

    def wait(self, shutdown_event: threading.Event) -> bool:
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        aborted = False
        while not self._release_event.is_set():
            if shutdown_event.wait(0.01):
                aborted = True
                break
        with self._lock:
            self.active -= 1
        return aborted


def test_bounded_to_32_active_handlers(make_simulator) -> None:
    waiter = BlockingWaiter()
    plan = {
        "version": 1,
        "rules": [
            {
                "operation": "submission",
                "idempotency_key": "*",
                "scenario": "connect_timeout",
            }
        ],
    }
    sim = make_simulator(plan, waiter=waiter, max_workers=32)

    total_requests = 40
    errors: list[BaseException] = []

    def send(index: int) -> None:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", sim.port, timeout=10)
            body = json.dumps({"work": index}).encode("utf-8")
            conn.request(
                "POST",
                "/jobs",
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": f"bound-{index}",
                },
            )
            try:
                conn.getresponse()
            except (http.client.HTTPException, OSError):
                pass  # aborted requests close without a complete response
            conn.close()
        except OSError as exc:  # pragma: no cover - diagnostic aid on failure
            errors.append(exc)

    threads = [threading.Thread(target=send, args=(i,)) for i in range(total_requests)]
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with waiter._lock:
            if waiter.active >= 32:
                break
        time.sleep(0.02)

    with waiter._lock:
        active_snapshot = waiter.active
        peak_snapshot = waiter.peak

    assert active_snapshot == 32
    assert peak_snapshot <= 32

    waiter.release()

    for thread in threads:
        thread.join(timeout=10)

    assert not errors
    assert waiter.peak <= 32
    assert not any(thread.is_alive() for thread in threads)


def test_shutdown_completes_within_bound_when_handlers_saturated(
    make_simulator,
) -> None:
    """Regression test: with every worker slot busy and one extra connection
    blocking the accept loop's own semaphore wait for a free slot, clean
    shutdown must still complete within its bound instead of deadlocking
    (the accept loop waiting on the semaphore, which only frees once the
    saturated handlers notice `shutdown_event`).
    """

    waiter = BlockingWaiter()
    plan = {
        "version": 1,
        "rules": [
            {
                "operation": "submission",
                "idempotency_key": "*",
                "scenario": "connect_timeout",
            }
        ],
    }
    max_workers = 2
    sim = make_simulator(plan, waiter=waiter, max_workers=max_workers)

    def send(index: int) -> None:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", sim.port, timeout=30)
            body = json.dumps({"work": index}).encode("utf-8")
            conn.request(
                "POST",
                "/jobs",
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": f"sat-{index}",
                },
            )
            try:
                conn.getresponse()
            except (http.client.HTTPException, OSError):
                pass
            conn.close()
        except OSError:
            pass

    # One extra connection beyond max_workers keeps the accept loop itself
    # blocked acquiring a semaphore slot for it.
    total_requests = max_workers + 1
    threads = [threading.Thread(target=send, args=(i,)) for i in range(total_requests)]
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with waiter._lock:
            if waiter.active >= max_workers:
                break
        time.sleep(0.02)
    assert waiter.active == max_workers

    # Give the accept loop a moment to actually become stuck dispatching
    # the (max_workers + 1)-th connection before shutting down.
    time.sleep(0.3)

    started = time.monotonic()
    sim.server.shutdown_and_close(timeout=2.0)
    elapsed = time.monotonic() - started

    # The contract bounds the handler-join wait at 2 seconds; allow only a
    # small scheduling tolerance on top, not slack large enough to hide a
    # regression back toward the old deadlock.
    assert elapsed < 2.5

    for thread in threads:
        thread.join(timeout=5)
    assert not any(thread.is_alive() for thread in threads)

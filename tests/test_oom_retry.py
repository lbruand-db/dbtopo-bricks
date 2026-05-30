"""Tests for the OOM retry harness used by `load_cmd`.

The harness is pure Python — `attempt` and `on_retry` are callables —
so we test the full retry policy without any Spark machinery.
"""

from __future__ import annotations

import pytest

from dbtopo.cli import run_with_oom_retry

OOM_MSG = "[MEMORY_LIMIT_SERVERLESS] task exceeded budget"
KERNEL_OOM_MSG = "Python worker exited unexpectedly with exit code '128'"


def test_succeeds_first_try_does_not_invoke_on_retry() -> None:
    calls: list[int] = []
    retries: list[tuple[int, int]] = []

    def attempt(bs: int) -> str:
        calls.append(bs)
        return "ok"

    final, result = run_with_oom_retry(
        5000,
        attempt=attempt,
        on_retry=lambda p, n: retries.append((p, n)),
    )
    assert final == 5000
    assert result == "ok"
    assert calls == [5000]
    assert retries == []


def test_one_oom_then_success_halves_batch_size() -> None:
    calls: list[int] = []
    retries: list[tuple[int, int]] = []

    def attempt(bs: int) -> int:
        calls.append(bs)
        if len(calls) == 1:
            raise RuntimeError(OOM_MSG)
        return bs * 10  # arbitrary "n_batches"-like return

    final, result = run_with_oom_retry(
        4000,
        attempt=attempt,
        on_retry=lambda p, n: retries.append((p, n)),
    )
    assert final == 2000
    assert result == 20000
    assert calls == [4000, 2000]
    assert retries == [(4000, 2000)]


def test_kernel_oom_message_also_triggers_retry() -> None:
    """`exit code '128'` (kernel-killed worker) must retry too."""
    calls: list[int] = []

    def attempt(bs: int) -> None:
        calls.append(bs)
        if len(calls) == 1:
            raise RuntimeError(KERNEL_OOM_MSG)

    run_with_oom_retry(1000, attempt=attempt)
    assert calls == [1000, 500]


def test_multiple_ooms_halve_until_success() -> None:
    calls: list[int] = []
    retries: list[tuple[int, int]] = []

    def attempt(bs: int) -> None:
        calls.append(bs)
        if bs > 250:
            raise RuntimeError(OOM_MSG)

    final, _ = run_with_oom_retry(
        2000,
        attempt=attempt,
        on_retry=lambda p, n: retries.append((p, n)),
    )
    assert final == 250
    assert calls == [2000, 1000, 500, 250]
    assert retries == [(2000, 1000), (1000, 500), (500, 250)]


def test_halving_floors_at_one() -> None:
    """2 → 1 (not 0), then if still OOM raises with diagnostic."""
    calls: list[int] = []

    def attempt(bs: int) -> None:
        calls.append(bs)
        raise RuntimeError(OOM_MSG)

    with pytest.raises(RuntimeError, match="batch_size=1"):
        run_with_oom_retry(
            2,
            attempt=attempt,
            max_blob_bytes=12_345_678,
            layer="cours_d_eau",
            dept="069",
        )
    assert calls == [2, 1]


def test_single_row_too_big_raises_with_diagnostic() -> None:
    def attempt(bs: int) -> None:
        raise RuntimeError(OOM_MSG)

    with pytest.raises(RuntimeError) as excinfo:
        run_with_oom_retry(
            1,
            attempt=attempt,
            max_blob_bytes=99_000_000,
            layer="zone_de_vegetation",
            dept="075",
        )
    msg = str(excinfo.value)
    assert "batch_size=1" in msg
    assert "zone_de_vegetation" in msg
    assert "075" in msg
    assert "99000000" in msg


def test_non_oom_exception_propagates_unwrapped() -> None:
    """Anything that isn't an OOM (e.g. a SQL/schema error) must
    bubble straight through — we mustn't silently retry on it."""

    def attempt(bs: int) -> None:
        raise ValueError("TABLE_OR_VIEW_NOT_FOUND: foo")

    with pytest.raises(ValueError, match="TABLE_OR_VIEW_NOT_FOUND"):
        run_with_oom_retry(1000, attempt=attempt)


def test_on_retry_default_noop_does_not_crash() -> None:
    """Caller can omit on_retry — harness still retries correctly."""
    calls: list[int] = []

    def attempt(bs: int) -> None:
        calls.append(bs)
        if len(calls) == 1:
            raise RuntimeError(OOM_MSG)

    run_with_oom_retry(100, attempt=attempt)
    assert calls == [100, 50]

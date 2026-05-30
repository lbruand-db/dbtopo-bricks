"""Tests for the OOM-detection helper used by the load_cmd retry loop.

The retry loop itself sits inside `load_cmd` and is wrapped in Spark
machinery that isn't worth standing up here; the easily-wrong bit is the
substring match that classifies an exception as a retriable executor OOM.
"""

from __future__ import annotations

import pytest

from dbtopo.cli import _is_oom_error


@pytest.mark.parametrize(
    "msg",
    [
        # Structured Spark error code (serverless + classic).
        "[MEMORY_LIMIT_SERVERLESS] task exceeded budget",
        "org.apache.spark.SparkException: MEMORY_LIMIT exceeded",
        # Kernel OOM-kill of the Python worker — surfaces without
        # MEMORY_LIMIT in the message.
        "Python worker exited unexpectedly with exit code '128'",
    ],
)
def test_recognises_oom_messages(msg: str) -> None:
    assert _is_oom_error(msg) is True


@pytest.mark.parametrize(
    "msg",
    [
        "TABLE_OR_VIEW_NOT_FOUND: table foo does not exist",
        "AnalysisException: column 'bar' is ambiguous",
        "exit code '1'",  # close-but-not-128 must not match
        "",
    ],
)
def test_does_not_misclassify_non_oom(msg: str) -> None:
    assert _is_oom_error(msg) is False

# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
# MindIE is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

"""Bounded worker observations, transported over the existing management IPC.

Enabled explicitly by MOTOR_RUNTIME_TARGET_ID and MOTOR_RUNTIME_STATE_DIR.
Only a confirmed query-fault latch is persisted, on transitions, so restarting a
worker cannot silently clear a previously failed query path. No token or request
identity is retained. Management merges worker slots, never sums failure streaks.
"""

import contextvars
import hashlib
import json
import math
import os
import re
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

QUERY_REASON = contextvars.ContextVar("motor_query_reason", default="not_queried")
_state: "WorkerObservation | None" = None


class WorkerObservation:
    """One writer per inference-worker slot, independent counters per boot."""

    def __init__(self, target_id: str, index: int, state_dir: Path) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_:-]{1,128}", target_id) or not 0 <= index < 256:
            raise ValueError("invalid runtime observation identity")
        self.target_id = target_id
        self.index = index
        self.boot = uuid.uuid4().hex
        self.started_at = time.time()
        self.sequence = 0
        self.path = state_dir / (hashlib.sha256(target_id.encode()).hexdigest() + "-" + str(index) + ".json")
        self.lock = threading.Lock()
        self.success_at = 0.0
        self.failure_at = 0.0
        self.failures = 0
        self.pending = False
        self.persistence_ready = True
        self.query_counts = {key: 0 for key in ("success", "failure", "circuit_open")}
        self.decisions = {key: 0 for key in ("affinity", "load_balance", "fallback_query_failure", "other")}
        self.fallback_at = 0.0
        try:
            if not state_dir.is_dir() or state_dir.stat().st_mode & 0o077:
                raise ValueError("runtime state directory must exist with mode 0700")
            if self.path.exists():
                if self.path.stat().st_size > 1024:
                    raise ValueError("invalid runtime recovery state")
                value = json.loads(self.path.read_text())
                if (
                    not isinstance(value, dict)
                    or value.get("target_id") != target_id
                    or not isinstance(value.get("query_fault"), bool)
                ):
                    raise ValueError("invalid runtime recovery state")
                self.pending = value["query_fault"]
        except (OSError, ValueError):
            self.persistence_ready = False

    def _persist(self) -> None:
        temporary = None
        try:
            if self.path.parent.stat().st_mode & 0o077:
                raise OSError("runtime state directory permissions changed")
            fd, temporary = tempfile.mkstemp(prefix=".runtime-", dir=self.path.parent)
            with os.fdopen(fd, "w") as stream:
                os.fchmod(stream.fileno(), 0o600)
                json.dump({"target_id": self.target_id, "query_fault": self.pending}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            self.persistence_ready = True
        except OSError:
            self.persistence_ready = False
        finally:
            if temporary and os.path.exists(temporary):
                try:
                    os.unlink(temporary)
                except OSError:
                    self.persistence_ready = False

    def query(self, outcome: str) -> None:
        with self.lock:
            before = self.pending
            self.query_counts[outcome] += 1
            if outcome == "success":
                self.success_at = time.time()
                self.failures = 0
                self.pending = False
            elif outcome == "failure":
                self.failure_at = time.time()
                self.failures += 1
                self.pending |= self.failures >= 3
            if before != self.pending or (outcome == "success" and not self.persistence_ready):
                self._persist()

    def decision(self, policy: str, reason: str) -> None:
        with self.lock:
            key = (
                "affinity" if policy == "kv_cache_affinity" else "load_balance" if policy == "load_balance" else "other"
            )
            if key == "load_balance" and reason in ("query_failure", "circuit_open"):
                key = "fallback_query_failure"
                self.fallback_at = time.time()
            self.decisions[key] += 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            self.sequence += 1
            return {
                "worker_index": self.index,
                "worker_start_id": self.boot,
                "worker_started_at": self.started_at,
                "sampled_at_monotonic_seconds": time.monotonic(),
                "sequence": self.sequence,
                "pid": os.getpid(),
                "target_id": self.target_id,
                "query_consecutive_failures": self.failures,
                "query_recovery_pending": int(self.pending),
                "persistence_ready": self.persistence_ready,
                "query_last_success_seconds": self.success_at,
                "query_last_failure_seconds": self.failure_at,
                "fallback_confirmed": int(
                    self.pending and self.fallback_at >= self.failure_at and self.fallback_at > 0
                ),
                "query_counts": dict(self.query_counts),
                "scheduling_counts": dict(self.decisions),
            }


def initialize_worker(index: int) -> None:
    global _state  # One observation object belongs to this OS worker process.
    target_id = os.environ.get("MOTOR_RUNTIME_TARGET_ID", "")
    directory = os.environ.get("MOTOR_RUNTIME_STATE_DIR", "")
    if target_id and directory:
        try:
            _state = WorkerObservation(target_id, index, Path(directory))
        except ValueError:
            _state = None


def record_query(outcome: str) -> None:
    QUERY_REASON.set("query_failure" if outcome == "failure" else outcome)
    if _state is not None:
        _state.query(outcome)


def record_decision(policy: str) -> None:
    if _state is not None:
        _state.decision(policy, QUERY_REASON.get())


def worker_snapshot() -> dict[str, Any] | None:
    return _state.snapshot() if _state is not None else None


class RuntimeObservations:
    """Management-owned bounded slots; stale slots remain explicitly incomplete."""

    def __init__(self, workers: int) -> None:
        self.workers = max(1, workers)
        self.boot = uuid.uuid4().hex
        self.rows: dict[int, tuple[float, dict[str, Any]]] = {}

    def receive(self, value: dict[str, Any]) -> None:
        index = value.get("worker_index")
        if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < min(256, self.workers):
            raise ValueError("invalid runtime worker slot")
        if len(json.dumps(value)) > 4096 or value.get("target_id") != os.environ.get("MOTOR_RUNTIME_TARGET_ID"):
            raise ValueError("invalid runtime worker identity")
        if not re.fullmatch(r"[0-9a-f]{32}", str(value.get("worker_start_id", ""))):
            raise ValueError("invalid runtime worker incarnation")
        for key in (
            "query_consecutive_failures",
            "query_recovery_pending",
            "query_last_success_seconds",
            "query_last_failure_seconds",
            "fallback_confirmed",
            "worker_started_at",
            "sequence",
            "sampled_at_monotonic_seconds",
        ):
            number = value.get(key)
            if type(number) not in (int, float) or not math.isfinite(number) or number < 0:
                raise ValueError("invalid runtime counter")
        sampled_at = value["sampled_at_monotonic_seconds"]
        if sampled_at > time.monotonic():
            raise ValueError("invalid runtime source time")
        old = self.rows.get(index)
        if old:
            previous = old[1]
            same = previous["worker_start_id"] == value["worker_start_id"]
            if (same and (value["sequence"] <= previous["sequence"] or sampled_at < old[0])) or (
                not same and value["worker_started_at"] <= previous["worker_started_at"]
            ):
                raise ValueError("out of order runtime observation")
            if previous["query_recovery_pending"] and not value["query_last_success_seconds"]:
                value = value | {"query_recovery_pending": 1}
        self.rows[index] = (sampled_at, dict(value))

    def snapshot(self, ready: bool) -> dict[str, Any]:
        now = time.monotonic()
        rows = [value | {"observation_age_seconds": now - sampled} for sampled, value in self.rows.values()]
        current = [row for row in rows if row["observation_age_seconds"] <= 15]
        complete = len(current) == self.workers and all(row.get("persistence_ready") for row in current)
        # Losing a worker sample does not recover a confirmed fault. Keep that
        # evidence while reporting incomplete coverage; a new worker reads the
        # durable latch and must actually query successfully to clear it.
        failed = [row for row in rows if row["query_recovery_pending"] or row["query_consecutive_failures"] >= 3]
        return {
            "schema": "motor-runtime-v1",
            "role": "coordinator",
            "target_id": os.environ.get("MOTOR_RUNTIME_TARGET_ID", ""),
            "service_start_id": self.boot,
            "ready": ready,
            "coverage_complete": complete,
            "expected_processes": self.workers,
            "observed_processes": len(current),
            "observation_age_seconds": 0,
            "query_consecutive_failures": max(
                (row["query_consecutive_failures"] for row in current + failed), default=0
            ),
            "query_recovery_pending": int(bool(failed)),
            "query_circuit_open": int(any(row.get("query_circuit_open") for row in current)),
            "query_last_success_seconds": min((row["query_last_success_seconds"] for row in current), default=0),
            "query_last_failure_seconds": max((row["query_last_failure_seconds"] for row in current), default=0),
            "fallback_confirmed": int(
                bool(failed)
                and all(row["fallback_confirmed"] and row["observation_age_seconds"] <= 15 for row in failed)
            ),
            "processes": rows,
            "fault": False,
        }

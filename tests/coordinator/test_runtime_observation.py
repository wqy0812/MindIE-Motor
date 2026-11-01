# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
# MindIE is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import json
import asyncio
from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest

from motor.coordinator import runtime_observation
from motor.coordinator.runtime_observation import RuntimeObservations, WorkerObservation


def test_single_failure_does_not_latch_but_restart_cannot_clear_confirmed_fault(tmp_path):
    tmp_path.chmod(0o700)
    first = WorkerObservation("router-coordinator", 0, tmp_path)
    first.query("failure")
    assert first.snapshot()["query_consecutive_failures"] == 1
    assert not first.snapshot()["query_recovery_pending"]
    first.query("circuit_open")
    assert first.snapshot()["query_consecutive_failures"] == 1
    first.query("failure")
    first.query("failure")
    assert first.snapshot()["query_recovery_pending"] == 1
    first.decision("load_balance", "query_failure")
    assert first.snapshot()["fallback_confirmed"] == 1
    replacement = WorkerObservation("router-coordinator", 0, tmp_path)
    assert replacement.snapshot()["worker_start_id"] != first.snapshot()["worker_start_id"]
    assert replacement.snapshot()["query_consecutive_failures"] == 0
    assert replacement.snapshot()["query_recovery_pending"] == 1
    replacement.query("success")
    assert not replacement.snapshot()["query_recovery_pending"]
    assert not WorkerObservation("router-coordinator", 0, tmp_path).snapshot()["query_recovery_pending"]


def test_multiworker_coverage_and_failures_are_not_added(monkeypatch, tmp_path):
    monkeypatch.setenv("MOTOR_RUNTIME_TARGET_ID", "coordinator")
    tmp_path.chmod(0o700)
    collector = RuntimeObservations(2)
    workers = [WorkerObservation("coordinator", index, tmp_path) for index in range(2)]
    for worker in workers:
        worker.query("failure")
        collector.receive(worker.snapshot())
    snapshot = collector.snapshot(True)
    assert snapshot["coverage_complete"]
    assert snapshot["query_consecutive_failures"] == 1
    workers[0].query("failure")
    workers[0].query("failure")
    workers[1].query("success")
    for worker in workers:
        collector.receive(worker.snapshot())
    assert collector.snapshot(True)["query_recovery_pending"] == 1
    stamp, value = collector.rows[0]
    collector.rows[0] = (stamp - 20, value)
    assert not collector.snapshot(True)["coverage_complete"]
    assert collector.snapshot(True)["query_recovery_pending"] == 1
    assert collector.snapshot(True)["fallback_confirmed"] == 0


@pytest.mark.parametrize('text', ['invalid-json', 'null', '[]', 'true', '1'])
def test_corrupt_recovery_state_is_unknown_until_real_success(tmp_path, text):
    tmp_path.chmod(0o700)
    worker = WorkerObservation("coordinator", 0, tmp_path)
    worker.path.write_text(text)
    worker = WorkerObservation("coordinator", 0, tmp_path)
    assert not worker.snapshot()["persistence_ready"]
    worker.query("failure")
    assert not worker.snapshot()["persistence_ready"]
    worker.query("success")
    assert worker.snapshot()["persistence_ready"]
    assert json.loads(worker.path.read_text())["query_fault"] is False


def test_worker_source_age_includes_ipc_delay_without_losing_fault_or_recovery(monkeypatch, tmp_path):
    monkeypatch.setenv("MOTOR_RUNTIME_TARGET_ID", "coordinator")
    tmp_path.chmod(0o700)
    clock = [1000.0]
    monkeypatch.setattr(runtime_observation.time, "monotonic", lambda: clock[0])
    worker = WorkerObservation("coordinator", 0, tmp_path)
    collector = RuntimeObservations(1)
    payload = worker.snapshot()
    clock[0] += 2
    collector.receive(payload)
    assert collector.snapshot(True)["coverage_complete"]
    clock[0] += 14
    assert not collector.snapshot(True)["coverage_complete"]
    assert collector.snapshot(True)["processes"][0]["observation_age_seconds"] == 16
    for _ in range(3):
        worker.query("failure")
    worker.decision("load_balance", "query_failure")
    payload = worker.snapshot()
    clock[0] += 20
    collector.receive(payload)
    stale = collector.snapshot(True)
    assert not stale["coverage_complete"] and stale["query_recovery_pending"] == 1
    assert stale["processes"][0]["observation_age_seconds"] == 20
    assert stale["fallback_confirmed"] == 0
    worker.query("success")
    collector.receive(worker.snapshot())
    recovered = collector.snapshot(True)
    assert recovered["coverage_complete"] and recovered["query_recovery_pending"] == 0


def test_observation_rejects_unknown_slots_and_unbounded_payload(monkeypatch, tmp_path):
    monkeypatch.setenv("MOTOR_RUNTIME_TARGET_ID", "coordinator")
    monkeypatch.setattr(runtime_observation.time, "monotonic", lambda: 1000.0)
    tmp_path.chmod(0o700)
    collector = RuntimeObservations(1)
    worker = WorkerObservation("coordinator", 0, tmp_path)
    with pytest.raises(ValueError, match="slot"):
        collector.receive(worker.snapshot() | {"worker_index": 1})
    with pytest.raises(ValueError, match="identity"):
        collector.receive(worker.snapshot() | {"extra": "x" * 5000})
    payload = worker.snapshot()
    for stamp in (None, float("nan"), 1001.0):
        with pytest.raises(ValueError):
            collector.receive(payload | {"sampled_at_monotonic_seconds": stamp})
    collector.receive(payload)
    with pytest.raises(ValueError, match="out of order"):
        collector.receive(worker.snapshot() | {"sampled_at_monotonic_seconds": 999.0})


@pytest.mark.asyncio
async def test_disconnect_repeated_cancel_still_reaps_observer_and_transport():
    from motor.coordinator.scheduler.runtime.scheduler_client import AsyncSchedulerClient

    client = AsyncSchedulerClient.__new__(AsyncSchedulerClient)
    worker = asyncio.create_task(asyncio.sleep(60))
    client._runtime_observation_task = worker
    entered, finish = asyncio.Event(), asyncio.Event()

    async def stop_stats():
        entered.set()
        await finish.wait()

    client._stop_dp_stats_task = stop_stats
    client._stop_cache_lifecycle_task = AsyncMock()
    client._kv_usage_reader = None
    client._push_subscriber = None
    client._workload_reader = None
    client._transport = SimpleNamespace(disconnect=AsyncMock())
    closing = asyncio.create_task(client.disconnect())
    await entered.wait()
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert worker.done()
    client._transport.disconnect.assert_awaited_once()
    await client.disconnect()
    assert client._transport.disconnect.await_count == 2

# [2026-11-02] Runtime observation keeps recovery errors and source age within its own boundary

- **Symptom**: A recovery file containing `null`, an array, a boolean or a number raised `AttributeError` during worker initialization. Delayed IPC snapshots could report complete coverage after their source observation was already older than 15 seconds.
- **Root cause**: `WorkerObservation` accessed parsed JSON as a mapping before validating its type. `RuntimeObservations.receive` recorded management reception time rather than worker sampling time.
- **Why**: Invalid JSON was covered, but structurally invalid JSON was omitted. RPC timeout and recent delivery were incorrectly treated as evidence of a recent source observation.
- **Fix**: Validate the recovery object before accessing fields. Include `sampled_at_monotonic_seconds` in local IPC, validate source time and ordering, and retain it as the age origin. An expired fault remains evidence with incomplete coverage; a real successful query remains the recovery condition.
- **Test interception**: `tests/coordinator/test_runtime_observation.py` covers malformed recovery types, successful state repair, IPC delay, expiry, retained fault and actual recovery, as well as missing, invalid, future and out-of-order source timestamps. Run it through `bash tests/run_tests.sh --serial`.
- **Scenario**: Enabling native runtime observation during worker restart, or processing a delayed worker snapshot after management IPC stalls. This does not add synthetic business queries or change scheduling decisions.
- **Keywords**: runtime observation, recovery JSON, monotonic source time, IPC delay, coverage.

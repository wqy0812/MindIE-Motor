// Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
// MindIE is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! Constant-size runtime summary. Never traverses registry/index/token state.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{http::StatusCode, response::Response, Json};
use serde_json::{json, Value};

pub struct RuntimeObservation {
    start_id: String,
    success: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
    last_success: AtomicU64,
    last_failure: AtomicU64,
}

impl RuntimeObservation {
    fn new() -> Self {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            start_id: format!("{}-{}", std::process::id(), epoch.as_nanos()),
            success: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last_success: AtomicU64::new(0),
            last_failure: AtomicU64::new(0),
        }
    }

    fn observe(&self, status: StatusCode) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if status.is_success() {
            self.success.fetch_add(1, Ordering::Relaxed);
            self.last_success.store(now, Ordering::Relaxed);
        } else {
            if status.is_client_error() {
                self.rejected.fetch_add(1, Ordering::Relaxed);
            } else {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
            self.last_failure.store(now, Ordering::Relaxed);
        }
    }

    fn snapshot(&self, target: &str) -> Value {
        json!({
            "schema": "motor-runtime-v1", "role": "conductor", "target_id": target,
            "service_start_id": self.start_id, "ready": true, "coverage_complete": true,
            "observation_age_seconds": 0, "fault": false,
            "query_counts": {
                "success": self.success.load(Ordering::Relaxed),
                "rejected": self.rejected.load(Ordering::Relaxed),
                "failed": self.failed.load(Ordering::Relaxed),
            },
            "query_last_success_seconds": self.last_success.load(Ordering::Relaxed),
            "query_last_failure_seconds": self.last_failure.load(Ordering::Relaxed),
            "optional_diagnostics": {"registration": "unsupported", "processing": "unsupported"}
        })
    }
}

static OBSERVATION: OnceLock<RuntimeObservation> = OnceLock::new();

pub fn initialize() {
    OBSERVATION.get_or_init(RuntimeObservation::new);
}

pub fn observe_response(response: Response) -> Response {
    OBSERVATION
        .get_or_init(RuntimeObservation::new)
        .observe(response.status());
    response
}

pub async fn handler() -> Result<Json<Value>, StatusCode> {
    let target = std::env::var("MOTOR_RUNTIME_TARGET_ID").unwrap_or_default();
    if target.is_empty()
        || target.len() > 128
        || !target
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_:-".contains(&c))
    {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(
        OBSERVATION
            .get_or_init(RuntimeObservation::new)
            .snapshot(&target),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_results_without_scanning_or_exposing_request_content() {
        let state = RuntimeObservation::new();
        state.observe(StatusCode::OK);
        state.observe(StatusCode::BAD_REQUEST);
        state.observe(StatusCode::INTERNAL_SERVER_ERROR);
        let value = state.snapshot("conductor-test");
        assert_eq!(
            value["query_counts"],
            json!({"success": 1, "rejected": 1, "failed": 1})
        );
        assert_eq!(value["target_id"], "conductor-test");
        assert_eq!(value["ready"], true);
        assert!(serde_json::to_vec(&value).unwrap().len() < 4096);
        assert_ne!(state.start_id, RuntimeObservation::new().start_id);
    }
}

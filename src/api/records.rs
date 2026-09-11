use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use super::errors::ApiError;
use super::models::{CreateRecordRequest, PatchRecordRequest, PutRecordRequest};
use super::router::AppState;
use crate::db::Record;

const UNIQUE_VIOLATION: &str = "23505";

fn validate_id(id: &str) -> Result<(), ApiError> {
    if id.trim().is_empty() {
        return Err(ApiError::BadRequest("id must not be empty".into()));
    }
    if id.len() > 255 {
        return Err(ApiError::BadRequest("id must be <= 255 chars".into()));
    }
    Ok(())
}

async fn fill_cache(state: &AppState, record: &Record) {
    if let Some(cache) = &state.cache {
        cache.insert(record).await;
    }
}

async fn invalidate_cache(state: &AppState, id: &str) {
    if let Some(cache) = &state.cache {
        cache.invalidate(id).await;
    }
}

async fn broadcast(state: &AppState, id: &str) {
    if let Some(channel) = &state.invalidation_channel {
        crate::cache::publish_invalidation(&state.redis, channel, id).await;
    }
}

pub async fn create_record(
    State(state): State<AppState>,
    Json(body): Json<CreateRecordRequest>,
) -> Result<(StatusCode, Json<Record>), ApiError> {
    validate_id(&body.id)?;
    let CreateRecordRequest { id, payload } = body;
    let rec = sqlx::query_as::<_, Record>(
        "INSERT INTO records (id, payload) VALUES ($1, $2) RETURNING id, payload, version, created_at, updated_at",
    )
    .bind(&id)
    .bind(payload)
    .fetch_one(&state.pg)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some(UNIQUE_VIOLATION) => {
            ApiError::Conflict(format!("Record with ID '{id}' already exists"))
        }
        _ => ApiError::from(e),
    })?;
    fill_cache(&state, &rec).await;
    broadcast(&state, &rec.id).await;
    Ok((StatusCode::CREATED, Json(rec)))
}

async fn fetch_pg(state: &AppState, id: &str) -> Result<Record, ApiError> {
    let rec = sqlx::query_as::<_, Record>(
        "SELECT id, payload, version, created_at, updated_at FROM records WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pg)
    .await?;
    let Some(rec) = rec else {
        return Err(ApiError::NotFound(format!("Record '{id}' does not exist")));
    };
    fill_cache(state, &rec).await;
    Ok(rec)
}

pub async fn get_record(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Record>, ApiError> {
    if let Some(cache) = &state.cache
        && let Some(rec) = cache.get(&id).await
    {
        return Ok(Json(rec));
    }
    if let Some(coalescer) = &state.coalescer {
        // move owned copies in: the leader's DB work runs on a detached task
        // that may outlive this request, so the closure must be 'static.
        let st = state.clone();
        let key = id.clone();
        return coalescer
            .execute(&id, move || {
                let st = st.clone();
                let key = key.clone();
                async move { fetch_pg(&st, &key).await }
            })
            .await
            .map(Json);
    }
    fetch_pg(&state, &id).await.map(Json)
}

pub async fn patch_record(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<PatchRecordRequest>,
) -> Result<Json<Record>, ApiError> {
    let PatchRecordRequest {
        payload,
        expected_version,
    } = body;
    if let Some(rec) = sqlx::query_as::<_, Record>(
        "UPDATE records SET payload = $1, version = version + 1 WHERE id = $2 AND version = $3 RETURNING id, payload, version, created_at, updated_at",
    )
    .bind(payload)
    .bind(&id)
    .bind(expected_version)
    .fetch_optional(&state.pg)
    .await?
    {
        fill_cache(&state, &rec).await;
        broadcast(&state, &rec.id).await;
        return Ok(Json(rec));
    }
    let current = sqlx::query_scalar::<_, i32>("SELECT version FROM records WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.pg)
        .await?;
    Err(match current {
        None => ApiError::NotFound(format!("Record '{id}' does not exist")),
        Some(v) => ApiError::VersionMismatch(format!(
            "Current version is {v}, but expected version was {expected_version}"
        )),
    })
}

pub async fn put_record(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<PutRecordRequest>,
) -> Result<(StatusCode, Json<Record>), ApiError> {
    validate_id(&id)?;
    let rec = sqlx::query_as::<_, Record>(
        "INSERT INTO records (id, payload) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, version = records.version + 1 RETURNING id, payload, version, created_at, updated_at",
    )
    .bind(&id)
    .bind(body.payload)
    .fetch_one(&state.pg)
    .await?;
    fill_cache(&state, &rec).await;
    broadcast(&state, &rec.id).await;
    let status = if rec.version == 1 {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(rec)))
}

pub async fn delete_record(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let res = sqlx::query("DELETE FROM records WHERE id = $1")
        .bind(&id)
        .execute(&state.pg)
        .await?;
    if res.rows_affected() == 0 {
        return Err(ApiError::NotFound(format!("Record '{id}' does not exist")));
    }
    invalidate_cache(&state, &id).await;
    broadcast(&state, &id).await;
    Ok(StatusCode::NO_CONTENT)
}

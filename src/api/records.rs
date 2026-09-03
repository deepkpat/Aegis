use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use sqlx::PgPool;

use super::errors::ApiError;
use super::models::{CreateRecordRequest, PatchRecordRequest};
use crate::db::Record;

fn validate_id(id: &str) -> Result<(), ApiError> {
    if id.trim().is_empty() {
        return Err(ApiError::BadRequest("id must not be empty".into()));
    }
    if id.len() > 255 {
        return Err(ApiError::BadRequest("id must be <= 255 chars".into()));
    }
    Ok(())
}

pub async fn create_record(
    State(pool): State<PgPool>,
    Json(body): Json<CreateRecordRequest>,
) -> Result<(StatusCode, Json<Record>), ApiError> {
    validate_id(&body.id)?;
    let CreateRecordRequest { id, payload } = body;
    let rec = sqlx::query_as::<_, Record>(
        "INSERT INTO records (id, payload) VALUES ($1, $2) RETURNING id, payload, version, created_at, updated_at",
    )
    .bind(id.clone())
    .bind(payload)
    .fetch_one(&pool)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => {
            ApiError::Conflict(format!("Record with ID '{id}' already exists"))
        }
        _ => ApiError::from(e),
    })?;
    Ok((StatusCode::CREATED, Json(rec)))
}

pub async fn get_record(
    State(pool): State<PgPool>,
    Path(id): Path<String>,
) -> Result<Json<Record>, ApiError> {
    let rec = sqlx::query_as::<_, Record>(
        "SELECT id, payload, version, created_at, updated_at FROM records WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&pool)
    .await?;
    rec.map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("Record '{id}' does not exist")))
}

pub async fn patch_record(
    State(pool): State<PgPool>,
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
    .fetch_optional(&pool)
    .await?
    {
        return Ok(Json(rec));
    }
    let current: Option<i32> = sqlx::query_scalar("SELECT version FROM records WHERE id = $1")
        .bind(&id)
        .fetch_optional(&pool)
        .await?;
    match current {
        None => Err(ApiError::NotFound(format!("Record '{id}' does not exist"))),
        Some(v) => Err(ApiError::VersionMismatch(format!(
            "Current version is {v}, but expected version was {expected_version}"
        ))),
    }
}

pub async fn delete_record(
    State(pool): State<PgPool>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let res = sqlx::query("DELETE FROM records WHERE id = $1")
        .bind(&id)
        .execute(&pool)
        .await?;
    if res.rows_affected() == 0 {
        return Err(ApiError::NotFound(format!("Record '{id}' does not exist")));
    }
    Ok(StatusCode::NO_CONTENT)
}

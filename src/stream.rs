//! JetStream stream provisioning and verification (the `nats` feature).
//!
//! The crate-level invariant (see [`crate::outbox::min_duplicate_window`]): the
//! stream backing the `fiducia.*` taxonomy must have a `duplicate_window` of at
//! least `claim_ttl + MAX_PUBLISH_BACKOFF`, or a relay crash-window re-publish
//! is **stored as a new message and delivered twice**. JetStream's out-of-the-box
//! dedup window is 2 minutes — silently below that bound — so leaving the stream
//! to ad-hoc creation is exactly the failure mode this module closes.
//!
//! [`ensure_stream`] runs at relay startup, before the first publish. It
//! creates the stream with an explicit [`Config`] when it does not exist, and
//! when it *does* exist it verifies the deployed `duplicate_window` against the
//! invariant and **fails closed** — a relay that cannot prove broker-side dedup
//! refuses to publish rather than double-deliver.

use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::stream::{Config, RetentionPolicy, StorageType, Stream};

use crate::error::MessagingError;
use crate::outbox::min_duplicate_window;

/// The stream that stores the whole `fiducia.*` subject taxonomy.
pub const STREAM_NAME: &str = "FIDUCIA_MESSAGES";

/// The stream's subject filter: every canonical `fiducia.<group>.<event>.v<n>`
/// routing class (identifiers never appear in subjects, so the space is small).
pub const STREAM_SUBJECTS: &str = "fiducia.>";

/// `FIDUCIA_STREAM_REPLICAS` — JetStream replica count (1–5), default 1.
pub const REPLICAS_ENV: &str = "FIDUCIA_STREAM_REPLICAS";

/// `FIDUCIA_STREAM_MAX_AGE_HOURS` — limits-retention message age; `0` keeps
/// messages until byte/count limits apply. Default [`DEFAULT_MAX_AGE`].
pub const MAX_AGE_ENV: &str = "FIDUCIA_STREAM_MAX_AGE_HOURS";

/// Default stream retention age: 7 days. Long enough for any consumer catch-up
/// or replay window we operate with; the outbox/inbox tables remain the durable
/// record beyond it.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

/// The explicit stream config this deployment requires: file storage,
/// limits-based retention, and — the invariant — a `duplicate_window` of
/// [`min_duplicate_window`]`(claim_ttl)` so the broker collapses the relay's
/// worst-case crash-window re-publish.
pub fn desired_config(claim_ttl: Duration, num_replicas: usize, max_age: Duration) -> Config {
    Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![STREAM_SUBJECTS.to_string()],
        storage: StorageType::File,
        retention: RetentionPolicy::Limits,
        num_replicas,
        max_age,
        duplicate_window: min_duplicate_window(claim_ttl),
        ..Config::default()
    }
}

/// Parse [`REPLICAS_ENV`]. Unset means 1; set-but-invalid is a hard error — a
/// typo'd replica count must not silently under-replicate the stream.
pub fn parse_replicas(raw: Option<&str>) -> Result<usize, MessagingError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(1),
        Some(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=5).contains(n))
            .ok_or_else(|| {
                MessagingError::Config(format!(
                    "{REPLICAS_ENV} must be an integer in 1..=5, got {v:?}"
                ))
            }),
    }
}

/// Parse [`MAX_AGE_ENV`] (hours). Unset means [`DEFAULT_MAX_AGE`]; `0` means
/// unlimited age (limits still apply); set-but-invalid is a hard error.
pub fn parse_max_age(raw: Option<&str>) -> Result<Duration, MessagingError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(DEFAULT_MAX_AGE),
        Some(v) => v
            .parse::<u64>()
            .ok()
            .and_then(|hours| hours.checked_mul(3600))
            .map(Duration::from_secs)
            .ok_or_else(|| {
                MessagingError::Config(format!(
                    "{MAX_AGE_ENV} must be a non-negative integer number of hours, got {v:?}"
                ))
            }),
    }
}

/// Build [`desired_config`] from the environment ([`REPLICAS_ENV`],
/// [`MAX_AGE_ENV`]). Misconfiguration is a hard error, not a silent default.
pub fn config_from_env(claim_ttl: Duration) -> Result<Config, MessagingError> {
    let replicas = parse_replicas(std::env::var(REPLICAS_ENV).ok().as_deref())?;
    let max_age = parse_max_age(std::env::var(MAX_AGE_ENV).ok().as_deref())?;
    Ok(desired_config(claim_ttl, replicas, max_age))
}

/// Verify a deployed stream's `duplicate_window` against the invariant
/// `duplicate_window >= claim_ttl + MAX_PUBLISH_BACKOFF`
/// ([`min_duplicate_window`]). Pure, so the fail-closed decision is testable
/// without a broker.
pub fn verify_duplicate_window(
    actual: Duration,
    claim_ttl: Duration,
) -> Result<(), MessagingError> {
    let required = min_duplicate_window(claim_ttl);
    if actual < required {
        return Err(MessagingError::Config(format!(
            "stream '{STREAM_NAME}' has duplicate_window {}s, below the required minimum {}s \
             (claim_ttl {}s + MAX_PUBLISH_BACKOFF {}s): a relay crash/backoff re-publish would \
             be stored and DELIVERED TWICE. Raise the stream's duplicate_window; refusing to \
             publish (fail closed)",
            actual.as_secs(),
            required.as_secs(),
            claim_ttl.as_secs(),
            crate::outbox::MAX_PUBLISH_BACKOFF.as_secs(),
        )));
    }
    Ok(())
}

/// Ensure the `fiducia.*` stream exists and satisfies the dedup invariant.
///
/// Creates the stream with `config` when absent. When it already exists,
/// JetStream returns it **as deployed** (an existing stream's config is never
/// silently rewritten from here); the deployed `duplicate_window` is then
/// checked via [`verify_duplicate_window`] and a too-small window is a startup
/// error — the caller must not publish.
pub async fn ensure_stream(
    js: &jetstream::Context,
    config: Config,
    claim_ttl: Duration,
) -> Result<Stream, MessagingError> {
    let stream = js
        .get_or_create_stream(config)
        .await
        .map_err(MessagingError::transport)?;
    let info = stream.cached_info();
    verify_duplicate_window(info.config.duplicate_window, claim_ttl)?;
    tracing::info!(
        stream = STREAM_NAME,
        duplicate_window_secs = info.config.duplicate_window.as_secs(),
        num_replicas = info.config.num_replicas,
        "JetStream stream verified: duplicate_window covers claim_ttl + MAX_PUBLISH_BACKOFF"
    );
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{DEFAULT_CLAIM_TTL, MAX_PUBLISH_BACKOFF};

    #[test]
    fn desired_config_encodes_the_invariant_and_durable_storage() {
        let config = desired_config(DEFAULT_CLAIM_TTL, 3, DEFAULT_MAX_AGE);
        assert_eq!(config.name, STREAM_NAME);
        assert_eq!(config.subjects, vec![STREAM_SUBJECTS.to_string()]);
        assert_eq!(config.storage, StorageType::File);
        assert_eq!(config.retention, RetentionPolicy::Limits);
        assert_eq!(config.num_replicas, 3);
        assert_eq!(config.max_age, DEFAULT_MAX_AGE);
        // The invariant, with the default claim TTL: 300s + 300s = 600s.
        assert_eq!(
            config.duplicate_window,
            DEFAULT_CLAIM_TTL + MAX_PUBLISH_BACKOFF
        );
        assert_eq!(config.duplicate_window, Duration::from_secs(600));
    }

    #[test]
    fn replicas_default_and_reject_nonsense() {
        assert_eq!(parse_replicas(None).unwrap(), 1);
        assert_eq!(parse_replicas(Some("")).unwrap(), 1);
        assert_eq!(parse_replicas(Some("3")).unwrap(), 3);
        for bad in ["0", "6", "-1", "3x", "many"] {
            let err = parse_replicas(Some(bad)).unwrap_err();
            assert!(matches!(err, MessagingError::Config(_)), "accepted {bad:?}");
        }
    }

    #[test]
    fn max_age_default_zero_and_rejection() {
        assert_eq!(parse_max_age(None).unwrap(), DEFAULT_MAX_AGE);
        assert_eq!(parse_max_age(Some("0")).unwrap(), Duration::ZERO);
        assert_eq!(
            parse_max_age(Some("48")).unwrap(),
            Duration::from_secs(48 * 3600)
        );
        for bad in ["-1", "1.5", "week"] {
            let err = parse_max_age(Some(bad)).unwrap_err();
            assert!(matches!(err, MessagingError::Config(_)), "accepted {bad:?}");
        }
    }

    #[test]
    fn a_window_at_or_above_the_minimum_passes() {
        let min = DEFAULT_CLAIM_TTL + MAX_PUBLISH_BACKOFF;
        assert!(verify_duplicate_window(min, DEFAULT_CLAIM_TTL).is_ok());
        assert!(verify_duplicate_window(min + Duration::from_secs(1), DEFAULT_CLAIM_TTL).is_ok());
    }

    #[test]
    fn a_too_small_window_fails_closed_naming_the_invariant() {
        // JetStream's out-of-the-box window (2 minutes) is exactly the silent
        // double-delivery case this module exists to catch.
        let err = verify_duplicate_window(Duration::from_secs(120), DEFAULT_CLAIM_TTL).unwrap_err();
        let message = err.to_string();
        assert!(matches!(err, MessagingError::Config(_)));
        assert!(message.contains("duplicate_window"), "{message}");
        assert!(message.contains("MAX_PUBLISH_BACKOFF"), "{message}");
        assert!(message.contains("600"), "{message}");
        assert!(message.contains("fail closed"), "{message}");
    }
}

//! The subject taxonomy.
//!
//! Subjects are **routing classes**, composed as `fiducia.<group>.<event>.v<version>`.
//! The cardinal rule this module encodes: *identifiers go in the envelope, not
//! the subject.* A subject names a kind of message (so consumers can subscribe
//! to `fiducia.executions.*.v1`); it must never embed a work-item id, tenant id,
//! or execution id — those live in [`MessageEnvelope`](crate::MessageEnvelope).
//! Baking ids into subjects explodes the subject space, defeats wildcard
//! subscriptions, and couples routing to data.
//!
//! Use the [`Subject`] builder to compose subjects safely, or the `pub const`s
//! below for the canonical set.

use thiserror::Error;
use uuid::Uuid;

/// The fixed root token of every fiducia subject.
pub const ROOT: &str = "fiducia";

/// `fiducia.work-items.created.v1`
pub const WORK_ITEMS_CREATED: &str = "fiducia.work-items.created.v1";
/// `fiducia.executions.requested.v1`
pub const EXECUTIONS_REQUESTED: &str = "fiducia.executions.requested.v1";
/// `fiducia.executions.progress.v1`
pub const EXECUTIONS_PROGRESS: &str = "fiducia.executions.progress.v1";
/// `fiducia.executions.completed.v1`
pub const EXECUTIONS_COMPLETED: &str = "fiducia.executions.completed.v1";
/// `fiducia.reviews.requested.v1`
pub const REVIEWS_REQUESTED: &str = "fiducia.reviews.requested.v1";
/// `fiducia.reviews.findings.v1`
pub const REVIEWS_FINDINGS: &str = "fiducia.reviews.findings.v1";
/// `fiducia.tests.requested.v1`
pub const TESTS_REQUESTED: &str = "fiducia.tests.requested.v1";
/// `fiducia.tests.completed.v1`
pub const TESTS_COMPLETED: &str = "fiducia.tests.completed.v1";
/// `fiducia.runners.heartbeat.v1`
pub const RUNNERS_HEARTBEAT: &str = "fiducia.runners.heartbeat.v1";
/// `fiducia.runners.commands.v1`
pub const RUNNERS_COMMANDS: &str = "fiducia.runners.commands.v1";
/// `fiducia.github.events.v1`
pub const GITHUB_EVENTS: &str = "fiducia.github.events.v1";
/// `fiducia.jira.events.v1`
pub const JIRA_EVENTS: &str = "fiducia.jira.events.v1";

/// Why a subject token or string was rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubjectError {
    /// A token was empty.
    #[error("subject token must be non-empty")]
    Empty,
    /// A token contained something other than lowercase alphanumerics or `-`,
    /// or began/ended with `-`.
    #[error(
        "subject token {0:?} must be lowercase alphanumeric or hyphen (no leading/trailing '-')"
    )]
    InvalidChar(String),
    /// A token parses as a UUID — an identifier leaking into the routing class.
    #[error("subject token {0:?} looks like an identifier; identifiers belong in the envelope, not the subject")]
    IdentifierInSubject(String),
    /// Version was zero (versions start at 1).
    #[error("subject version must be >= 1")]
    ZeroVersion,
    /// A string was not a `fiducia.<group>.<event>.v<version>` subject.
    #[error("not a fiducia subject: {0:?}")]
    Malformed(String),
}

/// Validate a single subject token: non-empty, `[a-z0-9-]`, no leading/trailing
/// hyphen, and not an identifier (UUID).
pub fn validate_token(token: &str) -> Result<(), SubjectError> {
    if token.is_empty() {
        return Err(SubjectError::Empty);
    }
    let ok_chars = token
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if !ok_chars || token.starts_with('-') || token.ends_with('-') {
        return Err(SubjectError::InvalidChar(token.to_string()));
    }
    // Encode the rule: a token that parses as a UUID is an id, not a class.
    if Uuid::parse_str(token).is_ok() {
        return Err(SubjectError::IdentifierInSubject(token.to_string()));
    }
    Ok(())
}

/// A validated `fiducia.<group>.<event>.v<version>` subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
    group: String,
    event: String,
    version: u32,
}

impl Subject {
    /// Compose and validate a subject from its parts.
    pub fn new(
        group: impl Into<String>,
        event: impl Into<String>,
        version: u32,
    ) -> Result<Self, SubjectError> {
        let group = group.into();
        let event = event.into();
        validate_token(&group)?;
        validate_token(&event)?;
        if version == 0 {
            return Err(SubjectError::ZeroVersion);
        }
        Ok(Subject {
            group,
            event,
            version,
        })
    }

    /// The `<group>` token, e.g. `executions`.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The `<event>` token, e.g. `completed`.
    pub fn event(&self) -> &str {
        &self.event
    }

    /// The schema/subject version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Render as `fiducia.<group>.<event>.v<version>`.
    pub fn as_string(&self) -> String {
        format!("{ROOT}.{}.{}.v{}", self.group, self.event, self.version)
    }

    /// A wildcard that matches every event in this group+version, e.g.
    /// `fiducia.executions.*.v1` — the routing class a consumer subscribes to.
    pub fn group_wildcard(group: &str, version: u32) -> Result<String, SubjectError> {
        validate_token(group)?;
        if version == 0 {
            return Err(SubjectError::ZeroVersion);
        }
        Ok(format!("{ROOT}.{group}.*.v{version}"))
    }

    /// Parse a `fiducia.<group>.<event>.v<version>` subject back into parts.
    pub fn parse(subject: &str) -> Result<Self, SubjectError> {
        let parts: Vec<&str> = subject.split('.').collect();
        // fiducia . group . event . vN
        if parts.len() != 4 || parts[0] != ROOT {
            return Err(SubjectError::Malformed(subject.to_string()));
        }
        let version = parts[3]
            .strip_prefix('v')
            .and_then(|v| v.parse::<u32>().ok())
            .ok_or_else(|| SubjectError::Malformed(subject.to_string()))?;
        // Canonical spelling only: `u32::parse` also accepts "v01"/"v+1", which
        // render back as "v1" — accepting them would split one routing class
        // into aliases that exact-match and `*.v1`-wildcard subscribers never
        // receive. A subject that does not round-trip byte-for-byte is not a
        // fiducia subject.
        if parts[3] != format!("v{version}") {
            return Err(SubjectError::Malformed(subject.to_string()));
        }
        Subject::new(parts[1], parts[2], version)
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composes_expected_string() {
        let s = Subject::new("executions", "completed", 1).unwrap();
        assert_eq!(s.as_string(), "fiducia.executions.completed.v1");
        assert_eq!(s.to_string(), "fiducia.executions.completed.v1");
        assert_eq!(s.group(), "executions");
        assert_eq!(s.event(), "completed");
        assert_eq!(s.version(), 1);
    }

    #[test]
    fn constants_match_the_builder() {
        assert_eq!(
            Subject::new("work-items", "created", 1)
                .unwrap()
                .as_string(),
            WORK_ITEMS_CREATED
        );
        assert_eq!(
            Subject::new("executions", "progress", 1)
                .unwrap()
                .as_string(),
            EXECUTIONS_PROGRESS
        );
        assert_eq!(
            Subject::new("github", "events", 1).unwrap().as_string(),
            GITHUB_EVENTS
        );
        assert_eq!(
            Subject::new("jira", "events", 1).unwrap().as_string(),
            JIRA_EVENTS
        );
    }

    #[test]
    fn rejects_bad_tokens() {
        assert_eq!(validate_token(""), Err(SubjectError::Empty));
        // uppercase
        assert!(matches!(
            Subject::new("Executions", "completed", 1),
            Err(SubjectError::InvalidChar(_))
        ));
        // dot inside a token (would forge extra subject levels)
        assert!(matches!(
            Subject::new("execu.tions", "completed", 1),
            Err(SubjectError::InvalidChar(_))
        ));
        // NATS wildcards must not be smuggled into a token
        assert!(matches!(
            Subject::new("executions", "*", 1),
            Err(SubjectError::InvalidChar(_))
        ));
        assert!(matches!(
            Subject::new("executions", ">", 1),
            Err(SubjectError::InvalidChar(_))
        ));
        // leading/trailing hyphen
        assert!(matches!(
            Subject::new("-executions", "completed", 1),
            Err(SubjectError::InvalidChar(_))
        ));
        // zero version
        assert_eq!(
            Subject::new("executions", "completed", 0),
            Err(SubjectError::ZeroVersion)
        );
    }

    #[test]
    fn rejects_identifier_tokens() {
        // A UUID in a subject means an id leaked out of the envelope.
        let uuid = "11111111-1111-4111-8111-111111111111";
        assert!(matches!(
            Subject::new("executions", uuid, 1),
            Err(SubjectError::IdentifierInSubject(_))
        ));
    }

    #[test]
    fn parse_round_trips() {
        let s = Subject::new("reviews", "findings", 2).unwrap();
        let parsed = Subject::parse(&s.as_string()).unwrap();
        assert_eq!(s, parsed);

        assert!(matches!(
            Subject::parse("fiducia.reviews.findings"),
            Err(SubjectError::Malformed(_))
        ));
        assert!(matches!(
            Subject::parse("other.reviews.findings.v1"),
            Err(SubjectError::Malformed(_))
        ));
        assert!(matches!(
            Subject::parse("fiducia.reviews.findings.x1"),
            Err(SubjectError::Malformed(_))
        ));
    }

    /// `u32::parse` alone would accept alias spellings of a version. Any
    /// subject that does not round-trip byte-for-byte must be rejected, or one
    /// routing class silently forks into variants no subscriber receives.
    #[test]
    fn parse_rejects_noncanonical_version_spellings() {
        for alias in [
            "fiducia.reviews.findings.v01",
            "fiducia.reviews.findings.v+1",
            "fiducia.reviews.findings.v0x1",
            "fiducia.reviews.findings.v 1",
        ] {
            assert!(
                matches!(Subject::parse(alias), Err(SubjectError::Malformed(_))),
                "accepted non-canonical subject {alias:?}"
            );
        }
        // Multi-digit canonical versions still parse.
        let v10 = Subject::parse("fiducia.reviews.findings.v10").unwrap();
        assert_eq!(v10.version(), 10);
        assert_eq!(v10.as_string(), "fiducia.reviews.findings.v10");
    }

    #[test]
    fn group_wildcard_builds() {
        assert_eq!(
            Subject::group_wildcard("executions", 1).unwrap(),
            "fiducia.executions.*.v1"
        );
    }
}

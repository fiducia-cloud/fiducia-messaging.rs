//! Hardened NATS connection policy.
//!
//! Every binary in this crate (and any embedding service) reaches NATS through
//! [`connect`], which decides — **before** dialing — whether the connection must
//! present TLS. The rule: a non-loopback endpoint gets `require_tls(true)`;
//! loopback (`localhost` / `127.0.0.0/8` / `::1`) may stay plaintext because the
//! traffic never leaves the host. Two environment switches adjust the policy:
//!
//! * [`REQUIRE_TLS_ENV`] (`FIDUCIA_NATS_REQUIRE_TLS=1`) — force TLS even for
//!   loopback (e.g. a local sidecar terminating mTLS).
//! * [`ALLOW_PLAINTEXT_ENV`] (`FIDUCIA_NATS_ALLOW_PLAINTEXT=1`) — explicit
//!   opt-out for a non-loopback endpoint; the helper connects but logs a loud
//!   warning, so plaintext across a network is always a deliberate, visible act.
//!
//! Credentials ride the environment, not the URL: [`CREDS_FILE_ENV`]
//! (`NATS_CREDS_FILE`) names an nkey/JWT `.creds` file loaded via
//! `ConnectOptions::with_credentials_file`, so `NATS_URL` never has to embed a
//! secret. Nothing here logs the URL or credentials (see the redaction
//! discipline in the crate README).
//!
//! The policy decision itself ([`decide_tls_policy`] and the host classifiers)
//! is pure and lives in the default offline build so it is unit-tested without
//! a NATS client; only the dialing [`connect`] function needs the `nats`
//! feature.

use std::net::IpAddr;

/// `FIDUCIA_NATS_REQUIRE_TLS=1` — enforce TLS even for loopback endpoints.
pub const REQUIRE_TLS_ENV: &str = "FIDUCIA_NATS_REQUIRE_TLS";

/// `FIDUCIA_NATS_ALLOW_PLAINTEXT=1` — explicit opt-out: allow plaintext to a
/// non-loopback endpoint (logged loudly). Ignored when [`REQUIRE_TLS_ENV`] is
/// also set; requiring wins.
pub const ALLOW_PLAINTEXT_ENV: &str = "FIDUCIA_NATS_ALLOW_PLAINTEXT";

/// `NATS_CREDS_FILE` — path to an nkey/JWT `.creds` file, so credentials never
/// ride the `NATS_URL`.
pub const CREDS_FILE_ENV: &str = "NATS_CREDS_FILE";

/// The TLS decision for one connection attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPolicy {
    /// `require_tls(true)`: every server in the URL list must offer TLS.
    Required,
    /// Every host is loopback; plaintext is acceptable (traffic stays on-host).
    LoopbackPlaintext,
    /// Non-loopback plaintext, explicitly opted into via
    /// [`ALLOW_PLAINTEXT_ENV`]. The caller must log a loud warning.
    PlaintextOptedOut,
}

/// Whether an env-switch value means "on". Accepts `1` and `true` (any case);
/// everything else — including empty — is off, so a stray `=0`/`=false` never
/// flips a security policy.
pub fn env_flag(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim),
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true")
    )
}

/// Extract the host from one NATS server URL: strip the scheme (`nats://`,
/// `tls://`, …), any userinfo (`user:pass@`), the port, and IPv6 brackets.
/// Returns an empty string for something host-less; callers must treat that as
/// **not** loopback so an unparseable URL fails toward requiring TLS.
pub fn host_of(url: &str) -> &str {
    let rest = match url.find("://") {
        Some(at) => &url[at + 3..],
        None => url,
    };
    // Userinfo cannot contain '@' unencoded, so the host starts after the last one.
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    // Trailing path/query (rare in NATS URLs, but never part of the host).
    let rest = rest.split(['/', '?']).next().unwrap_or(rest);
    if let Some(inner) = rest.strip_prefix('[') {
        // Bracketed IPv6: the host is what the brackets enclose.
        return inner.split(']').next().unwrap_or("");
    }
    // A bare IPv6 literal has multiple ':' and no port to strip.
    if rest.parse::<IpAddr>().is_ok() {
        return rest;
    }
    rest.rsplit_once(':')
        .filter(|(_, port)| !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
        .map_or(rest, |(host, _)| host)
}

/// Whether a single host is loopback: `localhost` (case-insensitive) or an IP
/// literal whose `IpAddr::is_loopback` holds (`127.0.0.0/8`, `::1`).
pub fn is_loopback_host(host: &str) -> bool {
    !host.is_empty()
        && (host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false))
}

/// Whether *every* server in a (possibly comma-separated) NATS URL list is
/// loopback. One non-loopback entry — or an entry whose host cannot be
/// determined — makes the whole list non-loopback: mixed lists take the
/// stricter policy.
pub fn all_hosts_loopback(nats_url: &str) -> bool {
    let mut any = false;
    for server in nats_url.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !is_loopback_host(host_of(server)) {
            return false;
        }
        any = true;
    }
    // An empty URL has no loopback host to trust; require TLS.
    any
}

/// The pure policy decision: given the URL and the two env switches, what does
/// this connection require? `require_tls` (the [`REQUIRE_TLS_ENV`] switch)
/// dominates; the plaintext opt-out only applies where TLS would otherwise be
/// enforced.
pub fn decide_tls_policy(nats_url: &str, require_tls: bool, allow_plaintext: bool) -> TlsPolicy {
    if require_tls {
        return TlsPolicy::Required;
    }
    if all_hosts_loopback(nats_url) {
        return TlsPolicy::LoopbackPlaintext;
    }
    if allow_plaintext {
        return TlsPolicy::PlaintextOptedOut;
    }
    TlsPolicy::Required
}

#[cfg(feature = "nats")]
pub use nats_impl::connect;

#[cfg(feature = "nats")]
mod nats_impl {
    use super::*;
    use crate::error::MessagingError;

    /// Connect to NATS under the crate's TLS policy (see the module docs):
    /// non-loopback endpoints get `require_tls(true)` unless
    /// `FIDUCIA_NATS_ALLOW_PLAINTEXT=1` opts out (loudly), loopback stays
    /// plaintext unless `FIDUCIA_NATS_REQUIRE_TLS=1` forces TLS, and
    /// `NATS_CREDS_FILE` supplies nkey/JWT credentials without putting them in
    /// the URL. Neither the URL nor the credentials are ever logged.
    pub async fn connect(nats_url: &str) -> Result<async_nats::Client, MessagingError> {
        let policy = decide_tls_policy(
            nats_url,
            env_flag(std::env::var(REQUIRE_TLS_ENV).ok().as_deref()),
            env_flag(std::env::var(ALLOW_PLAINTEXT_ENV).ok().as_deref()),
        );

        let mut options = match std::env::var(CREDS_FILE_ENV) {
            Ok(path) if !path.trim().is_empty() => {
                async_nats::ConnectOptions::with_credentials_file(path.trim())
                    .await
                    // The error names the file path (not its contents) — safe to surface.
                    .map_err(|error| {
                        MessagingError::transport(format!("read {CREDS_FILE_ENV}: {error}"))
                    })?
            }
            _ => async_nats::ConnectOptions::new(),
        };

        match policy {
            TlsPolicy::Required => {
                options = options.require_tls(true);
            }
            TlsPolicy::LoopbackPlaintext => {
                tracing::debug!("NATS endpoint is loopback; TLS not enforced");
            }
            TlsPolicy::PlaintextOptedOut => {
                tracing::warn!(
                    "{ALLOW_PLAINTEXT_ENV}=1: connecting to a NON-LOOPBACK NATS endpoint \
                     WITHOUT enforced TLS — messages (and any credentials) can cross the \
                     network in the clear. Remove the override and terminate TLS instead."
                );
            }
        }

        // The connect error is surfaced as-is; this crate never interpolates
        // the URL into messages it constructs.
        options
            .connect(nats_url)
            .await
            .map_err(MessagingError::transport)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_flag_accepts_only_explicit_truths() {
        assert!(env_flag(Some("1")));
        assert!(env_flag(Some("true")));
        assert!(env_flag(Some("TRUE")));
        assert!(env_flag(Some(" 1 ")));
        for off in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("yes"),
            Some("2"),
        ] {
            assert!(!env_flag(off), "treated {off:?} as on");
        }
    }

    #[test]
    fn host_extraction_handles_schemes_userinfo_ports_and_ipv6() {
        assert_eq!(host_of("nats://localhost:4222"), "localhost");
        assert_eq!(host_of("tls://nats.example.com:4222"), "nats.example.com");
        assert_eq!(host_of("nats://user:pass@10.0.0.5:4222"), "10.0.0.5");
        assert_eq!(host_of("nats.example.com"), "nats.example.com");
        assert_eq!(host_of("nats://[::1]:4222"), "::1");
        assert_eq!(host_of("::1"), "::1");
        assert_eq!(host_of("127.0.0.1"), "127.0.0.1");
        assert_eq!(host_of("nats://host:4222/path"), "host");
        assert_eq!(host_of(""), "");
    }

    #[test]
    fn loopback_classification() {
        for lo in ["localhost", "LOCALHOST", "127.0.0.1", "127.0.0.53", "::1"] {
            assert!(is_loopback_host(lo), "{lo} should be loopback");
        }
        for not in ["", "10.0.0.5", "nats.example.com", "192.168.1.1", "::2"] {
            assert!(!is_loopback_host(not), "{not} should NOT be loopback");
        }
    }

    #[test]
    fn mixed_server_lists_take_the_stricter_policy() {
        assert!(all_hosts_loopback("nats://localhost:4222"));
        assert!(all_hosts_loopback(
            "nats://127.0.0.1:4222, nats://[::1]:4222"
        ));
        assert!(!all_hosts_loopback(
            "nats://localhost:4222,nats://10.0.0.5:4222"
        ));
        assert!(!all_hosts_loopback(""));
        assert!(!all_hosts_loopback("   "));
    }

    #[test]
    fn policy_requires_tls_for_non_loopback_by_default() {
        assert_eq!(
            decide_tls_policy("nats://nats.example.com:4222", false, false),
            TlsPolicy::Required
        );
        // An unparseable / host-less URL fails toward requiring TLS.
        assert_eq!(decide_tls_policy("", false, false), TlsPolicy::Required);
    }

    #[test]
    fn policy_permits_loopback_plaintext_unless_tls_is_forced() {
        assert_eq!(
            decide_tls_policy("nats://localhost:4222", false, false),
            TlsPolicy::LoopbackPlaintext
        );
        // FIDUCIA_NATS_REQUIRE_TLS=1 forces TLS even on loopback.
        assert_eq!(
            decide_tls_policy("nats://localhost:4222", true, false),
            TlsPolicy::Required
        );
    }

    #[test]
    fn plaintext_opt_out_is_explicit_and_never_overrides_require() {
        assert_eq!(
            decide_tls_policy("nats://nats.example.com:4222", false, true),
            TlsPolicy::PlaintextOptedOut
        );
        // Requiring wins over the opt-out when both are set.
        assert_eq!(
            decide_tls_policy("nats://nats.example.com:4222", true, true),
            TlsPolicy::Required
        );
    }
}

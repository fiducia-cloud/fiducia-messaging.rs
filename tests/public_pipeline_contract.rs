use chrono::{Duration, TimeZone, Utc};
use fiducia_messaging::{
    tenant_scoped_dedup_id, Inbox, MessageEnvelope, MessagingError, OutboxRecord,
    RecordingPublisher, Relay,
};
use serde_json::json;
use uuid::Uuid;

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 3, 12, 0, 0)
        .single()
        .expect("valid fixed timestamp")
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

#[tokio::test]
async fn tenant_scoped_retry_is_effectively_once_across_the_public_api() {
    let tenant_a = id(1);
    let tenant_b = id(2);
    let idempotency_key = "capture/invoice-42";
    let subject = "fiducia.billing.capture.v1";

    let envelope_a = MessageEnvelope::new_at(
        now(),
        id(10),
        "billing.capture.requested",
        json!({"invoice_id": "invoice-42", "amount_minor": 1250}),
        idempotency_key,
    )
    .with_source("fiducia-customer")
    .with_tenant(tenant_a)
    .with_fencing_token(41);

    let envelope_b = MessageEnvelope::new_at(
        now(),
        id(20),
        "billing.capture.requested",
        json!({"invoice_id": "invoice-42", "amount_minor": 1250}),
        idempotency_key,
    )
    .with_source("fiducia-customer")
    .with_tenant(tenant_b)
    .with_fencing_token(73);

    assert_eq!(
        envelope_a
            .ensure_consumable(now(), true)
            .expect("fresh fenced message"),
        Some(41)
    );
    assert_eq!(
        envelope_b
            .ensure_consumable(now(), true)
            .expect("fresh fenced message"),
        Some(73)
    );

    let first = OutboxRecord::from_envelope(id(100), subject, &envelope_a)
        .expect("stage tenant A envelope");
    let crash_window_retry = OutboxRecord::from_envelope(id(101), subject, &envelope_a)
        .expect("stage the same business effect after a relay crash");
    let other_tenant = OutboxRecord::from_envelope(id(200), subject, &envelope_b)
        .expect("stage tenant B envelope");

    assert_eq!(
        first.dedup_id,
        tenant_scoped_dedup_id(Some(tenant_a), idempotency_key)
    );
    assert_eq!(first.dedup_id, crash_window_retry.dedup_id);
    assert_ne!(
        first.dedup_id, other_tenant.dedup_id,
        "the same business key must not collide across tenants"
    );

    let publisher = RecordingPublisher::new();
    let relay = Relay::new(&publisher);
    let outcome = relay
        .drain(&[first, crash_window_retry, other_tenant])
        .await;

    assert_eq!(outcome.published_count(), 3);
    assert_eq!(outcome.failed_count(), 0);
    assert_eq!(
        publisher.len(),
        2,
        "the retry collapses, while the other tenant remains independent"
    );

    let published = publisher.published();
    let decoded_a: MessageEnvelope<serde_json::Value> =
        MessageEnvelope::decode(&published[0].payload).expect("decode tenant A message");
    let decoded_b: MessageEnvelope<serde_json::Value> =
        MessageEnvelope::decode(&published[1].payload).expect("decode tenant B message");

    assert_eq!(decoded_a.tenant_id, Some(tenant_a));
    assert_eq!(decoded_b.tenant_id, Some(tenant_b));
    assert_eq!(decoded_a.require_fencing_token().unwrap(), 41);
    assert_eq!(decoded_b.require_fencing_token().unwrap(), 73);
    assert_eq!(decoded_a.idempotency_key, idempotency_key);
    assert_eq!(decoded_b.idempotency_key, idempotency_key);

    let mut inbox = Inbox::with_capacity(8);
    assert!(inbox.accept_for_tenant(Some(tenant_a), idempotency_key));
    assert!(!inbox.accept_for_tenant(Some(tenant_a), idempotency_key));
    assert!(
        inbox.accept_for_tenant(Some(tenant_b), idempotency_key),
        "consumer dedup must retain tenant isolation too"
    );
}

#[test]
fn expired_or_unfenced_messages_fail_before_an_external_effect() {
    let expiry = now() + Duration::seconds(30);
    let expired = MessageEnvelope::new_at(
        now(),
        id(300),
        "billing.capture.requested",
        json!({"invoice_id": "invoice-99"}),
        "capture/invoice-99",
    )
    .with_expiry(expiry)
    .with_fencing_token(99);

    assert!(matches!(
        expired.ensure_consumable(expiry, true),
        Err(MessagingError::Expired { expired_at }) if expired_at == expiry
    ));

    let unfenced = MessageEnvelope::new_at(
        now(),
        id(301),
        "billing.capture.requested",
        json!({"invoice_id": "invoice-100"}),
        "capture/invoice-100",
    );

    assert!(matches!(
        unfenced.ensure_consumable(now(), true),
        Err(MessagingError::MissingFencingToken { ref message_type })
            if message_type == "billing.capture.requested"
    ));
}

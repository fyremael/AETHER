use aether_storage::{PostgresJournal, PostgresTlsConfig, PostgresTlsMode};
use std::{
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn required(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn unique_namespace(label: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("tls_{label}_{nonce}")
}

fn verified(ca_paths: Vec<PathBuf>) -> PostgresTlsConfig {
    PostgresTlsConfig {
        mode: PostgresTlsMode::VerifyFull,
        ca_certificate_paths: ca_paths,
        disable_system_roots: true,
        ..PostgresTlsConfig::default()
    }
}

#[test]
fn trusted_ca_and_matching_hostname_connect() {
    let (Some(url), Some(ca)) = (
        required("AETHER_POSTGRES_TLS_TEST_URL"),
        required("AETHER_POSTGRES_TLS_CA"),
    ) else {
        return;
    };
    PostgresJournal::open_with_tls(
        &url,
        "aether_tls_test",
        unique_namespace("trusted"),
        &verified(vec![ca.into()]),
    )
    .expect("trusted CA and matching hostname must connect");
}

#[test]
fn hostname_mismatch_fails_closed() {
    let (Some(url), Some(ca)) = (
        required("AETHER_POSTGRES_TLS_HOSTNAME_MISMATCH_URL"),
        required("AETHER_POSTGRES_TLS_CA"),
    ) else {
        return;
    };
    let result = PostgresJournal::open_with_tls(
        &url,
        "aether_tls_test",
        unique_namespace("hostname"),
        &verified(vec![ca.into()]),
    );
    assert!(result.is_err(), "hostname mismatch must fail closed");
}

#[test]
fn verify_ca_is_explicit_and_omits_only_hostname_matching() {
    let (Some(url), Some(ca)) = (
        required("AETHER_POSTGRES_TLS_HOSTNAME_MISMATCH_URL"),
        required("AETHER_POSTGRES_TLS_CA"),
    ) else {
        return;
    };
    let tls = PostgresTlsConfig {
        mode: PostgresTlsMode::VerifyCa,
        ca_certificate_paths: vec![ca.into()],
        disable_system_roots: true,
        ..PostgresTlsConfig::default()
    };
    PostgresJournal::open_with_tls(&url, "aether_tls_test", unique_namespace("verify_ca"), &tls)
        .expect("verify_ca should retain trust verification while omitting hostname matching");
}

#[test]
fn untrusted_ca_fails_closed() {
    let (Some(url), Some(untrusted_ca)) = (
        required("AETHER_POSTGRES_TLS_TEST_URL"),
        required("AETHER_POSTGRES_TLS_UNTRUSTED_CA"),
    ) else {
        return;
    };
    let result = PostgresJournal::open_with_tls(
        &url,
        "aether_tls_test",
        unique_namespace("untrusted"),
        &verified(vec![untrusted_ca.into()]),
    );
    assert!(result.is_err(), "untrusted CA must fail closed");
}

#[test]
fn tls_failure_does_not_downgrade_to_plaintext() {
    let Some(url) = required("AETHER_POSTGRES_TLS_TEST_URL") else {
        return;
    };
    let result = PostgresJournal::open_with_tls(
        &url,
        "aether_tls_test",
        unique_namespace("no_downgrade"),
        &PostgresTlsConfig::development_plaintext(),
    );
    assert!(
        result.is_err(),
        "TLS-only fixture must reject explicit plaintext without any retry path"
    );
}

#[test]
fn expired_server_certificate_fails_closed() {
    let (Some(url), Some(ca)) = (
        required("AETHER_POSTGRES_TLS_EXPIRED_URL"),
        required("AETHER_POSTGRES_TLS_EXPIRED_CA"),
    ) else {
        return;
    };
    let result = PostgresJournal::open_with_tls(
        &url,
        "aether_tls_test",
        unique_namespace("expired"),
        &verified(vec![ca.into()]),
    );
    assert!(
        result.is_err(),
        "expired server certificate must fail closed"
    );
}

#[test]
fn mtls_requires_and_accepts_the_configured_client_identity() {
    let (Some(url), Some(ca), Some(client_cert), Some(client_key)) = (
        required("AETHER_POSTGRES_MTLS_TEST_URL"),
        required("AETHER_POSTGRES_TLS_CA"),
        required("AETHER_POSTGRES_MTLS_CLIENT_CERT"),
        required("AETHER_POSTGRES_MTLS_CLIENT_KEY"),
    ) else {
        return;
    };
    let without_identity = PostgresJournal::open_with_tls(
        &url,
        "aether_tls_test",
        unique_namespace("mtls_missing"),
        &verified(vec![ca.clone().into()]),
    );
    assert!(
        without_identity.is_err(),
        "mTLS endpoint must reject a missing client identity"
    );

    let mut tls = verified(vec![ca.into()]);
    tls.client_certificate_path = Some(client_cert.into());
    tls.client_private_key_path = Some(client_key.into());
    PostgresJournal::open_with_tls(&url, "aether_tls_test", unique_namespace("mtls"), &tls)
        .expect("configured mTLS identity must connect");
}

#[test]
fn two_ca_transition_trusts_old_and_rotated_servers() {
    let (Some(old_url), Some(new_url), Some(old_ca), Some(new_ca)) = (
        required("AETHER_POSTGRES_TLS_TEST_URL"),
        required("AETHER_POSTGRES_TLS_ROTATED_URL"),
        required("AETHER_POSTGRES_TLS_CA"),
        required("AETHER_POSTGRES_TLS_ROTATED_CA"),
    ) else {
        return;
    };
    let tls = verified(vec![old_ca.into(), new_ca.into()]);
    PostgresJournal::open_with_tls(
        &old_url,
        "aether_tls_test",
        unique_namespace("rotation_old"),
        &tls,
    )
    .expect("old CA remains trusted during transition");
    PostgresJournal::open_with_tls(
        &new_url,
        "aether_tls_test",
        unique_namespace("rotation_new"),
        &tls,
    )
    .expect("new CA is trusted during transition");
}

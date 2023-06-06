//! Tests that we still support running using deprecated names so that deployments continue to work
//! while transitioning. There was never a `querier2` command, so there isn't a test for it here.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;
use tempfile::tempdir;
use test_helpers_end_to_end::{AddAddrEnv, BindAddresses, ServerType};

#[tokio::test]
async fn ingester2_runs_ingester() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    let mut command = Command::cargo_bin("influxdb_iox").unwrap();

    if cfg!(not(windows)) {
        // Only clear the environment on non Windows platforms. On Windows, the SYSTEMROOT environment variable must be
        // preserved for DLLs to correctly load, otherwise the OS error WSAEPROVIDERFAILEDINIT (10106) is thrown when
        // attempting to bind the TCP listener. See:
        // https://learn.microsoft.com/en-us/windows/win32/winsock/windows-sockets-error-codes-2
        // https://travis-ci.community/t/socket-the-requested-service-provider-could-not-be-loaded-or-initialized/1127/1
        command.env_clear();
    }

    command
        .args(["run", "ingester2", "-v"])
        .env("HOME", tmpdir.path())
        .env("INFLUXDB_IOX_WAL_DIRECTORY", tmpdir.path())
        .env("INFLUXDB_IOX_CATALOG_DSN", "memory")
        .add_addr_env(ServerType::Ingester, &addrs)
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("error: unrecognized subcommand 'ingester2'").not())
        .stdout(predicate::str::contains(
            "InfluxDB IOx Ingester server ready",
        ));
}

#[tokio::test]
async fn router2_runs_router() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    let mut command = Command::cargo_bin("influxdb_iox").unwrap();

    if cfg!(not(windows)) {
        // Only clear the environment on non Windows platforms. On Windows, the SYSTEMROOT environment variable must be
        // preserved for DLLs to correctly load, otherwise the OS error WSAEPROVIDERFAILEDINIT (10106) is thrown when
        // attempting to bind the TCP listener. See:
        // https://learn.microsoft.com/en-us/windows/win32/winsock/windows-sockets-error-codes-2
        // https://travis-ci.community/t/socket-the-requested-service-provider-could-not-be-loaded-or-initialized/1127/1
        command.env_clear();
    }

    command
        .args(["run", "router2", "-v"])
        .env("HOME", tmpdir.path())
        .env("INFLUXDB_IOX_WAL_DIRECTORY", tmpdir.path())
        .env("INFLUXDB_IOX_CATALOG_DSN", "memory")
        .add_addr_env(ServerType::Router, &addrs)
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("error: unrecognized subcommand 'router2'").not())
        .stdout(predicate::str::contains("InfluxDB IOx Router server ready"));
}

#[tokio::test]
async fn compactor2_runs_compactor() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    let mut command = Command::cargo_bin("influxdb_iox").unwrap();

    if cfg!(not(windows)) {
        // Only clear the environment on non Windows platforms. On Windows, the SYSTEMROOT environment variable must be
        // preserved for DLLs to correctly load, otherwise the OS error WSAEPROVIDERFAILEDINIT (10106) is thrown when
        // attempting to bind the TCP listener. See:
        // https://learn.microsoft.com/en-us/windows/win32/winsock/windows-sockets-error-codes-2
        // https://travis-ci.community/t/socket-the-requested-service-provider-could-not-be-loaded-or-initialized/1127/1
        command.env_clear();
    }

    command
        .args(["run", "compactor2", "-v"])
        .env("HOME", tmpdir.path())
        .env("INFLUXDB_IOX_WAL_DIRECTORY", tmpdir.path())
        .env("INFLUXDB_IOX_CATALOG_DSN", "memory")
        .add_addr_env(ServerType::Compactor, &addrs)
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("error: unrecognized subcommand 'compactor2'").not())
        .stdout(predicate::str::contains(
            "InfluxDB IOx Compactor server ready",
        ));
}

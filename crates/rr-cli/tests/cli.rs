//! End-to-end smoke test for the `rr` binary.

use assert_cmd::Command;

#[test]
#[expect(clippy::panic, reason = "test failure path")]
fn version_flag_succeeds() {
    let Ok(mut cmd) = Command::cargo_bin("rr") else {
        panic!("rr binary not found");
    };
    cmd.arg("--version").assert().success();
}

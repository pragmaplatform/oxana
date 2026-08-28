#[test]
fn worker_macro_validation_errors() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/batch_size_requires_timeout.rs");
    t.compile_fail("tests/ui/batch_size_zero.rs");
    t.compile_fail("tests/ui/batch_timeout_requires_size.rs");
    t.compile_fail("tests/ui/cron_replace_conflict.rs");
    t.compile_fail("tests/ui/static_queue_discovery_interval.rs");
    t.compile_fail("tests/ui/worker_group_requires_unit_struct.rs");
}

#[test]
fn batch_worker_requires_process_batch_hook() {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let temp_dir = std::env::temp_dir().join(format!(
        "oxana-missing-process-batch-{}",
        std::process::id()
    ));

    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(temp_dir.join("src")).expect("temp crate src dir should be created");

    std::fs::write(
        temp_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "oxana-missing-process-batch"
version = "0.0.0"
edition = "2024"

[dependencies]
async-trait = "0.1"
oxana = {{ path = "{}" }}
serde = {{ version = "1.0", features = ["derive"] }}
thiserror = "2.0"
"#,
            crate_dir.display()
        ),
    )
    .expect("temp crate manifest should be written");
    std::fs::write(
        temp_dir.join("src/main.rs"),
        include_str!("ui/batch_missing_process_batch.rs"),
    )
    .expect("temp crate main should be written");

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = std::process::Command::new(cargo)
        .arg("check")
        .arg("--quiet")
        .env("CARGO_TARGET_DIR", temp_dir.join("target"))
        .current_dir(&temp_dir)
        .output()
        .expect("cargo check should run");

    let _ = std::fs::remove_dir_all(&temp_dir);

    assert!(
        !output.status.success(),
        "missing process_batch fixture should fail to compile"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("process_batch") && stderr.contains("MissingProcessBatchWorker"),
        "missing process_batch fixture failed with unexpected stderr:\n{stderr}"
    );
}

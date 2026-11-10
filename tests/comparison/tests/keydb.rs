use avalanche_comparison::{Database, Mode, database_test};

#[test]
#[ignore = "external database load test; set BENCH_ADDR and use --release --ignored --nocapture"]
fn get_rps() -> anyhow::Result<()> {
    database_test(Database::Keydb, Mode::Get)
}

#[test]
#[ignore = "external database load test; set BENCH_ADDR and use --release --ignored --nocapture"]
fn set_rps() -> anyhow::Result<()> {
    database_test(Database::Keydb, Mode::Set)
}

#[test]
#[ignore = "external database load test; set BENCH_ADDR and use --release --ignored --nocapture"]
fn mixed_rps() -> anyhow::Result<()> {
    database_test(Database::Keydb, Mode::Mixed)
}

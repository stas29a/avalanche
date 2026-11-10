fn main() -> anyhow::Result<()> {
    let settings = avalanche_comparison::Settings::parse(std::env::args().skip(1))?;
    for measurement in avalanche_comparison::execute(settings)? {
        println!("{}", measurement.json());
    }
    Ok(())
}

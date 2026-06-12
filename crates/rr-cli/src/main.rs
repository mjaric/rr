//! `rr` — trading intelligence app command-line interface.

use clap::Parser;

/// Simulation-first crypto trading agent.
#[derive(Parser)]
#[command(name = "rr", version, about)]
struct Cli {}

fn main() {
    let Cli {} = Cli::parse();
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }
}

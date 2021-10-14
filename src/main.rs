mod cmd_misc;
mod cmd_pull;
mod rewind;
mod undo;
mod undo_sink;
mod undo_source;

use anyhow::Result;
use cmd_pull::Pullcmd;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
  #[structopt(subcommand)]
  subcommand: Subcmd,
}

#[derive(Debug, StructOpt)]
enum Subcmd {
  Pull(Pullcmd),
}

fn main() -> Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }
  pretty_env_logger::init_timed();
  let opt = Opt::from_args();
  match &opt.subcommand {
    Subcmd::Pull(cmd) => {
      cmd.run()?;
    }
  }
  Ok(())
}

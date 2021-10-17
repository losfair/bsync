mod blob;
mod cmd_pull;
mod cmd_replay;
mod cmd_squash;
mod config;
mod db;
mod util;

use anyhow::Result;
use cmd_pull::Pullcmd;
use cmd_replay::Replaycmd;
use cmd_squash::SquashCmd;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
  #[structopt(subcommand)]
  subcommand: Subcmd,
}

#[derive(Debug, StructOpt)]
enum Subcmd {
  Pull(Pullcmd),
  Replay(Replaycmd),
  Squash(SquashCmd),
}

fn main() -> Result<()> {
  pretty_env_logger::init_timed();
  let opt = Opt::from_args();
  match &opt.subcommand {
    Subcmd::Pull(cmd) => {
      cmd.run()?;
    }
    Subcmd::Replay(cmd) => {
      cmd.run()?;
    }
    Subcmd::Squash(cmd) => {
      cmd.run()?;
    }
  }
  Ok(())
}

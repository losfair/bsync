mod cmd_misc;
mod cmd_pull;
mod cmd_serve;
mod cmd_versions;
mod config;
mod gc;
mod managed;
mod overlay;
mod recover;
mod rewind;
mod signals;
mod store;
mod util;

use anyhow::Result;
use cmd_pull::Pullcmd;
use cmd_serve::ServeCmd;
use cmd_versions::VersionsCmd;
use gc::GcCmd;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
  #[structopt(subcommand)]
  subcommand: Subcmd,
}

#[derive(Debug, StructOpt)]
enum Subcmd {
  Pull(Pullcmd),
  Versions(VersionsCmd),
  Gc(GcCmd),
  Serve(ServeCmd),
}

fn main() -> Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }
  pretty_env_logger::init_timed();
  signals::init();
  let opt = Opt::from_args();
  match &opt.subcommand {
    Subcmd::Pull(cmd) => {
      cmd.run()?;
    }
    Subcmd::Versions(cmd) => {
      cmd.run()?;
    }
    Subcmd::Gc(cmd) => {
      cmd.run()?;
    }
    Subcmd::Serve(cmd) => {
      cmd.run()?;
    }
  }
  Ok(())
}

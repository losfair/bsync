use parking_lot::{lock_api::RawMutex, Mutex};
use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;

pub static CRITICAL_WRITE_LOCK: Mutex<()> = Mutex::const_new(RawMutex::INIT, ());

pub fn init() {
  let mut signals = Signals::new(&[SIGINT, SIGTERM, SIGHUP]).unwrap();
  std::thread::spawn(move || {
    for sig in &mut signals {
      log::info!("Received signal {}. Waiting for critical writes.", sig);
      let _guard = CRITICAL_WRITE_LOCK.lock();
      std::process::exit(1);
    }
  });
}

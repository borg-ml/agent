use borg_view::wire::{ClientFrame, ServerFrame};
use ts_rs::{Config, TS};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args()
        .nth(1)
        .ok_or("usage: export_types DIRECTORY")?;
    let config = Config::default().with_out_dir(destination);
    ClientFrame::export_all(&config)?;
    ServerFrame::export_all(&config)?;
    Ok(())
}

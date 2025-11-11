use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let proto_dir = Path::new("proto");
    let proto_files: Vec<PathBuf> = match fs::read_dir(proto_dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("capnp"))
            .collect(),
        Err(_) => Vec::new(),
    };

    if proto_files.is_empty() {
        return Ok(());
    }

    let mut command = capnpc::CompilerCommand::new();
    command.src_prefix(proto_dir);

    for file in proto_files {
        command.file(file);
    }

    command.run()?;
    Ok(())
}

//! Persistent device identity. The endpoint id is derived from this key, so
//! losing or regenerating it turns the device into a new one.

use std::{fs, path::Path};

use anyhow::{Context, bail};
use iroh::SecretKey;

pub fn load_or_create(path: &Path) -> anyhow::Result<SecretKey> {
    if path.exists() {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let Ok(bytes) = <[u8; 32]>::try_from(bytes.as_slice()) else {
            bail!("{} is not a 32-byte secret key", path.display());
        };
        return Ok(SecretKey::from_bytes(&bytes));
    }

    let key = SecretKey::generate();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    write_private(path, &key.to_bytes())?;
    Ok(key)
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    f.write_all(bytes)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    fs::write(path, bytes).with_context(|| format!("creating {}", path.display()))
}

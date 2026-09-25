use crate::Error;
use crate::storage::{sha256_hex, write_if_absent};
use std::fs;
use std::path::Path;

pub(super) fn write_checksums(staging: &Path) -> Result<String, Error> {
    let bytes = checksum_payload(staging)?;
    write_if_absent(&staging.join("checksums.sha256"), &bytes)?;
    Ok(sha256_hex(&bytes))
}

pub(super) fn refresh_checksums(staging: &Path) -> Result<String, Error> {
    let bytes = checksum_payload(staging)?;
    fs::write(staging.join("checksums.sha256"), &bytes)
        .map_err(|e| Error::storage(format!("refresh Generation checksums: {e}")))?;
    Ok(sha256_hex(&bytes))
}

fn checksum_payload(staging: &Path) -> Result<Vec<u8>, Error> {
    let mut files = Vec::new();
    collect_files(staging, staging, &mut files)?;
    files.retain(|(relative, _)| relative != "checksums.sha256");
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut bytes = Vec::new();
    for (relative, content) in files {
        bytes.extend_from_slice(sha256_hex(&content).as_bytes());
        bytes.extend_from_slice(b"  ");
        bytes.extend_from_slice(relative.as_bytes());
        bytes.push(b'\n');
    }
    Ok(bytes)
}

pub(crate) fn directory_digest(directory: &Path) -> Result<String, Error> {
    let mut files = Vec::new();
    collect_files(directory, directory, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let mut digest_input = Vec::new();
    for (relative, bytes) in files {
        digest_input.extend(relative.as_bytes());
        digest_input.push(0);
        digest_input.extend(bytes);
        digest_input.push(0);
    }
    Ok(sha256_hex(&digest_input))
}

pub(super) fn directory_or_file_digest(path: &Path) -> Result<String, Error> {
    if path.is_file() {
        return fs::read(path)
            .map(|bytes| sha256_hex(&bytes))
            .map_err(|e| Error::storage(format!("read artifact {}: {e}", path.display())));
    }
    directory_digest(path)
}

pub(crate) fn directory_bytes(directory: &Path) -> Result<u64, Error> {
    let mut files = Vec::new();
    collect_files(directory, directory, &mut files)?;
    Ok(files.iter().map(|(_, bytes)| bytes.len() as u64).sum())
}

pub(crate) fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), Error> {
    for entry in
        fs::read_dir(current).map_err(|e| Error::storage(format!("read index directory: {e}")))?
    {
        let path = entry
            .map_err(|e| Error::storage(format!("read index entry: {e}")))?
            .path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| Error::storage(format!("relative index path: {e}")))?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((
                relative,
                fs::read(&path).map_err(|e| Error::storage(format!("read index artifact: {e}")))?,
            ));
        }
    }
    Ok(())
}

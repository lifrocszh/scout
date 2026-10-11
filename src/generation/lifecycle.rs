use super::GenerationPointer;
use super::checksums::refresh_checksums;
use super::validation::{load_generation_manifest, validate_generation, write_generation_manifest};
use crate::error::Error;
use crate::storage::new_run_id;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn activate_generation(data_dir: &Path, generation_id: &str) -> Result<Value, Error> {
    let _lock = GenerationLock::acquire(data_dir)?;
    let candidate = find_generation_path(data_dir, generation_id)?;
    let sealed = if candidate
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == OsStr::new(".staging"))
    {
        seal_generation(data_dir, &candidate, generation_id)?
    } else {
        candidate
    };
    let manifest = validate_generation(&sealed, "sealed")?;
    let pointer = GenerationPointer {
        generation_id: manifest.generation_id.clone(),
        manifest_sha256: manifest.manifest_sha256.clone(),
    };
    let current = read_pointer(data_dir, "CURRENT").ok().flatten();
    if current
        .as_ref()
        .is_some_and(|current| current.generation_id == pointer.generation_id)
    {
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "changed": false,
            "sealed_path": sealed,
        }));
    }
    if let Some(current) = current.filter(|current| validate_pointer(data_dir, current).is_ok()) {
        write_pointer_atomic(data_dir, "PREVIOUS", &current)?;
    }
    write_pointer_atomic(data_dir, "CURRENT", &pointer)?;
    Ok(json!({
        "generation_id": pointer.generation_id,
        "manifest_sha256": pointer.manifest_sha256,
        "changed": true,
        "sealed_path": sealed,
    }))
}

pub(crate) fn recover_generation(data_dir: &Path) -> Result<Value, Error> {
    let current = read_pointer(data_dir, "CURRENT").ok().flatten();
    if let Some(pointer) = current.as_ref()
        && validate_pointer(data_dir, pointer).is_ok()
    {
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "fallback": false,
            "ready": true,
        }));
    }

    let previous = read_pointer(data_dir, "PREVIOUS").ok().flatten();
    if let Some(pointer) = previous.as_ref()
        && validate_pointer(data_dir, pointer).is_ok()
    {
        write_pointer_atomic(data_dir, "CURRENT", pointer)?;
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "fallback": true,
            "ready": true,
        }));
    }

    let generations = data_dir.join("generations");
    let mut paths = fs::read_dir(&generations)
        .map_err(|e| Error::storage(format!("read Generation directory: {e}")))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .is_some_and(|name| name != OsStr::new(".staging"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        let Ok(manifest) = validate_generation(&path, "sealed") else {
            continue;
        };
        let pointer = GenerationPointer {
            generation_id: manifest.generation_id,
            manifest_sha256: manifest.manifest_sha256,
        };
        write_pointer_atomic(data_dir, "CURRENT", &pointer)?;
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "fallback": true,
            "ready": true,
        }));
    }
    Err(Error::invalid("no valid sealed Generation is available"))
}

pub(crate) fn prune_generations(
    data_dir: &Path,
    explicitly_retained: &[String],
) -> Result<Value, Error> {
    let _lock = GenerationLock::acquire(data_dir)?;
    let current = read_pointer(data_dir, "CURRENT").ok().flatten();
    let previous = read_pointer(data_dir, "PREVIOUS").ok().flatten();
    let mut protected = explicitly_retained.iter().cloned().collect::<HashSet<_>>();
    if let Some(pointer) = current.as_ref() {
        protected.insert(pointer.generation_id.clone());
    }
    if let Some(pointer) = previous.as_ref() {
        protected.insert(pointer.generation_id.clone());
    }
    let generations = data_dir.join("generations");
    let entries = fs::read_dir(&generations)
        .map_err(|e| Error::storage(format!("read Generation directory: {e}")))?;
    let mut removed = Vec::new();
    let mut kept = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| Error::storage(format!("read Generation entry: {e}")))?
            .path();
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if !path.is_dir() || name == ".staging" {
            continue;
        }
        if protected.contains(name)
            || path.join(".pinned").exists()
            || path.join(".in-use").exists()
            || validate_generation(&path, "sealed").is_err()
        {
            kept.push(name.to_string());
            continue;
        }
        fs::remove_dir_all(&path)
            .map_err(|e| Error::storage(format!("remove Generation {}: {e}", path.display())))?;
        removed.push(name.to_string());
    }
    removed.sort();
    kept.sort();
    Ok(json!({"removed": removed, "kept": kept}))
}

struct GenerationLock {
    path: PathBuf,
}

impl GenerationLock {
    fn acquire(data_dir: &Path) -> Result<Self, Error> {
        let generations = data_dir.join("generations");
        fs::create_dir_all(&generations)
            .map_err(|e| Error::storage(format!("create Generation directory: {e}")))?;
        let path = generations.join(".generation.lock");
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => Ok(Self { path }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(Error::storage(
                "another Generation operation is in progress",
            )),
            Err(error) => Err(Error::storage(format!("create Generation lock: {error}"))),
        }
    }
}

impl Drop for GenerationLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn find_generation_path(data_dir: &Path, generation_id: &str) -> Result<PathBuf, Error> {
    if generation_id.is_empty() || generation_id.contains('/') || generation_id.contains('\\') {
        return Err(Error::invalid("generation ID is not a safe path component"));
    }
    let generations = data_dir.join("generations");
    let sealed = generations.join(generation_id);
    if sealed.is_dir() {
        return Ok(sealed);
    }
    let staging = generations.join(".staging");
    let entries = fs::read_dir(&staging)
        .map_err(|e| Error::invalid(format!("read Generation staging directory: {e}")))?;
    let mut matches = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| Error::storage(format!("read Generation staging entry: {e}")))?
            .path();
        if !path.is_dir() {
            continue;
        }
        let Ok(manifest) = load_generation_manifest(&path) else {
            continue;
        };
        if manifest.generation_id == generation_id {
            matches.push(path);
        }
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(Error::invalid(format!(
            "Generation {generation_id:?} not found"
        ))),
        _ => Err(Error::invalid(format!(
            "Generation {generation_id:?} is ambiguous"
        ))),
    }
}

fn seal_generation(data_dir: &Path, staging: &Path, generation_id: &str) -> Result<PathBuf, Error> {
    let mut manifest = validate_generation(staging, "validated")?;
    if manifest.generation_id != generation_id {
        return Err(Error::invalid(
            "staging Generation ID does not match activation request",
        ));
    }
    manifest.status = "sealed".into();
    write_generation_manifest(&staging.join("generation-manifest.json"), &mut manifest)?;
    refresh_checksums(staging)?;
    let final_path = data_dir.join("generations").join(generation_id);
    fs::rename(staging, &final_path)
        .map_err(|e| Error::storage(format!("seal Generation {}: {e}", final_path.display())))?;
    validate_generation(&final_path, "sealed")?;
    Ok(final_path)
}

pub(crate) fn read_pointer(
    data_dir: &Path,
    name: &str,
) -> Result<Option<GenerationPointer>, Error> {
    let path = data_dir.join(name);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::storage(format!("read {name} pointer: {error}"))),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| Error::invalid(format!("parse {name} pointer: {e}")))
}

pub(crate) fn validate_pointer(data_dir: &Path, pointer: &GenerationPointer) -> Result<(), Error> {
    let path = data_dir.join("generations").join(&pointer.generation_id);
    let manifest = validate_generation(&path, "sealed")?;
    if manifest.manifest_sha256 != pointer.manifest_sha256 {
        return Err(Error::invalid("Generation pointer digest mismatch"));
    }
    Ok(())
}

fn write_pointer_atomic(
    data_dir: &Path,
    name: &str,
    pointer: &GenerationPointer,
) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(pointer)
        .map_err(|e| Error::storage(format!("serialize {name} pointer: {e}")))?;
    fs::create_dir_all(data_dir)
        .map_err(|e| Error::storage(format!("create data directory: {e}")))?;
    let temporary = data_dir.join(format!(".{name}.tmp-{}", new_run_id()));
    fs::write(&temporary, bytes)
        .map_err(|e| Error::storage(format!("write temporary {name} pointer: {e}")))?;
    fs::rename(&temporary, data_dir.join(name))
        .map_err(|e| Error::storage(format!("publish {name} pointer: {e}")))?;
    if let Ok(directory) = File::open(data_dir) {
        let _ = directory.sync_all();
    }
    Ok(())
}

use crate::error::Error;
use crate::storage::{new_run_id, sha256_hex};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

const CHECKPOINT_MAGIC: &[u8] = b"SCOUT-EMBEDDING-CHUNK-V1\0";
pub(super) const CHECKPOINT_SCHEMA: &str = "scout.embedding-checkpoint.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct BuildCheckpointIdentity {
    schema_version: String,
    build_run_id: String,
    corpus_snapshot_sha256: String,
    build_profile_sha256: String,
    catalog_sha256: String,
    embedding_fingerprint: String,
    dimensions: usize,
    chunk_size: usize,
}

impl BuildCheckpointIdentity {
    pub(super) fn new(
        build_run_id: &str,
        corpus_snapshot_sha256: &str,
        build_profile_sha256: &str,
        catalog_sha256: &str,
        embedding_fingerprint: &str,
        dimensions: usize,
        chunk_size: usize,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_SCHEMA.into(),
            build_run_id: build_run_id.into(),
            corpus_snapshot_sha256: corpus_snapshot_sha256.into(),
            build_profile_sha256: build_profile_sha256.into(),
            catalog_sha256: catalog_sha256.into(),
            embedding_fingerprint: embedding_fingerprint.into(),
            dimensions,
            chunk_size,
        }
    }
}

pub(super) struct BuildCheckpointStore {
    root: PathBuf,
    identity_sha256: String,
    dimensions: usize,
}

impl BuildCheckpointStore {
    pub(super) fn open(
        root: &Path,
        identity: &BuildCheckpointIdentity,
        resume: bool,
    ) -> Result<Self, Error> {
        let identity_path = root.join("identity.json");
        if root.exists() {
            if !resume {
                return Err(Error::storage(format!(
                    "checkpoint directory {} already exists; use --resume with its build run ID",
                    root.display()
                )));
            }
            if !identity_path.exists() {
                let entries = fs::read_dir(root)
                    .map_err(|error| Error::storage(format!("read checkpoint directory: {error}")))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| Error::storage(format!("read checkpoint entry: {error}")))?;
                if !entries.is_empty() {
                    return Err(Error::invalid(
                        "resume checkpoint identity is missing from a nonempty checkpoint directory",
                    ));
                }
                write_identity(&identity_path, identity)?;
            } else {
                let bytes = fs::read(&identity_path).map_err(|error| {
                    Error::storage(format!("read checkpoint identity: {error}"))
                })?;
                let existing: BuildCheckpointIdentity =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        Error::invalid(format!("parse checkpoint identity: {error}"))
                    })?;
                if &existing != identity {
                    return Err(Error::invalid(
                        "resume checkpoint identity does not match the Corpus, Generation profile, model assets, or runtime",
                    ));
                }
            }
        } else {
            fs::create_dir_all(root)
                .map_err(|error| Error::storage(format!("create checkpoint directory: {error}")))?;
            write_identity(&identity_path, identity)?;
        }
        let identity_bytes = serde_json::to_vec(identity)
            .map_err(|error| Error::storage(format!("serialize checkpoint identity: {error}")))?;
        Ok(Self {
            root: root.to_path_buf(),
            identity_sha256: sha256_hex(&identity_bytes),
            dimensions: identity.dimensions,
        })
    }

    pub(super) fn load_chunk(
        &self,
        chunk_number: usize,
        expected_keys: &[u64],
    ) -> Result<Option<Vec<(u64, Vec<f32>)>>, Error> {
        let path = self.chunk_path(chunk_number);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(Error::storage(format!(
                    "read embedding checkpoint: {error}"
                )));
            }
        };
        Ok(parse_chunk(
            &bytes,
            &self.identity_sha256,
            chunk_number,
            expected_keys,
            self.dimensions,
        ))
    }

    pub(super) fn identity_sha256(&self) -> &str {
        &self.identity_sha256
    }

    pub(super) fn save_chunk(
        &self,
        chunk_number: usize,
        vectors: &[(u64, Vec<f32>)],
    ) -> Result<(), Error> {
        let mut bytes = Vec::with_capacity(
            CHECKPOINT_MAGIC.len() + 64 + 16 + vectors.len() * (8 + self.dimensions * 4) + 64,
        );
        bytes.extend_from_slice(CHECKPOINT_MAGIC);
        bytes.extend_from_slice(self.identity_sha256.as_bytes());
        bytes.extend_from_slice(&(chunk_number as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.dimensions as u32).to_le_bytes());
        bytes.extend_from_slice(&(vectors.len() as u32).to_le_bytes());
        for (key, vector) in vectors {
            if vector.len() != self.dimensions || vector.iter().any(|value| !value.is_finite()) {
                return Err(Error::invalid(
                    "cannot checkpoint an invalid embedding vector",
                ));
            }
            bytes.extend_from_slice(&key.to_le_bytes());
            for value in vector {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        let checksum = sha256_hex(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        atomic_write(&self.chunk_path(chunk_number), &bytes)
    }

    fn chunk_path(&self, chunk_number: usize) -> PathBuf {
        self.root.join(format!("chunk-{chunk_number:06}.bin"))
    }
}

pub(super) fn remove_checkpoint_store(root: &Path) -> Result<(), Error> {
    match fs::remove_dir_all(root) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::storage(format!("remove build checkpoints: {error}"))),
    }
}

fn write_identity(path: &Path, identity: &BuildCheckpointIdentity) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(identity)
        .map_err(|error| Error::storage(format!("serialize checkpoint identity: {error}")))?;
    atomic_write(path, &bytes)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::storage("checkpoint path has no parent directory"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkpoint");
    let temporary = parent.join(format!(".{file_name}.tmp-{}", new_run_id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| Error::storage(format!("create checkpoint temporary file: {error}")))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::storage(format!("write durable checkpoint: {error}")))?;
    drop(file);
    fs::rename(&temporary, path)
        .map_err(|error| Error::storage(format!("publish checkpoint atomically: {error}")))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::storage(format!("sync checkpoint directory: {error}")))
}

fn parse_chunk(
    bytes: &[u8],
    expected_identity: &str,
    expected_chunk_number: usize,
    expected_keys: &[u64],
    expected_dimensions: usize,
) -> Option<Vec<(u64, Vec<f32>)>> {
    if bytes.len() < CHECKPOINT_MAGIC.len() + 64 + 16 + 64 {
        return None;
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - 64);
    if std::str::from_utf8(checksum).ok()? != sha256_hex(payload)
        || !payload.starts_with(CHECKPOINT_MAGIC)
    {
        return None;
    }
    let mut offset = CHECKPOINT_MAGIC.len();
    let identity_end = offset + 64;
    if std::str::from_utf8(payload.get(offset..identity_end)?).ok()? != expected_identity {
        return None;
    }
    offset = identity_end;
    let chunk_number = u64::from_le_bytes(payload.get(offset..offset + 8)?.try_into().ok()?);
    offset += 8;
    let dimensions = u32::from_le_bytes(payload.get(offset..offset + 4)?.try_into().ok()?) as usize;
    offset += 4;
    let vector_count =
        u32::from_le_bytes(payload.get(offset..offset + 4)?.try_into().ok()?) as usize;
    offset += 4;
    if chunk_number != expected_chunk_number as u64
        || dimensions != expected_dimensions
        || vector_count != expected_keys.len()
        || payload.len() != offset + vector_count * (8 + dimensions * 4)
    {
        return None;
    }
    let mut vectors = Vec::with_capacity(vector_count);
    for expected_key in expected_keys {
        let key = u64::from_le_bytes(payload.get(offset..offset + 8)?.try_into().ok()?);
        offset += 8;
        if key != *expected_key {
            return None;
        }
        let mut vector = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            let value = f32::from_le_bytes(payload.get(offset..offset + 4)?.try_into().ok()?);
            offset += 4;
            if !value.is_finite() {
                return None;
            }
            vector.push(value);
        }
        vectors.push((key, vector));
    }
    Some(vectors)
}

use crate::error::Error;
use crate::storage::new_run_id;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const CACHE_SCHEMA: &str = "scout.embedding-reuse-cache.v1";
const CACHE_RECORD_MAGIC: &[u8] = b"SCOUT-EMBEDDING-CACHE-V1\0";
const INDEX_ENTRY_BYTES: usize = 32 + 8;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CacheIdentity {
    schema_version: String,
    embedding_fingerprint: String,
    dimensions: usize,
}

pub(super) struct EmbeddingReuseCache {
    identity: CacheIdentity,
    record_size: usize,
    vectors: File,
    index: File,
    entries: HashMap<[u8; 32], u64>,
    _lock: CacheLock,
}

struct CacheLock {
    path: PathBuf,
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl EmbeddingReuseCache {
    pub(super) fn open(
        root: &Path,
        embedding_fingerprint: &str,
        dimensions: usize,
    ) -> Result<Self, Error> {
        let cache_root = root.join(embedding_fingerprint);
        fs::create_dir_all(&cache_root)
            .map_err(|error| Error::storage(format!("create embedding reuse cache: {error}")))?;
        let lock = acquire_lock(&cache_root.join("writer.lock"))?;
        let identity = CacheIdentity {
            schema_version: CACHE_SCHEMA.into(),
            embedding_fingerprint: embedding_fingerprint.into(),
            dimensions,
        };
        let identity_path = cache_root.join("identity.json");
        if identity_path.exists() {
            let bytes = fs::read(&identity_path).map_err(|error| {
                Error::storage(format!("read embedding cache identity: {error}"))
            })?;
            let existing: CacheIdentity = serde_json::from_slice(&bytes).map_err(|error| {
                Error::invalid(format!("parse embedding cache identity: {error}"))
            })?;
            if existing != identity {
                return Err(Error::invalid(
                    "embedding reuse cache fingerprint or dimensions mismatch",
                ));
            }
        } else {
            let bytes = serde_json::to_vec_pretty(&identity).map_err(|error| {
                Error::storage(format!("serialize embedding cache identity: {error}"))
            })?;
            atomic_write(&identity_path, &bytes)?;
        }

        let dimensions_bytes = dimensions
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| Error::invalid("embedding cache dimensions overflow"))?;
        let record_size = CACHE_RECORD_MAGIC.len() + 32 + 4 + dimensions_bytes + 32;
        let vectors_path = cache_root.join("vectors.bin");
        let index_path = cache_root.join("index.bin");
        let mut vectors = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&vectors_path)
            .map_err(|error| Error::storage(format!("open embedding cache vectors: {error}")))?;
        let mut index = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&index_path)
            .map_err(|error| Error::storage(format!("open embedding cache index: {error}")))?;
        let mut entries = HashMap::new();
        let mut bytes = vec![0_u8; INDEX_ENTRY_BYTES];
        let mut valid_length = 0_u64;
        let vectors_length = vectors
            .metadata()
            .map_err(|error| Error::storage(format!("stat embedding cache vectors: {error}")))?
            .len();
        let valid_vectors_length = vectors_length / record_size as u64 * record_size as u64;
        if valid_vectors_length != vectors_length {
            vectors
                .set_len(valid_vectors_length)
                .and_then(|_| vectors.sync_all())
                .map_err(|error| {
                    Error::storage(format!("trim incomplete cache vector: {error}"))
                })?;
        }
        loop {
            match index.read_exact(&mut bytes) {
                Ok(()) => {
                    let mut key = [0_u8; 32];
                    key.copy_from_slice(&bytes[..32]);
                    let offset = u64::from_le_bytes(bytes[32..40].try_into().expect("fixed width"));
                    if offset % record_size as u64 == 0
                        && offset.saturating_add(record_size as u64) <= valid_vectors_length
                    {
                        entries.insert(key, offset);
                    }
                    valid_length += INDEX_ENTRY_BYTES as u64;
                }
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => {
                    return Err(Error::storage(format!(
                        "read embedding cache index: {error}"
                    )));
                }
            }
        }
        if index
            .metadata()
            .map_err(|error| Error::storage(format!("stat embedding cache index: {error}")))?
            .len()
            != valid_length
        {
            index.set_len(valid_length).map_err(|error| {
                Error::storage(format!("trim incomplete cache index entry: {error}"))
            })?;
            index
                .sync_all()
                .map_err(|error| Error::storage(format!("sync trimmed cache index: {error}")))?;
        }
        vectors
            .seek(SeekFrom::End(0))
            .map_err(|error| Error::storage(format!("seek embedding cache vectors: {error}")))?;
        index
            .seek(SeekFrom::End(0))
            .map_err(|error| Error::storage(format!("seek embedding cache index: {error}")))?;
        Ok(Self {
            identity,
            record_size,
            vectors,
            index,
            entries,
            _lock: lock,
        })
    }

    pub(super) fn key_for_input(&self, input: &str) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"scout.document-embedding-cache-key.v1\0");
        digest.update(self.identity.embedding_fingerprint.as_bytes());
        digest.update([0]);
        digest.update(input.as_bytes());
        digest.finalize().into()
    }

    pub(super) fn load(&mut self, key: &[u8; 32]) -> Result<Option<Vec<f32>>, Error> {
        let Some(offset) = self.entries.get(key).copied() else {
            return Ok(None);
        };
        let mut record = vec![0_u8; self.record_size];
        if let Err(error) = self
            .vectors
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.vectors.read_exact(&mut record))
        {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                self.entries.remove(key);
                return Ok(None);
            }
            return Err(Error::storage(format!(
                "read embedding cache vector: {error}"
            )));
        }
        let vector_start = CACHE_RECORD_MAGIC.len() + 32 + 4;
        let payload_end = self.record_size - 32;
        let valid = record.starts_with(CACHE_RECORD_MAGIC)
            && record
                .get(CACHE_RECORD_MAGIC.len()..CACHE_RECORD_MAGIC.len() + 32)
                .is_some_and(|record_key| record_key == key)
            && u32::from_le_bytes(
                record[CACHE_RECORD_MAGIC.len() + 32..vector_start]
                    .try_into()
                    .expect("fixed width"),
            ) as usize
                == self.identity.dimensions
            && Sha256::digest(&record[..payload_end]).as_slice() == &record[payload_end..];
        if !valid {
            self.entries.remove(key);
            return Ok(None);
        }
        let mut vector = Vec::with_capacity(self.identity.dimensions);
        for value in record[vector_start..payload_end].chunks_exact(4) {
            let value = f32::from_le_bytes(value.try_into().expect("fixed width"));
            if !value.is_finite() {
                self.entries.remove(key);
                return Ok(None);
            }
            vector.push(value);
        }
        Ok(Some(vector))
    }

    pub(super) fn store(&mut self, entries: &[([u8; 32], Vec<f32>)]) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let start = self
            .vectors
            .seek(SeekFrom::End(0))
            .map_err(|error| Error::storage(format!("seek embedding cache vectors: {error}")))?;
        let mut index_records = Vec::with_capacity(entries.len() * INDEX_ENTRY_BYTES);
        let mut offset = start;
        for (key, vector) in entries {
            if vector.len() != self.identity.dimensions
                || vector.iter().any(|value| !value.is_finite())
            {
                return Err(Error::invalid("cannot cache an invalid embedding vector"));
            }
            let mut record = Vec::with_capacity(self.record_size);
            record.extend_from_slice(CACHE_RECORD_MAGIC);
            record.extend_from_slice(key);
            record.extend_from_slice(&(self.identity.dimensions as u32).to_le_bytes());
            for value in vector {
                record.extend_from_slice(&value.to_le_bytes());
            }
            record.extend_from_slice(&Sha256::digest(&record));
            if record.len() != self.record_size {
                return Err(Error::invalid("embedding cache record size mismatch"));
            }
            self.vectors.write_all(&record).map_err(|error| {
                Error::storage(format!("append embedding cache vector: {error}"))
            })?;
            index_records.extend_from_slice(key);
            index_records.extend_from_slice(&offset.to_le_bytes());
            self.entries.insert(*key, offset);
            offset += self.record_size as u64;
        }
        self.vectors
            .sync_data()
            .map_err(|error| Error::storage(format!("sync embedding cache vectors: {error}")))?;
        self.index
            .write_all(&index_records)
            .and_then(|_| self.index.sync_data())
            .map_err(|error| Error::storage(format!("publish embedding cache index: {error}")))
    }
}

fn acquire_lock(path: &Path) -> Result<CacheLock, Error> {
    for attempt in 0..2 {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                writeln!(file, "{}", std::process::id())
                    .and_then(|_| file.sync_all())
                    .map_err(|error| {
                        Error::storage(format!("write embedding cache lock: {error}"))
                    })?;
                return Ok(CacheLock {
                    path: path.to_path_buf(),
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && attempt == 0 => {
                let owner = fs::read_to_string(path)
                    .ok()
                    .and_then(|text| text.trim().parse::<u32>().ok());
                let live =
                    owner.is_some_and(|pid| Path::new("/proc").join(pid.to_string()).exists());
                if live {
                    return Err(Error::storage("embedding reuse cache is already in use"));
                }
                let _ = fs::remove_file(path);
            }
            Err(error) => {
                return Err(Error::storage(format!(
                    "acquire embedding cache lock: {error}"
                )));
            }
        }
    }
    Err(Error::storage(
        "could not acquire embedding reuse cache lock",
    ))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::storage("embedding cache identity has no parent"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("identity.json");
    let temporary = parent.join(format!(".{file_name}.tmp-{}", new_run_id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| Error::storage(format!("create embedding cache identity: {error}")))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::storage(format!("write embedding cache identity: {error}")))?;
    drop(file);
    fs::rename(&temporary, path)
        .map_err(|error| Error::storage(format!("publish embedding cache identity: {error}")))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::storage(format!("sync embedding cache directory: {error}")))
}

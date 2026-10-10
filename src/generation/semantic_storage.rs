use crate::{CatalogRecord, Error, SEMANTIC_MAGIC, SemanticHit, StoredSemanticIndex};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;

pub(crate) fn read_semantic_index(path: &Path) -> Result<StoredSemanticIndex, Error> {
    let bytes = fs::read(path)
        .map_err(|e| Error::storage(format!("read semantic artifact {}: {e}", path.display())))?;
    read_semantic_bytes(&bytes)
}

pub(super) fn read_semantic_bytes(bytes: &[u8]) -> Result<StoredSemanticIndex, Error> {
    if !bytes.starts_with(SEMANTIC_MAGIC) {
        return Err(Error::invalid("semantic artifact magic mismatch"));
    }
    let mut cursor = SEMANTIC_MAGIC.len();
    let dimensions = usize::try_from(read_semantic_u32(bytes, &mut cursor)?)
        .map_err(|_| Error::invalid("semantic dimensions overflow"))?;
    let count = usize::try_from(read_semantic_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::invalid("semantic vector count overflow"))?;
    if dimensions == 0 || dimensions > 4096 {
        return Err(Error::invalid(
            "semantic dimensions outside supported bound",
        ));
    }
    let vector_bytes = dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| Error::invalid("semantic record size overflow"))?;
    let record_size = 8_usize
        .checked_add(vector_bytes)
        .ok_or_else(|| Error::invalid("semantic record size overflow"))?;
    let expected_len = cursor
        .checked_add(
            count
                .checked_mul(record_size)
                .ok_or_else(|| Error::invalid("semantic artifact size overflow"))?,
        )
        .ok_or_else(|| Error::invalid("semantic artifact size overflow"))?;
    if expected_len != bytes.len() {
        return Err(Error::invalid("semantic artifact length mismatch"));
    }
    let mut vectors = Vec::with_capacity(count);
    for _ in 0..count {
        let key = read_semantic_u64(bytes, &mut cursor)?;
        let mut vector = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            let raw = read_semantic_slice(bytes, &mut cursor, std::mem::size_of::<f32>())?;
            vector.push(f32::from_le_bytes(raw.try_into().expect("float width")));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(Error::invalid(
                "semantic artifact contains non-finite vector",
            ));
        }
        vectors.push((key, vector));
    }
    Ok(StoredSemanticIndex {
        dimensions,
        vectors,
    })
}

fn read_semantic_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        read_semantic_slice(bytes, cursor, std::mem::size_of::<u32>())?
            .try_into()
            .expect("integer width"),
    ))
}

fn read_semantic_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, Error> {
    Ok(u64::from_le_bytes(
        read_semantic_slice(bytes, cursor, std::mem::size_of::<u64>())?
            .try_into()
            .expect("integer width"),
    ))
}

fn read_semantic_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], Error> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| Error::invalid("semantic cursor overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::invalid("truncated semantic artifact"))?;
    *cursor = end;
    Ok(slice)
}

pub(super) fn validate_semantic_index(
    index: &StoredSemanticIndex,
    catalog: &[CatalogRecord],
    dimensions: usize,
) -> Result<(), Error> {
    if index.dimensions != dimensions || index.vectors.len() != catalog.len() {
        return Err(Error::invalid("semantic dimensions/count mismatch"));
    }
    let expected = catalog
        .iter()
        .map(|record| record.vector_key)
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for (key, vector) in &index.vectors {
        if !actual.insert(*key) {
            return Err(Error::invalid("duplicate semantic vector key"));
        }
        if vector.len() != dimensions || !expected.contains(key) {
            return Err(Error::invalid(
                "semantic vector key does not map to catalog",
            ));
        }
    }
    if actual != expected {
        return Err(Error::invalid(
            "semantic vector keys do not equal catalog keys",
        ));
    }
    Ok(())
}

pub(super) fn validate_semantic_file(
    path: &Path,
    expected_keys: &mut BTreeSet<u64>,
    expected_dimensions: usize,
) -> Result<usize, Error> {
    // Caller passes the catalog's already-validated 1..=N key set.
    let expected_count = expected_keys.len();
    let maximum_key = u64::try_from(expected_count)
        .map_err(|_| Error::invalid("semantic vector count overflow"))?;
    let file = File::open(path)
        .map_err(|e| Error::invalid(format!("read semantic vector artifact: {e}")))?;
    let file_len = file
        .metadata()
        .map_err(|e| Error::invalid(format!("stat semantic vector artifact: {e}")))?
        .len();
    let mut reader = BufReader::new(file);
    let mut magic = [0; SEMANTIC_MAGIC.len()];
    read_semantic_file_exact(&mut reader, &mut magic)?;
    if magic.as_slice() != SEMANTIC_MAGIC {
        return Err(Error::invalid("semantic artifact magic mismatch"));
    }

    let dimensions = usize::try_from(read_semantic_file_u32(&mut reader)?)
        .map_err(|_| Error::invalid("semantic dimensions overflow"))?;
    let count_u64 = read_semantic_file_u64(&mut reader)?;
    let count =
        usize::try_from(count_u64).map_err(|_| Error::invalid("semantic vector count overflow"))?;
    if dimensions == 0 || dimensions > 4096 {
        return Err(Error::invalid(
            "semantic dimensions outside supported bound",
        ));
    }
    if dimensions != expected_dimensions || count != expected_count {
        return Err(Error::invalid("semantic dimensions/count mismatch"));
    }
    let vector_bytes = dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| Error::invalid("semantic record size overflow"))?;
    let record_size = 8_usize
        .checked_add(vector_bytes)
        .ok_or_else(|| Error::invalid("semantic record size overflow"))?;
    let expected_len = SEMANTIC_MAGIC
        .len()
        .checked_add(12)
        .and_then(|header_len| count.checked_mul(record_size)?.checked_add(header_len))
        .ok_or_else(|| Error::invalid("semantic artifact size overflow"))?;
    if u64::try_from(expected_len).ok() != Some(file_len) {
        return Err(Error::invalid("semantic artifact length mismatch"));
    }

    let mut key_error = None;
    let mut vector = vec![0; vector_bytes];
    // Remove each valid key as seen; repeats fail immediately and leftovers are missing keys.
    for _ in 0..count {
        let key = read_semantic_file_u64(&mut reader)?;
        if key == 0 || key > maximum_key {
            key_error.get_or_insert("semantic vector key does not map to catalog");
        } else if !expected_keys.remove(&key) {
            key_error.get_or_insert("duplicate semantic vector key");
        }
        read_semantic_file_exact(&mut reader, &mut vector)?;
        for raw in vector.chunks_exact(std::mem::size_of::<f32>()) {
            if !f32::from_le_bytes(raw.try_into().expect("float width")).is_finite() {
                return Err(Error::invalid(
                    "semantic artifact contains non-finite vector",
                ));
            }
        }
    }
    let mut trailing = [0; 1];
    if reader
        .read(&mut trailing)
        .map_err(|e| Error::invalid(format!("read semantic vector artifact: {e}")))?
        != 0
    {
        return Err(Error::invalid("semantic artifact length mismatch"));
    }

    if let Some(message) = key_error {
        return Err(Error::invalid(message));
    }
    if !expected_keys.is_empty() {
        return Err(Error::invalid(
            "semantic vector keys do not equal catalog keys",
        ));
    }
    Ok(count)
}

fn read_semantic_file_exact(reader: &mut impl Read, bytes: &mut [u8]) -> Result<(), Error> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::invalid("truncated semantic artifact")
        } else {
            Error::invalid(format!("read semantic vector artifact: {error}"))
        }
    })
}

fn read_semantic_file_u32(reader: &mut impl Read) -> Result<u32, Error> {
    let mut raw = [0; std::mem::size_of::<u32>()];
    read_semantic_file_exact(reader, &mut raw)?;
    Ok(u32::from_le_bytes(raw))
}

fn read_semantic_file_u64(reader: &mut impl Read) -> Result<u64, Error> {
    let mut raw = [0; std::mem::size_of::<u64>()];
    read_semantic_file_exact(reader, &mut raw)?;
    Ok(u64::from_le_bytes(raw))
}

pub(super) fn semantic_hits(
    index: &StoredSemanticIndex,
    catalog: &[CatalogRecord],
    query: &[f32],
    candidate_depth: usize,
) -> Result<Vec<SemanticHit>, Error> {
    let catalog_by_key = catalog
        .iter()
        .map(|record| (record.vector_key, record))
        .collect::<BTreeMap<_, _>>();
    let mut scored = index
        .vectors
        .iter()
        .map(|(key, vector)| {
            let score = vector
                .iter()
                .zip(query)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            (*key, score)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
        .into_iter()
        .take(candidate_depth)
        .map(|(vector_key, score)| {
            let record = catalog_by_key
                .get(&vector_key)
                .ok_or_else(|| Error::invalid("semantic candidate missing from catalog"))?;
            Ok(SemanticHit {
                vector_key,
                page_id: record.page_id.clone(),
                passage_id: record.passage_id.clone(),
                score,
            })
        })
        .collect()
}

use super::{
    Deserialize, Digest, Error, OpenOptions, Path, Read, SecondsFormat, Serialize, Sha256,
    SystemTime, UNIX_EPOCH, Utc, Write, fs, io,
};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn new_run_id() -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "run-{}",
        sha256_hex(format!("{stamp}:{}", std::process::id()).as_bytes())
    )
}

pub(crate) fn write_if_absent(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| Error::storage(format!("create {}: {e}", parent.display())))?;
    }
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => file
            .write_all(bytes)
            .map_err(|e| Error::storage(format!("write {}: {e}", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(Error::storage(format!("create {}: {e}", path.display()))),
    }
}

pub(crate) fn read_bounded(
    response: reqwest::blocking::Response,
    limit: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let mut body = Vec::new();
    response
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut body)
        .map_err(|e| Error::fetch(format!("read response body: {e}")))?;
    if body.len() > limit {
        Ok(None)
    } else {
        Ok(Some(body))
    }
}

pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<String, Error> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|e| Error::storage(format!("serialize {}: {e}", path.display())))?;
    write_if_absent(path, &bytes)?;
    Ok(sha256_hex(&bytes))
}

pub(crate) fn jsonl<T: Serialize>(values: &[T]) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend(
            serde_json::to_vec(value)
                .map_err(|e| Error::storage(format!("serialize JSONL: {e}")))?,
        );
        bytes.push(b'\n');
    }
    Ok(bytes)
}

pub(crate) fn parse_jsonl<T>(bytes: &[u8], name: &str) -> Result<Vec<T>, Error>
where
    T: for<'de> Deserialize<'de>,
{
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_slice(line).map_err(|e| Error::invalid(format!("parse {name}: {e}")))
        })
        .collect()
}

pub(crate) fn normalize_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn trim_snippet(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.into();
    }
    let end = text
        .char_indices()
        .take_while(|(index, _)| *index < limit)
        .last()
        .map(|(index, character)| index + character.len_utf8())
        .unwrap_or(0);
    text[..end].trim_end().into()
}

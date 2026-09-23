use super::{Error, Url};

pub(crate) fn normalize_url(mut url: Url) -> Result<String, String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err("non_http_url".into());
    }
    if url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() {
        return Err("url_missing_host_or_credentials".into());
    }
    if matches!(
        (url.scheme(), url.port()),
        ("http", Some(80)) | ("https", Some(443))
    ) {
        let _ = url.set_port(None);
    }
    url.set_fragment(None);
    Ok(url.to_string())
}

pub(crate) fn normalize_origin(value: &str) -> Result<String, Error> {
    let url = Url::parse(value)
        .map_err(|e| Error::invalid(format!("invalid allowed origin {value:?}: {e}")))?;
    if !url.path().is_empty() && url.path() != "/" {
        return Err(Error::invalid("allowed origin must not contain a path"));
    }
    let normalized = normalize_url(url).map_err(Error::invalid)?;
    Ok(origin_key(
        &Url::parse(&normalized).expect("normalized origin"),
    ))
}

pub(crate) fn origin_key(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.into()
    };
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

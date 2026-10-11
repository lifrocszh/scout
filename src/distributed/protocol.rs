use super::PROTOCOL_VERSION;
use ::url::Url;
use tonic::Status;

pub(super) fn check_protocol(version: u32) -> Result<(), Status> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "unsupported protocol version {version}; server supports {PROTOCOL_VERSION}"
        )))
    }
}

pub(super) fn validate_identity(value: &str, label: &str) -> Result<(), Status> {
    if value.trim().is_empty()
        || value.len() > 256
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        Err(Status::invalid_argument(format!("invalid {label}")))
    } else {
        Ok(())
    }
}

pub(super) fn safe_event_url(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return "[invalid-url]".to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

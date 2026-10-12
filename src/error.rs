use std::string::String;

#[derive(Debug, Clone)]
pub(crate) struct Error {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) exit_code: i32,
}

impl Error {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_configuration".into(),
            message: message.into(),
            exit_code: 2,
        }
    }

    pub(crate) fn fetch(message: impl Into<String>) -> Self {
        Self {
            code: "fetch_failed".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    pub(crate) fn storage(message: impl Into<String>) -> Self {
        Self {
            code: "storage_failed".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    pub(crate) fn safety(message: impl Into<String>) -> Self {
        Self {
            code: "safety_cap".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    pub(crate) fn stale_lease(message: impl Into<String>) -> Self {
        Self {
            code: "stale_lease".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    pub(crate) fn not_ready(message: impl Into<String>) -> Self {
        Self {
            code: "not_ready".into(),
            message: message.into(),
            exit_code: 3,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RobotsPolicy {
    pub(crate) groups: Vec<RobotsGroup>,
    pub(crate) sitemaps: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RobotsGroup {
    pub(crate) agents: Vec<String>,
    pub(crate) rules: Vec<RobotsRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RobotsRule {
    pub(crate) allow: bool,
    pub(crate) path: String,
}

impl RobotsPolicy {
    pub(crate) fn allows(&self, path: &str) -> bool {
        let has_specific_group = self
            .groups
            .iter()
            .any(|group| group.agents.iter().any(|agent| agent == "scout"));
        let mut rules = self
            .groups
            .iter()
            .filter(|group| {
                group.agents.iter().any(|agent| {
                    if has_specific_group {
                        agent == "scout"
                    } else {
                        agent == "*"
                    }
                })
            })
            .flat_map(|group| group.rules.iter())
            .filter(|rule| !rule.path.is_empty() && path.starts_with(&rule.path))
            .collect::<Vec<_>>();
        rules.sort_by(|left, right| {
            right
                .path
                .len()
                .cmp(&left.path.len())
                .then_with(|| right.allow.cmp(&left.allow))
        });
        rules.first().map(|rule| rule.allow).unwrap_or(true)
    }
}

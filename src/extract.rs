use super::{
    BTreeSet, ContentBlock, ElementRef, Extracted, Html, Selector, Url, storage::sha256_hex,
    url::normalize_url,
};

pub(crate) fn extract(html: &str, page_url: &Url, minimum: usize) -> Result<Extracted, String> {
    let document = Html::parse_document(html);
    let main_selector = Selector::parse("main, [role='main']").expect("valid selector");
    let mains = document
        .select(&main_selector)
        .filter(|element| !hidden(*element))
        .collect::<Vec<_>>();
    let readability_selector = Selector::parse(
        "article, [role='article'], .article, .content, .main-content, #content, .document",
    )
    .expect("valid selector");
    let readability_scope = document
        .select(&readability_selector)
        .filter(|element| !hidden(*element))
        .max_by_key(|element| useful_characters(&collect_blocks(*element)));
    let mut candidates = Vec::new();
    if mains.len() == 1 {
        candidates.push((mains[0], "semantic_main"));
    }
    if let Some(scope) = readability_scope {
        candidates.push((scope, "readability"));
    }
    candidates.push((document.root_element(), "cleaned_body"));

    let (title, description, language, canonical_url, base_url) =
        document_metadata(&document, page_url);
    for (scope, extraction_method) in candidates {
        let blocks = collect_blocks(scope);
        if useful_characters(&blocks) < minimum {
            continue;
        }
        let (identity, content) = block_representation(&blocks);
        let link_selector = Selector::parse("a[href]").expect("valid selector");
        let mut links = BTreeSet::new();
        for element in scope.select(&link_selector) {
            if hidden(element) {
                continue;
            }
            let Some(href) = element.attr("href") else {
                continue;
            };
            if let Some(url) = base_url
                .join(href)
                .ok()
                .and_then(|url| normalize_url(url).ok())
            {
                links.insert(url);
            }
        }
        let title = if title.is_empty() {
            blocks
                .iter()
                .find_map(|block| match block {
                    ContentBlock::Heading { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default()
        } else {
            title.clone()
        };
        return Ok(Extracted {
            title,
            description: description.clone(),
            language: language.clone(),
            canonical_url: canonical_url.clone(),
            extraction_method,
            content,
            identity: identity.clone(),
            content_hash: sha256_hex(identity.as_bytes()),
            blocks,
            links: links.into_iter().collect(),
        });
    }
    Err("useful_content_below_minimum".into())
}

fn document_metadata(
    document: &Html,
    page_url: &Url,
) -> (String, Option<String>, Option<String>, Option<String>, Url) {
    let title_selector = Selector::parse("title").expect("valid selector");
    let title = document
        .select(&title_selector)
        .map(|element| clean(&element.text().collect::<Vec<_>>().join(" ")))
        .find(|title| !title.is_empty())
        .unwrap_or_default();
    let meta_selector = Selector::parse("meta").expect("valid selector");
    let description = document.select(&meta_selector).find_map(|element| {
        let name = element
            .attr("name")
            .or_else(|| element.attr("property"))
            .unwrap_or_default();
        if name.eq_ignore_ascii_case("description") || name.eq_ignore_ascii_case("og:description") {
            element
                .attr("content")
                .map(clean)
                .filter(|value| !value.is_empty())
        } else {
            None
        }
    });
    let language = document
        .root_element()
        .attr("lang")
        .map(clean)
        .filter(|value| reliable_language(value));
    let base_url = Selector::parse("base[href]")
        .ok()
        .and_then(|selector| document.select(&selector).next())
        .and_then(|element| element.attr("href"))
        .and_then(|href| page_url.join(href).ok())
        .unwrap_or_else(|| page_url.clone());
    let canonical_url = Selector::parse("link[href]").ok().and_then(|selector| {
        document.select(&selector).find_map(|element| {
            let canonical = element.attr("rel").is_some_and(|rel| {
                rel.split_whitespace()
                    .any(|value| value.eq_ignore_ascii_case("canonical"))
            });
            canonical
                .then(|| element.attr("href"))
                .flatten()
                .and_then(|href| base_url.join(href).ok())
                .and_then(|url| normalize_url(url).ok())
        })
    });
    (title, description, language, canonical_url, base_url)
}

fn reliable_language(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(primary) = parts.next() else {
        return false;
    };
    (2..=8).contains(&primary.len())
        && primary
            .chars()
            .all(|character| character.is_ascii_alphabetic())
        && parts.all(|part| {
            !part.is_empty()
                && part.len() <= 8
                && part
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
        })
}

fn collect_blocks(scope: ElementRef<'_>) -> Vec<ContentBlock> {
    let block_selector = Selector::parse("h1,h2,h3,h4,h5,h6,p,pre,li,blockquote,dt,dd,td,th")
        .expect("valid selector");
    let mut blocks = Vec::new();
    for element in scope.select(&block_selector) {
        if hidden(element) || block_ancestor(element) {
            continue;
        }
        let tag = element.value().name();
        let raw = element
            .text()
            .collect::<Vec<_>>()
            .join(if tag == "pre" { "" } else { " " });
        let text = if tag == "pre" {
            clean_code(&raw)
        } else {
            clean(&raw)
        };
        if text.is_empty() {
            continue;
        }
        if let Some(level) = tag.strip_prefix('h').and_then(|value| value.parse().ok()) {
            blocks.push(ContentBlock::Heading { level, text });
        } else if tag == "pre" {
            blocks.push(ContentBlock::Code(text));
        } else {
            blocks.push(ContentBlock::Prose(text));
        }
    }
    blocks
}

fn useful_characters(blocks: &[ContentBlock]) -> usize {
    blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Prose(text) | ContentBlock::Code(text) => Some(text),
            ContentBlock::Heading { .. } => None,
        })
        .map(|text| {
            text.chars()
                .filter(|character| !character.is_whitespace())
                .count()
        })
        .sum()
}

fn block_representation(blocks: &[ContentBlock]) -> (String, String) {
    let identity = blocks
        .iter()
        .map(|block| match block {
            ContentBlock::Heading { level, text } => format!("heading:{level}:{text}"),
            ContentBlock::Prose(text) => format!("prose:{text}"),
            ContentBlock::Code(text) => format!("code:{text}"),
        })
        .collect::<Vec<_>>()
        .join("\n");
    let content = blocks
        .iter()
        .map(|block| match block {
            ContentBlock::Heading { text, .. }
            | ContentBlock::Prose(text)
            | ContentBlock::Code(text) => text.as_str(),
        })
        .collect::<Vec<_>>()
        .join("\n");
    (identity, content)
}

fn block_ancestor(element: ElementRef<'_>) -> bool {
    const BLOCKS: &[&str] = &[
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "p",
        "pre",
        "li",
        "blockquote",
        "dt",
        "dd",
        "td",
        "th",
    ];
    element
        .ancestors()
        .skip(1)
        .filter_map(ElementRef::wrap)
        .any(|ancestor| BLOCKS.contains(&ancestor.value().name()))
}

fn hidden(element: ElementRef<'_>) -> bool {
    const EXCLUDED: &[&str] = &[
        "head", "script", "style", "noscript", "template", "nav", "footer", "aside",
    ];
    element
        .ancestors()
        .filter_map(ElementRef::wrap)
        .any(|ancestor| {
            let tag = ancestor.value().name();
            EXCLUDED.contains(&tag)
                || ancestor.attr("hidden").is_some()
                || ancestor
                    .attr("aria-hidden")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                || ancestor.attr("style").is_some_and(|value| {
                    let value = value.to_ascii_lowercase().replace(' ', "");
                    value.contains("display:none") || value.contains("visibility:hidden")
                })
        })
}

fn clean(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn clean_code(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .trim()
        .into()
}

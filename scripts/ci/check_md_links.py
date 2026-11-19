#!/usr/bin/env python3
"""Fail on a relative Markdown link whose file or #heading does not exist.

Checks the tracked *.md files (`git ls-files`, so ignored local run output does not count). External
links (a URL scheme) are not fetched; this is a check for links inside the repository. Links in code
blocks and code spans are skipped.

Usage: scripts/ci/check_md_links.py [file.md ...]
"""

import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]

FENCE = re.compile(r"^ {0,3}(```|~~~)")
CODE_SPAN = re.compile(r"(`+)(.+?)\1")
# [text](target "title"), ![alt](target) and a reference definition `[ref]: target`.
INLINE_LINK = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
REFERENCE_DEFINITION = re.compile(r"^ {0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$")
HEADING = re.compile(r"^ {0,3}#{1,6}\s+(.*?)\s*#*\s*$")
SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def prose_lines(text):
    """(number, line) for the lines of `text` outside fenced code blocks."""
    fence = None
    for number, line in enumerate(text.splitlines(), 1):
        marker = FENCE.match(line)
        if fence:
            if marker and marker.group(1) == fence:
                fence = None
            continue
        if marker:
            fence = marker.group(1)
            continue
        yield number, line


def slug(heading):
    """GitHub's anchor for a heading: lowercase, no punctuation, spaces to hyphens."""
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # a link keeps its text
    text = re.sub(r"[`*]", "", text).strip().lower()
    text = "".join(
        ch for ch in text if ch in " -_" or unicodedata.category(ch)[0] in "LN" or ch == "‍"
    )
    return text.replace(" ", "-")


_ANCHORS = {}


def anchors(path):
    """The anchors of the headings in `path` (memoized)."""
    if path not in _ANCHORS:
        found, seen = set(), {}
        for _, line in prose_lines(path.read_text(encoding="utf-8")):
            heading = HEADING.match(line)
            if heading:
                base = slug(heading.group(1))
                count = seen.get(base, 0)
                seen[base] = count + 1
                found.add(base if count == 0 else f"{base}-{count}")
        _ANCHORS[path] = found
    return _ANCHORS[path]


def problems(path):
    """(line, target, reason) for each broken relative link in the Markdown file `path`."""
    for number, line in prose_lines(path.read_text(encoding="utf-8")):
        line = CODE_SPAN.sub(lambda m: " " * len(m.group(0)), line)  # `[x](y)` is code, not a link
        targets = INLINE_LINK.findall(line)
        definition = REFERENCE_DEFINITION.match(line)
        if definition:
            targets.append(definition.group(1))
        for target in targets:
            if SCHEME.match(target) or target.startswith("//"):
                continue
            file_part, _, fragment = target.partition("#")
            destination = (path.parent / unquote(file_part)).resolve() if file_part else path
            if file_part and not destination.exists():
                yield number, target, "no such file"
            elif fragment and destination.suffix == ".md" and destination.is_file():
                if unquote(fragment).lower() not in anchors(destination):
                    yield number, target, "no such heading"


def tracked_markdown():
    listing = subprocess.run(
        ["git", "ls-files", "*.md"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    return [ROOT / name for name in listing.splitlines()]


def main(argv):
    files = [Path(name).resolve() for name in argv] or tracked_markdown()
    broken = 0
    for path in files:
        for number, target, reason in problems(path):
            print(f"{path.relative_to(ROOT)}:{number}: {reason}: {target}")
            broken += 1
    if broken:
        print(f"{broken} broken link(s) in {len(files)} Markdown file(s)", file=sys.stderr)
        return 1
    print(f"markdown links: {len(files)} files, no broken relative links")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

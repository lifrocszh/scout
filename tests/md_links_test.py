"""Tests for scripts/ci/check_md_links.py (the Markdown link check)."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ci" / "check_md_links.py"
spec = importlib.util.spec_from_file_location("check_md_links", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class LinkCheckTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "sub").mkdir()
        (self.root / "other.md").write_text("# Other\n\n## Two words, `code`\n\n## Repeat\n\n## Repeat\n")

    def broken(self, text, name="doc.md"):
        path = self.root / name
        path.write_text(text)
        return [(target, reason) for _, target, reason in checker.problems(path)]

    def test_existing_files_and_headings_pass(self):
        text = (
            "[a](other.md) [b](./other.md#other) [c](other.md#two-words-code) [d](other.md#repeat-1)\n"
            "[e](sub/) [f](#top) [g](other.md \"title\") ![h](other.md)\n\n# Top\n"
        )
        self.assertEqual(self.broken(text), [])

    def test_missing_file_and_heading_are_reported(self):
        text = "[a](nope.md) [b](other.md#nope) [c](#nope) [d](sub/missing/)\n"
        self.assertEqual(
            self.broken(text),
            [
                ("nope.md", "no such file"),
                ("other.md#nope", "no such heading"),
                ("#nope", "no such heading"),
                ("sub/missing/", "no such file"),
            ],
        )

    def test_relative_to_the_linking_file(self):
        self.assertEqual(self.broken("[a](../other.md) [b](sub/x.md)\n", "sub/doc.md"), [("sub/x.md", "no such file")])

    def test_urls_code_and_fences_are_not_links(self):
        text = (
            "[a](https://example.com/x.md) [b](mailto:a@b.c) [c](//example.com/x)\n"
            "`[d](nope.md)` and ``[e](nope.md)``\n"
            "```\n[f](nope.md)\n```\n"
            "~~~sh\n[g](nope.md)\n~~~\n"
        )
        self.assertEqual(self.broken(text), [])

    def test_reference_definitions_are_checked(self):
        self.assertEqual(self.broken("[a][ref]\n\n[ref]: nope.md\n[ok]: other.md\n"), [("nope.md", "no such file")])

    def test_slug_follows_github(self):
        for heading, want in [
            ("2. Postgres `SKIP LOCKED` instead of a broker", "2-postgres-skip-locked-instead-of-a-broker"),
            ("Kafka ingestion log (optional)", "kafka-ingestion-log-optional"),
            ("Where to look", "where-to-look"),
            ("[Link text](x.md) and **bold**", "link-text-and-bold"),
            ("Crash/restart: what's next?", "crashrestart-whats-next"),
        ]:
            self.assertEqual(checker.slug(heading), want)


if __name__ == "__main__":
    unittest.main()

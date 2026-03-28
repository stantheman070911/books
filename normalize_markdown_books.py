#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable


CHUNK_SIZE = 300

CHAR_REPLACEMENTS = {
    "\ufeff": "",
    "\xa0": " ",
    "â€™": "’",
    "â€˜": "‘",
    "â€œ": "“",
    "â€": "”",
    "â€“": "–",
    "â€”": "—",
    "â€¦": "…",
    "â€": "\"",
    "ﬁ": "fi",
    "ﬂ": "fl",
    "ﬀ": "ff",
    "ﬃ": "ffi",
    "ﬄ": "ffl",
}

MAJOR_HEADING_PREFIXES = (
    "chapter ",
    "book ",
    "part ",
    "contents",
    "preface",
    "foreword",
    "introduction",
    "conclusion",
    "appendix",
    "epilogue",
    "prologue",
    "timeline",
    "about the author",
    "acknowledgments",
    "acknowledgements",
    "important notes",
    "notes on this book",
    "roadmap stages",
)

BYLINE_PREFIXES = (
    "by ",
    "foreword by ",
    "visuals by ",
    "translated by ",
    "edited by ",
)

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "being",
    "but",
    "by",
    "can",
    "did",
    "do",
    "does",
    "for",
    "from",
    "had",
    "has",
    "have",
    "he",
    "her",
    "hers",
    "him",
    "his",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "me",
    "more",
    "my",
    "no",
    "not",
    "of",
    "on",
    "one",
    "or",
    "our",
    "out",
    "she",
    "so",
    "than",
    "that",
    "the",
    "their",
    "them",
    "there",
    "they",
    "this",
    "those",
    "through",
    "to",
    "up",
    "us",
    "was",
    "we",
    "were",
    "what",
    "when",
    "which",
    "who",
    "why",
    "will",
    "with",
    "you",
    "your",
}

TITLECASE_SMALL_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "but",
    "by",
    "for",
    "from",
    "if",
    "in",
    "nor",
    "of",
    "on",
    "or",
    "per",
    "the",
    "to",
    "vs.",
    "via",
    "with",
}

LIST_ITEM_RE = re.compile(
    r"^(?P<indent>\s*)(?P<marker>(?:[-+*•])|(?:\d{1,3}[.)])|(?:[A-Za-z][.)]))\s+(?P<body>.*)$"
)
ORDERED_MARKER_RE = re.compile(r"(?<!\S)(?:\d{1,3}[.)]|[A-Za-z][.)])\s+")
PAGE_RE = re.compile(r"^\s*page\s+(?:\d+|[ivxlcdm]+)\s*$", re.IGNORECASE)
HEADING_RE = re.compile(r"^(?P<level>#{1,6})\s*(?P<text>.*?)\s*$")
URL_JOIN_TAIL_RE = re.compile(r"(?:https?://|www\.|\w+\.)[\w./?&=#:%+-]*$", re.IGNORECASE)


@dataclass
class Block:
    kind: str
    text: str
    level: int | None = None
    explicit: bool = False


@dataclass
class FileSummary:
    path: str
    changed: bool
    input_lines: int
    output_lines: int
    blocks: int
    headings: int
    removed_repeats: int
    heading_skips: int


def iter_file_chunks(path: Path, chunk_size: int) -> Iterable[list[str]]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        chunk: list[str] = []
        for raw_line in handle:
            chunk.append(raw_line.rstrip("\n"))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


@lru_cache(maxsize=1)
def load_lexicon() -> set[str]:
    lexicon: set[str] = set()
    for dictionary_path in (
        "/usr/share/dict/words",
        "/usr/share/dict/web2",
        "/usr/share/dict/web2a",
        "/usr/share/dict/propernames",
    ):
        if not os.path.exists(dictionary_path):
            continue
        with open(dictionary_path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                word = line.strip()
                if not word:
                    continue
                lexicon.add(word.lower())
    lexicon.update(
        {
            "bitcoin",
            "cyberspace",
            "epinions",
            "linkedin",
            "naval",
            "neuralink",
            "palantir",
            "paypal",
            "spacex",
            "startupboy",
            "substack",
            "tesla",
            "twitter",
            "youtube",
            "isbn",
            "roadmap",
            "microprocessing",
            "incerto",
            "angelist",
        }
    )
    return lexicon


def replace_characters(text: str) -> str:
    for bad, good in CHAR_REPLACEMENTS.items():
        text = text.replace(bad, good)
    return unicodedata.normalize("NFC", text)


def normalize_line(raw_line: str) -> str:
    line = replace_characters(raw_line)
    line = line.replace("\t", "  ")
    line = re.sub(
        r"^\s*((?:#\s*){2,6})(?=\S)",
        lambda match: "#" * match.group(1).count("#") + " ",
        line,
    )
    line = re.sub(r"^(#{1,6})(\S)", r"\1 \2", line)
    line = re.sub(r"\s+$", "", line)
    return line


def strip_markdown_prefix(line: str) -> str:
    line = re.sub(r"^\s*#{1,6}\s*", "", line)
    line = re.sub(r"^\s*(?:[-+*•]|\d{1,3}[.)]|[A-Za-z][.)])\s*", "", line)
    return line.strip()


def repeat_key(line: str) -> str | None:
    text = strip_markdown_prefix(line)
    if not text:
        return None
    if len(text) > 80:
        return None
    if len(text.split()) > 10:
        return None
    if re.search(r"[.!?]$", text):
        return None
    key = text.lower()
    key = re.sub(r"\b\d+\b", "<n>", key)
    key = re.sub(r"\b[ivxlcdm]+\b", "<r>", key)
    key = re.sub(r"\s+", " ", key).strip()
    return key or None


def gather_profile(path: Path, chunk_size: int) -> tuple[Counter[str], str | None, int]:
    repeat_counts: Counter[str] = Counter()
    first_nonblank: str | None = None
    input_lines = 0
    for chunk in iter_file_chunks(path, chunk_size):
        for raw_line in chunk:
            input_lines += 1
            line = normalize_line(raw_line)
            if first_nonblank is None and line.strip():
                first_nonblank = line.strip()
            if PAGE_RE.match(line):
                repeat_counts["__page__"] += 1
                continue
            key = repeat_key(line)
            if key:
                repeat_counts[key] += 1
    return repeat_counts, first_nonblank, input_lines


def is_known_word(word: str) -> bool:
    lexicon = load_lexicon()
    lower = word.lower()
    if lower in lexicon:
        return True
    if lower.endswith("’s") and lower[:-2] in lexicon:
        return True
    for suffix in ("s", "es", "ed", "ing", "ly", "er", "ers", "est", "ness", "ment"):
        if len(lower) <= len(suffix) + 2 or not lower.endswith(suffix):
            continue
        stem = lower[: -len(suffix)]
        if stem in lexicon or (stem + "e") in lexicon:
            return True
    if "-" in lower and all(part in lexicon or part in STOPWORDS for part in lower.split("-")):
        return True
    return False


def preserve_case(template: str, replacement_parts: list[str]) -> str:
    if template.isupper():
        return " ".join(part.upper() for part in replacement_parts)
    if template.istitle():
        rendered: list[str] = []
        for index, part in enumerate(replacement_parts):
            if index == 0 or part not in TITLECASE_SMALL_WORDS:
                rendered.append(part.capitalize())
            else:
                rendered.append(part)
        return " ".join(rendered)
    return " ".join(replacement_parts)


@lru_cache(maxsize=50000)
def split_ocr_token(token: str) -> str:
    if len(token) < 7 or len(token) > 22:
        return token
    if not token.isalpha():
        return token
    if is_known_word(token):
        return token

    lower = token.lower()

    @lru_cache(maxsize=None)
    def helper(rest: str, parts_left: int) -> list[str] | None:
        if is_known_word(rest) and len(rest) >= 2:
            return [rest]
        if parts_left == 1:
            return None
        for index in range(2, len(rest) - 1):
            left = rest[:index]
            right = rest[index:]
            if left in STOPWORDS:
                candidate = helper(right, parts_left - 1)
                if candidate:
                    return [left] + candidate
            if right in STOPWORDS and is_known_word(left):
                return [left, right]
        return None

    segments = helper(lower, 3)
    if not segments or len(segments) == 1:
        return token
    if not (segments[0] in STOPWORDS or segments[-1] in STOPWORDS):
        return token
    return preserve_case(token, segments)


def collapse_hyphen_space(match: re.Match[str]) -> str:
    left = match.group(1)
    right = match.group(2)
    merged = left + right
    if is_known_word(merged):
        return merged
    return f"{left}-{right}"


def fix_url_spacing(text: str) -> str:
    text = re.sub(r"(https?://)\s+", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"(https?://\S+)\s+([A-Za-z0-9/_#?=&%-])", r"\1\2", text)
    text = re.sub(r"(www\.\S+)\s+([A-Za-z0-9/_#?=&%-])", r"\1\2", text)
    text = re.sub(r"((?:https?://|www\.)[A-Za-z0-9.-]+)\.\s+([A-Za-z]{2,6}\b)", r"\1.\2", text)
    text = re.sub(r"((?:https?://|www\.)[^\s]+/)\s+([A-Za-z0-9])", r"\1\2", text)
    return text


def fix_intra_word_spacing(text: str) -> str:
    text = re.sub(r"\b([A-Za-z]{2,})-\s+([A-Za-z]{2,})\b", collapse_hyphen_space, text)
    while True:
        updated = re.sub(
            r"\b([A-Za-z]{2,14})\s+([A-Za-z]{2,14})\b",
            lambda match: (
                match.group(1) + match.group(2)
                if is_known_word(match.group(1) + match.group(2))
                else match.group(0)
            ),
            text,
        )
        if updated == text:
            break
        text = updated
    text = re.sub(r"(?<=[A-Za-z])\.(?=[A-Z])", ". ", text)
    text = re.sub(r"\bU\.\s*S\.\b", "U.S.", text)
    text = re.sub(r"\b([A-Z]\.[A-Z]\.)(?=[A-Za-z])", r"\1 ", text)
    text = re.sub(r"(?<=\d)(?=[A-Za-z]{3,})", " ", text)
    text = re.sub(r"\s+([,.;:?!])", r"\1", text)
    text = re.sub(r"([(\[“‘])\s+", r"\1", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return fix_url_spacing(text)


def should_remove_line(
    line: str,
    repeated_keys: set[str],
    first_nonblank: str | None,
    seen_nonblank: int,
) -> bool:
    if not line.strip():
        return False
    if PAGE_RE.match(line):
        return True
    key = repeat_key(line)
    if not key or key not in repeated_keys:
        return False
    if first_nonblank and seen_nonblank == 0 and line.strip() == first_nonblank:
        return False
    if line.lstrip().startswith("#") and strip_markdown_prefix(line).lower().startswith(
        ("chapter ", "book ", "part ")
    ):
        return False
    return True


def split_compound_list_line(line: str) -> list[str]:
    stripped = line.lstrip()
    indent = line[: len(line) - len(stripped)]
    if stripped.startswith(("• ", "* ", "+ ", "- ")):
        raw_body = stripped[2:]
        if " • " in raw_body:
            parts = [part.strip() for part in raw_body.split(" • ") if part.strip()]
            return [f"{indent}• {part}" for part in parts]
        return [line]
    if not ORDERED_MARKER_RE.match(stripped):
        return [line]
    matches = list(ORDERED_MARKER_RE.finditer(stripped))
    if len(matches) <= 1:
        return [line]
    parts: list[str] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(stripped)
        parts.append(f"{indent}{stripped[start:end].strip()}")
    return parts


def load_filtered_lines(
    path: Path,
    chunk_size: int,
    repeated_keys: set[str],
    first_nonblank: str | None,
) -> tuple[list[str], int]:
    lines: list[str] = []
    removed = 0
    seen_nonblank = 0
    for chunk in iter_file_chunks(path, chunk_size):
        for raw_line in chunk:
            line = normalize_line(raw_line)
            if should_remove_line(line, repeated_keys, first_nonblank, seen_nonblank):
                removed += 1
                continue
            if line.strip():
                seen_nonblank += 1
            for expanded in split_compound_list_line(line):
                lines.append(expanded)
    return lines, removed


def is_all_caps(text: str) -> bool:
    letters = [char for char in text if char.isalpha()]
    return bool(letters) and all(char.isupper() for char in letters)


def is_title_like(text: str) -> bool:
    words = re.findall(r"[A-Za-z0-9’'-]+", text)
    if not words or len(words) > 12:
        return False
    titleish = 0
    for word in words:
        lower = word.lower()
        if lower in TITLECASE_SMALL_WORDS:
            titleish += 1
        elif word[:1].isupper() or word.isupper() or word[0].isdigit():
            titleish += 1
    return titleish / len(words) >= 0.7


def heading_from_line(line: str) -> tuple[int | None, str, bool]:
    match = HEADING_RE.match(line.strip())
    if match:
        text = match.group("text").strip()
        return len(match.group("level")), text, True
    return None, line.strip(), False


def is_heading_line(line: str) -> tuple[bool, int | None, str, bool]:
    level, text, explicit = heading_from_line(line)
    if not text:
        return False, None, "", explicit
    lower = text.lower()
    if lower.startswith(BYLINE_PREFIXES):
        return False, None, text, explicit
    if lower.startswith("isbn:"):
        return False, None, text, explicit
    if "|" in text:
        return False, None, text, explicit
    if re.search(r",\s*(?:[a-z]{1,3}\.)?\d", text, flags=re.IGNORECASE):
        return False, None, text, False
    if len(text) > 120:
        return False, None, text, explicit
    word_count = len(text.split())
    if explicit:
        if len(text) <= 3 and text.isupper():
            return False, None, text, False
        if text.startswith(("“", "\"", "'")):
            return False, None, text, False
        return True, level, text, True
    if lower.startswith(MAJOR_HEADING_PREFIXES):
        return True, 2, text, False
    if text.endswith("."):
        return False, None, text, False
    if word_count <= 12 and (is_title_like(text) or is_all_caps(text)):
        return True, None, text, False
    return False, None, text, False


def is_code_block(lines: list[str]) -> bool:
    if len(lines) < 2:
        return False
    indented = sum(1 for line in lines if line.startswith("    ") or line.startswith("\t"))
    symbol_dense = sum(
        1
        for line in lines
        if len(line) >= 10
        and (
            sum(1 for char in line if char in "{}[]();=<>`") / max(len(line), 1) >= 0.12
            or re.search(r"[{}();=<>]{2,}", line)
        )
    )
    return indented == len(lines) and symbol_dense >= 2


def is_pipe_table(lines: list[str]) -> bool:
    if len(lines) < 3:
        return False
    counts = [line.count("|") for line in lines]
    return min(counts) >= 1 and len(set(counts)) == 1


def is_list_block(lines: list[str]) -> bool:
    if not lines:
        return False
    matches = sum(1 for line in lines if LIST_ITEM_RE.match(line))
    return matches >= 1 and matches >= max(1, len(lines) // 2)


def split_raw_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.strip():
            current.append(line)
            continue
        if current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    return blocks


def normalize_list_block(lines: list[str]) -> str:
    rendered: list[str] = []
    current_indent = 0
    current_body: list[str] = []
    current_marker = "-"
    for line in lines:
        match = LIST_ITEM_RE.match(line)
        if match:
            if current_body:
                rendered.append(f"{'  ' * current_indent}{current_marker} {fix_intra_word_spacing(' '.join(current_body))}")
            indent = len(match.group("indent").replace("\t", "  "))
            current_indent = indent // 2
            marker = match.group("marker")
            body = match.group("body").strip()
            if re.match(r"\d{1,3}[.)]", marker):
                current_marker = re.sub(r"\)$", ".", marker)
            elif re.match(r"[A-Za-z][.)]", marker):
                current_marker = marker[0] + "."
            else:
                current_marker = "-"
            current_body = [body]
        elif current_body:
            current_body.append(line.strip())
        else:
            current_body = [line.strip()]
    if current_body:
        rendered.append(f"{'  ' * current_indent}{current_marker} {fix_intra_word_spacing(' '.join(current_body))}")
    return "\n".join(rendered)


def choose_joiner(left: str, right: str) -> str:
    if not left:
        return right
    if not right:
        return left
    left = left.rstrip()
    right = right.lstrip()
    if left.endswith("-"):
        left_head = re.search(r"([A-Za-z]+)-$", left)
        right_head = re.match(r"([A-Za-z]+)(.*)$", right)
        if left_head and right_head:
            merged = left_head.group(1) + right_head.group(1)
            if is_known_word(merged):
                return left[:-1] + right
        return left + right
    if URL_JOIN_TAIL_RE.search(left) and re.match(r"[A-Za-z0-9/_#?=&%-]", right):
        return left + right
    return f"{left} {right}"


def normalize_paragraph_block(lines: list[str], aggressive: bool = True) -> str:
    text = ""
    for line in lines:
        text = choose_joiner(text, line.strip())
    text = re.sub(r"\s{2,}", " ", text).strip()
    text = fix_url_spacing(text)
    text = fix_intra_word_spacing(text) if aggressive else text
    heading_like = re.match(r"^(#{1,6})\s+(.*)$", text)
    if heading_like:
        body = heading_like.group(2).strip()
        if body.startswith(("“", "\"", "'")) or (
            len(body) > 70 and body.lower().startswith(("this ", "that ", "these ", "those ", "it "))
        ):
            text = body
    return text


def classify_block(lines: list[str]) -> Block:
    if is_code_block(lines):
        return Block(kind="code", text="\n".join(lines))
    if is_pipe_table(lines):
        header_cells = [cell.strip() for cell in lines[0].split("|")]
        normalized_lines = [header_cells]
        for line in lines[1:]:
            normalized_lines.append([cell.strip() for cell in line.split("|")])
        width = max(len(row) for row in normalized_lines)
        padded = [row + [""] * (width - len(row)) for row in normalized_lines]
        header = "| " + " | ".join(padded[0]) + " |"
        divider = "| " + " | ".join("---" for _ in padded[0]) + " |"
        body = ["| " + " | ".join(row) + " |" for row in padded[1:]]
        return Block(kind="table", text="\n".join([header, divider, *body]))
    if is_list_block(lines):
        return Block(kind="list", text=normalize_list_block(lines))
    if len(lines) == 1:
        is_heading, level, text, explicit = is_heading_line(lines[0])
        if is_heading:
            return Block(kind="heading", text=text, level=level, explicit=explicit)
    aggressive = not any(
        token in " ".join(lines).lower()
        for token in ("http://", "https://", "www.", "[1]", "[2]", "isbn")
    )
    return Block(kind="paragraph", text=normalize_paragraph_block(lines, aggressive=aggressive))


def merge_adjacent_paragraphs(blocks: list[Block]) -> list[Block]:
    merged: list[Block] = []
    for block in blocks:
        if (
            merged
            and block.kind == "paragraph"
            and merged[-1].kind == "paragraph"
            and should_merge_paragraphs(merged[-1].text, block.text)
        ):
            merged[-1].text = choose_joiner(merged[-1].text, block.text)
            merged[-1].text = fix_intra_word_spacing(merged[-1].text)
        else:
            merged.append(block)
    return merged


def should_merge_paragraphs(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left.endswith("-"):
        return True
    if re.search(r"[.!?\"”’:)]$", left):
        return False
    return bool(re.match(r"[a-z0-9\[(“‘]", right))


def normalize_heading_levels(blocks: list[Block]) -> list[Block]:
    normalized: list[Block] = []
    prev_level = 0
    title_assigned = False
    for block in blocks:
        if block.kind != "heading":
            normalized.append(block)
            continue
        text = re.sub(r"^#+\s*", "", fix_intra_word_spacing(block.text)).strip()
        lower = text.lower()
        desired = block.level
        if not title_assigned:
            desired = 1
            title_assigned = True
        elif lower.startswith(MAJOR_HEADING_PREFIXES):
            desired = 2
        elif block.explicit and block.level is not None:
            desired = max(2, min(block.level, 4))
            if prev_level <= 1:
                desired = min(desired, 2)
        elif prev_level <= 1:
            desired = 2
        else:
            desired = min(prev_level + 1, 4)
        if prev_level and desired > prev_level + 1:
            desired = prev_level + 1
        desired = max(1, min(desired, 4))
        prev_level = desired
        normalized.append(Block(kind="heading", text=text, level=desired, explicit=True))
    return normalized


def demote_contextual_headings(blocks: list[Block]) -> list[Block]:
    normalized: list[Block] = []
    for index, block in enumerate(blocks):
        if block.kind != "heading":
            normalized.append(block)
            continue
        next_block = blocks[index + 1] if index + 1 < len(blocks) else None
        text = block.text.strip()
        if text.startswith(("“", "\"", "'")):
            normalized.append(Block(kind="paragraph", text=text))
            continue
        if (
            next_block
            and next_block.kind == "paragraph"
            and re.match(r"^[a-z(“‘\"]", next_block.text)
            and (text.endswith(("—", "-")) or len(text) > 70)
        ):
            normalized.append(Block(kind="paragraph", text=text))
            continue
        normalized.append(block)
    return normalized


def convert_contents_headings_to_list(blocks: list[Block]) -> list[Block]:
    converted: list[Block] = []
    index = 0
    while index < len(blocks):
        block = blocks[index]
        converted.append(block)
        if block.kind == "heading" and block.text.lower() == "contents":
            list_lines: list[str] = []
            index += 1
            while index < len(blocks):
                current = blocks[index]
                if current.kind != "heading":
                    break
                if index + 1 < len(blocks) and blocks[index + 1].kind != "heading":
                    break
                list_lines.append(f"- {current.text}")
                index += 1
            if list_lines:
                converted.append(Block(kind="list", text="\n".join(list_lines)))
            continue
        index += 1
    return converted


BACKMATTER_DEMOTE_HEADINGS = {"index", "sources", "references", "bibliography", "works cited"}


def demote_backmatter_headings(blocks: list[Block]) -> list[Block]:
    normalized: list[Block] = []
    in_backmatter = False
    for block in blocks:
        if block.kind == "heading" and block.text.lower() in BACKMATTER_DEMOTE_HEADINGS:
            in_backmatter = True
            normalized.append(block)
            continue
        if in_backmatter and block.kind == "heading":
            normalized.append(Block(kind="paragraph", text=block.text))
            continue
        normalized.append(block)
    return normalized


def demote_render_headings(blocks: list[Block]) -> list[Block]:
    normalized: list[Block] = []
    for index, block in enumerate(blocks):
        if block.kind != "heading":
            normalized.append(block)
            continue
        text = block.text.strip()
        next_block = blocks[index + 1] if index + 1 < len(blocks) else None
        if text.startswith(("“", "\"", "'")):
            normalized.append(Block(kind="paragraph", text=text))
            continue
        if (
            next_block
            and next_block.kind == "paragraph"
            and re.match(r"^[a-z(“‘\"]", next_block.text)
            and (
                text.endswith(("—", "-"))
                or len(text) > 70
                or text.lower().startswith(("this ", "that ", "these ", "those ", "it "))
            )
        ):
            normalized.append(Block(kind="paragraph", text=text))
            continue
        normalized.append(block)
    return normalized


def render_blocks(blocks: list[Block]) -> str:
    rendered: list[str] = []
    for block in blocks:
        if block.kind == "heading":
            rendered.append(f"{'#' * (block.level or 2)} {block.text.strip()}")
        elif block.kind == "code":
            rendered.append(f"```\n{block.text.rstrip()}\n```")
        else:
            rendered.append(block.text.rstrip())
    output = "\n\n".join(part for part in rendered if part.strip())
    output = re.sub(r"\n{3,}", "\n\n", output).strip() + "\n"
    return cleanup_rendered_output(output)


def cleanup_rendered_output(output: str) -> str:
    cleaned_lines: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.rstrip()
        line = re.sub(r"^(\s*)[•*+]\s+", r"\1- ", line)
        spaced_hash = re.match(r"^(?:#\s+){2,}(.*)$", line)
        if spaced_hash:
            body = spaced_hash.group(1).strip()
            if body.startswith(("“", "\"", "'")) or "|" in body or body.lower().startswith(
                ("this ", "that ", "these ", "those ", "it ")
            ):
                line = body
            else:
                line = "#" * raw_line.count("#") + " " + body
        heading = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading:
            body = heading.group(2).strip()
            if body.startswith(("“", "\"", "'")) or "|" in body or (
                len(body) > 70 and body.lower().startswith(("this ", "that ", "these ", "those ", "it "))
            ):
                line = body
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip() + "\n"
    return cleaned


def count_heading_skips(markdown: str) -> int:
    skips = 0
    prev = 0
    for line in markdown.splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        level = len(match.group("level"))
        if prev and level > prev + 1:
            skips += 1
        prev = level
    return skips


def process_file(path_str: str, chunk_size: int) -> dict[str, object]:
    path = Path(path_str)
    repeat_counts, first_nonblank, input_lines = gather_profile(path, chunk_size)
    repeated_keys = {
        key
        for key, count in repeat_counts.items()
        if key != "__page__" and count >= 4
    }
    lines, removed_repeats = load_filtered_lines(path, chunk_size, repeated_keys, first_nonblank)
    raw_blocks = split_raw_blocks(lines)
    blocks = [classify_block(block_lines) for block_lines in raw_blocks]
    blocks = demote_contextual_headings(blocks)
    blocks = merge_adjacent_paragraphs(blocks)
    blocks = normalize_heading_levels(blocks)
    blocks = convert_contents_headings_to_list(blocks)
    blocks = demote_backmatter_headings(blocks)
    blocks = demote_render_headings(blocks)
    blocks = merge_adjacent_paragraphs(blocks)
    output = render_blocks(blocks)
    original = path.read_text(encoding="utf-8", errors="replace")
    changed = original != output
    if changed:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as handle:
            handle.write(output)
            temp_name = handle.name
        os.replace(temp_name, path)
    summary = FileSummary(
        path=str(path),
        changed=changed,
        input_lines=input_lines,
        output_lines=output.count("\n"),
        blocks=len(blocks),
        headings=sum(1 for block in blocks if block.kind == "heading"),
        removed_repeats=removed_repeats,
        heading_skips=count_heading_skips(output),
    )
    return summary.__dict__


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize OCR-derived Markdown books.")
    parser.add_argument("paths", nargs="+", help="Markdown files to normalize")
    parser.add_argument("--jobs", type=int, default=1, help="Parallel worker count")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Chunk size per scan pass")
    args = parser.parse_args()

    max_workers = max(1, min(args.jobs, len(args.paths)))
    results: list[dict[str, object]] = []
    if max_workers == 1:
        for path in args.paths:
            results.append(process_file(path, args.chunk_size))
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_file, path, args.chunk_size): path for path in args.paths
            }
            for future in as_completed(futures):
                results.append(future.result())
    for result in sorted(results, key=lambda item: str(item["path"])):
        print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

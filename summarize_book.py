#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
import copy
import hashlib
import json
import os
import re
import sys
import textwrap
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
from urllib import error, request


HEADING_RE = re.compile(r"^(#{1,6})\s+(.*\S)\s*$")
CHAPTER_LIKE_RE = re.compile(r"^(chapter|book|part|section|appendix|prologue|epilogue)\b", re.IGNORECASE)
TOC_ENTRY_RE = re.compile(r"^\s*(?:[-*+]\s+)?(?:\d+[.):]?\s+)?(.+?)\s*$")

PROMPT_FILE = Path(__file__).resolve().parent / "prompts" / "book_summary_compromise.md"

PROFILES = {
    "cheap": {
        "chunk_model": "gpt-5-mini",
        "chapter_model": "gpt-5-mini",
        "final_model": "gpt-5-mini",
        "verify_model": "gpt-5-mini",
        "chunk_reasoning_effort": None,
        "chapter_reasoning_effort": None,
        "final_reasoning_effort": None,
        "verify_reasoning_effort": None,
        "target_chunk_tokens": 5000,
        "max_chunk_tokens": 7000,
        "verify": False,
    },
    "compromise": {
        "chunk_model": "gpt-5-mini",
        "chapter_model": "gpt-5-mini",
        "final_model": "gpt-5.4",
        "verify_model": "gpt-5.4",
        "chunk_reasoning_effort": None,
        "chapter_reasoning_effort": None,
        "final_reasoning_effort": "medium",
        "verify_reasoning_effort": "low",
        "target_chunk_tokens": 6500,
        "max_chunk_tokens": 8500,
        "verify": True,
    },
    "quality": {
        "chunk_model": "gpt-5-mini",
        "chapter_model": "gpt-5.4",
        "final_model": "gpt-5.4",
        "verify_model": "gpt-5.4",
        "chunk_reasoning_effort": None,
        "chapter_reasoning_effort": "low",
        "final_reasoning_effort": "medium",
        "verify_reasoning_effort": "medium",
        "target_chunk_tokens": 5000,
        "max_chunk_tokens": 7000,
        "verify": True,
    },
}


SECTION_KIND_VALUES = ["core_argument", "case_study", "framing", "reference", "endmatter", "mixed"]
SECTION_KIND_PRIORITY = {
    "core_argument": 5,
    "case_study": 4,
    "mixed": 3,
    "framing": 2,
    "reference": 1,
    "endmatter": 1,
}

REFERENCE_TITLE_MARKERS = {
    "sources",
    "references",
    "bibliography",
    "recommended reading",
    "reading list",
}

ENDMATTER_TITLE_MARKERS = {
    "about the author",
    "acknowledgments",
    "acknowledgements",
    "appreciation",
    "want more",
    "bonus",
}

SUPPLEMENTAL_TITLE_MARKERS = {
    *REFERENCE_TITLE_MARKERS,
    *ENDMATTER_TITLE_MARKERS,
    "timeline",
    "timeline of",
    "69 core musk methods",
}


LEGACY_CHUNK_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "chapter_title": {"type": "string"},
        "covered_sections": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string"},
        "main_points": {"type": "array", "items": {"type": "string"}},
        "examples_or_evidence": {"type": "array", "items": {"type": "string"}},
        "ambiguity_flags": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
    },
    "required": [
        "chapter_title",
        "covered_sections",
        "summary",
        "main_points",
        "examples_or_evidence",
        "ambiguity_flags",
        "confidence",
    ],
}

CHUNK_SCHEMA = copy.deepcopy(LEGACY_CHUNK_SCHEMA)
CHUNK_SCHEMA["properties"].update(
    {
        "packet_kind": {"type": "string", "enum": SECTION_KIND_VALUES},
        "role_in_book": {"type": "string"},
        "primary_claims": {"type": "array", "items": {"type": "string"}},
        "methods_or_principles": {"type": "array", "items": {"type": "string"}},
        "story_events": {"type": "array", "items": {"type": "string"}},
        "priority_weight": {"type": "integer", "minimum": 1, "maximum": 5},
    }
)
CHUNK_SCHEMA["required"] = [
    *LEGACY_CHUNK_SCHEMA["required"],
    "packet_kind",
    "role_in_book",
    "primary_claims",
    "methods_or_principles",
    "story_events",
    "priority_weight",
]

LEGACY_CHAPTER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "chapter_title": {"type": "string"},
        "summary": {"type": "string"},
        "major_points": {"type": "array", "items": {"type": "string"}},
        "notable_examples": {"type": "array", "items": {"type": "string"}},
        "ambiguity_flags": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
    },
    "required": [
        "chapter_title",
        "summary",
        "major_points",
        "notable_examples",
        "ambiguity_flags",
        "confidence",
    ],
}

CHAPTER_SCHEMA = copy.deepcopy(LEGACY_CHAPTER_SCHEMA)
CHAPTER_SCHEMA["properties"].update(
    {
        "chapter_kind": {"type": "string", "enum": SECTION_KIND_VALUES},
        "chapter_thesis": {"type": "string"},
        "priority_weight": {"type": "integer", "minimum": 1, "maximum": 5},
        "primary_claims": {"type": "array", "items": {"type": "string"}},
        "methods_or_principles": {"type": "array", "items": {"type": "string"}},
        "story_progression": {"type": "array", "items": {"type": "string"}},
        "what_to_preserve_in_final": {"type": "array", "items": {"type": "string"}},
    }
)
CHAPTER_SCHEMA["required"] = [
    *LEGACY_CHAPTER_SCHEMA["required"],
    "chapter_kind",
    "chapter_thesis",
    "priority_weight",
    "primary_claims",
    "methods_or_principles",
    "story_progression",
    "what_to_preserve_in_final",
]

FINAL_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "book_title": {"type": "string"},
        "executive_summary": {"type": "string"},
        "central_thesis": {"type": "string"},
        "structural_overview": {"type": "string"},
        "chapter_summaries": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "chapter_title": {"type": "string"},
                    "summary": {"type": "string"},
                },
                "required": ["chapter_title", "summary"],
            },
        },
        "major_themes": {"type": "array", "items": {"type": "string"}},
        "notable_examples": {"type": "array", "items": {"type": "string"}},
        "practical_takeaways": {"type": "array", "items": {"type": "string"}},
        "faithfulness_notes": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "book_title",
        "executive_summary",
        "central_thesis",
        "structural_overview",
        "chapter_summaries",
        "major_themes",
        "notable_examples",
        "practical_takeaways",
        "faithfulness_notes",
    ],
}

VERIFY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "severity": {"type": "string", "enum": ["high", "medium", "low"]},
                    "problem": {"type": "string"},
                    "correction": {"type": "string"},
                },
                "required": ["severity", "problem", "correction"],
            },
        },
        **FINAL_SCHEMA["properties"],
    },
    "required": ["issues", *FINAL_SCHEMA["required"]],
}


def schema_errors(payload: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    errors: list[str] = []
    expected_type = schema.get("type")

    if expected_type == "object":
        if not isinstance(payload, dict):
            return [f"{path}: expected object, got {type(payload).__name__}"]
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for key in required:
            if key not in payload:
                errors.append(f"{path}.{key}: missing required property")
        if schema.get("additionalProperties") is False:
            extras = sorted(key for key in payload if key not in properties)
            for key in extras:
                errors.append(f"{path}.{key}: unexpected property")
        for key, subschema in properties.items():
            if key in payload:
                errors.extend(schema_errors(payload[key], subschema, f"{path}.{key}"))
        return errors

    if expected_type == "array":
        if not isinstance(payload, list):
            return [f"{path}: expected array, got {type(payload).__name__}"]
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(payload):
                errors.extend(schema_errors(item, item_schema, f"{path}[{index}]"))
        return errors

    if expected_type == "string":
        if not isinstance(payload, str):
            return [f"{path}: expected string, got {type(payload).__name__}"]
    elif expected_type == "integer":
        if not isinstance(payload, int) or isinstance(payload, bool):
            return [f"{path}: expected integer, got {type(payload).__name__}"]
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if minimum is not None and payload < minimum:
            errors.append(f"{path}: expected >= {minimum}, got {payload}")
        if maximum is not None and payload > maximum:
            errors.append(f"{path}: expected <= {maximum}, got {payload}")
    elif expected_type is None:
        return errors
    else:
        errors.append(f"{path}: unsupported schema type {expected_type!r}")
        return errors

    enum_values = schema.get("enum")
    if enum_values is not None and payload not in enum_values:
        errors.append(f"{path}: expected one of {enum_values!r}, got {payload!r}")
    return errors


def matches_schema(payload: Any, schema: dict[str, Any]) -> bool:
    return not schema_errors(payload, schema)


def validate_payload_or_raise(
    payload: Any,
    schema_options: list[tuple[str, dict[str, Any]]],
    context: str,
) -> str:
    failures: list[str] = []
    for name, schema in schema_options:
        errors = schema_errors(payload, schema)
        if not errors:
            return name
        preview = "; ".join(errors[:6])
        if len(errors) > 6:
            preview += f"; ... ({len(errors)} errors total)"
        failures.append(f"{name}: {preview}")
    raise RuntimeError(f"{context} does not match expected schema. " + " | ".join(failures))


@dataclass
class Section:
    section_id: str
    level: int
    title: str
    path: list[str]
    chapter_title: str
    raw_text: str
    token_estimate: int


@dataclass
class Packet:
    packet_id: str
    chapter_title: str
    section_labels: list[str]
    raw_text: str
    token_estimate: int


class TokenCounter:
    def __init__(self) -> None:
        self.mode = "approx"
        self.encoder = None
        try:
            import tiktoken  # type: ignore

            self.encoder = tiktoken.get_encoding("cl100k_base")
            self.mode = "cl100k_base"
        except Exception:
            self.encoder = None

    def count(self, text: str) -> int:
        if not text:
            return 0
        if self.encoder is not None:
            try:
                return len(self.encoder.encode(text))
            except Exception:
                pass
        return max(1, len(text) // 4)


class CacheStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, key: str) -> Path:
        return self.root / f"{key}.json"

    def load(self, key: str) -> dict[str, Any] | None:
        path = self.path_for(key)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def save(self, key: str, payload: dict[str, Any]) -> None:
        self.path_for(key).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class ResponsesClient:
    def __init__(self, api_key: str, cache: CacheStore, timeout: int = 300) -> None:
        self.api_key = api_key
        self.cache = cache
        self.timeout = timeout
        self.endpoint = "https://api.openai.com/v1/responses"

    def call_json(
        self,
        *,
        stage: str,
        model: str,
        instructions: str,
        user_text: str,
        schema_name: str,
        schema: dict[str, Any],
        reasoning_effort: str | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "store": False,
            "instructions": instructions,
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_text}],
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": schema_name,
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        if reasoning_effort:
            payload["reasoning"] = {"effort": reasoning_effort}

        cache_key = hashlib.sha256(
            json.dumps(
                {"stage": stage, "payload": payload},
                sort_keys=True,
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        cached = self.cache.load(cache_key)
        if cached is not None:
            validate_payload_or_raise(cached["parsed"], [("response", schema)], f"Cached payload for {stage}")
            return cached["parsed"]

        req = request.Request(
            self.endpoint,
            method="POST",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                with request.urlopen(req, timeout=self.timeout) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                text = self._extract_output_text(body)
                parsed = json.loads(text)
                validate_payload_or_raise(parsed, [("response", schema)], f"Model payload for {stage}")
                self.cache.save(cache_key, {"response": body, "parsed": parsed})
                return parsed
            except error.HTTPError as exc:
                message = exc.read().decode("utf-8", errors="replace")
                last_error = RuntimeError(f"OpenAI API error ({exc.code}): {message}")
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"Failed OpenAI call for stage {stage}: {last_error}")

    def _extract_output_text(self, body: dict[str, Any]) -> str:
        if isinstance(body.get("output_text"), str) and body["output_text"].strip():
            return body["output_text"]
        parts: list[str] = []
        for item in body.get("output", []):
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    parts.append(content.get("text", ""))
                if content.get("type") == "refusal":
                    raise RuntimeError(f"Model refused request: {content.get('refusal', '')}")
        text = "".join(parts).strip()
        if not text:
            raise RuntimeError("No output_text found in response payload.")
        return text


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "book"


def extract_book_title(markdown: str, path: Path) -> str:
    for line in markdown.splitlines():
        match = HEADING_RE.match(line.strip())
        if match and len(match.group(1)) == 1:
            return match.group(2).strip()
    return path.stem.replace("_", " ").replace("-", " ").strip()


def normalize_title_for_match(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^(?:chapter|book|part|section)\s+\d+[.:]?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\d+[.):]?\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.casefold()


def priority_weight_for_kind(kind: str) -> int:
    return SECTION_KIND_PRIORITY.get(kind, 3)


def detail_guidance_for_priority(priority_weight: int, kind: str) -> str:
    if priority_weight >= 5:
        return (
            "High-detail material. Preserve the chapter's core claims, methods, pivots, and "
            "representative examples with generous space in the final summary."
        )
    if priority_weight == 4:
        return (
            "Detailed coverage. Keep the main argument or chronology specific enough that a "
            "serious reader could reconstruct the chapter's distinctive contribution."
        )
    if priority_weight == 3:
        return (
            "Moderate coverage. Preserve the chapter's distinct role and strongest examples, "
            "but do not expand every subsection equally."
        )
    if kind in {"reference", "endmatter"}:
        return "Compressed coverage. Summarize this material honestly as supporting or documentary matter."
    return "Light coverage. Preserve only the role this material plays in setting up or closing the book."


def unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = value.strip()
        key = cleaned.casefold()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def leading_sentence(text: str) -> str:
    cleaned = " ".join(text.split())
    if not cleaned:
        return ""
    match = re.search(r"(?<=[.!?])\s+", cleaned)
    if match:
        return cleaned[: match.start()].strip()
    return cleaned


def infer_section_kind(title: str, raw_text: str) -> str:
    normalized = normalize_title_for_match(title)
    haystack = f"{normalized}\n{raw_text[:3000].casefold()}"
    if any(marker in normalized for marker in REFERENCE_TITLE_MARKERS):
        return "reference"
    if any(marker in normalized for marker in ENDMATTER_TITLE_MARKERS):
        return "endmatter"
    if normalized.startswith("timeline"):
        return "endmatter"
    if any(
        token in haystack
        for token in [
            "bibliography",
            "references",
            "sources",
            "citations",
            "works cited",
            "further reading",
            "endnotes",
            "footnotes",
            "recommended reading",
            "reading list",
        ]
    ):
        return "reference"
    if any(
        token in haystack
        for token in [
            "index",
            "acknowledg",
            "about the author",
            "credits",
            "copyright",
            "timeline",
            "appreciation",
            "want more",
            "bonus",
        ]
    ):
        return "endmatter"
    if any(
        token in haystack
        for token in [
            "contents",
            "preface",
            "foreword",
            "introduction",
            "prologue",
            "epilogue",
            "conclusion",
            "notes on this book",
            "welcome",
            "highlights",
        ]
    ):
        return "framing"
    principle_hits = sum(
        1
        for token in [
            "principle",
            "strategy",
            "framework",
            "lesson",
            "method",
            "why ",
            "how ",
            "should ",
        ]
        if token in haystack
    )
    chronology_hits = len(re.findall(r"\b(?:18|19|20)\d{2}\b", raw_text))
    if principle_hits >= 2 and chronology_hits <= 3:
        return "core_argument"
    if chronology_hits >= 4:
        return "case_study"
    return "mixed"


def role_for_kind(kind: str, chapter_title: str, labels: list[str]) -> str:
    joined = ", ".join(labels[:3]) if labels else chapter_title
    if kind == "core_argument":
        return f"Carries a main explanatory or prescriptive argument centered on {chapter_title}."
    if kind == "case_study":
        return f"Advances the book through concrete events, chronology, or examples in {chapter_title}."
    if kind == "framing":
        return f"Frames or transitions the book's agenda around {chapter_title}."
    if kind == "reference":
        return f"Documents supporting source material for {joined}."
    if kind == "endmatter":
        return f"Wraps up or documents non-argumentative back matter related to {joined}."
    return f"Supports the book through mixed exposition and examples in {chapter_title}."


def extract_toc_titles(markdown: str) -> set[str]:
    lines = markdown.splitlines()
    toc_titles: set[str] = set()
    in_contents = False
    for line in lines:
        heading = HEADING_RE.match(line.strip())
        if heading:
            title = heading.group(2).strip()
            if title.casefold() == "contents":
                in_contents = True
                continue
            if in_contents:
                break
        if not in_contents:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        match = TOC_ENTRY_RE.match(stripped)
        if not match:
            continue
        candidate = match.group(1).strip()
        if candidate:
            toc_titles.add(normalize_title_for_match(candidate))
    return toc_titles


def is_major_front_or_back_matter(title: str) -> bool:
    return normalize_title_for_match(title) in {
        "preface",
        "foreword",
        "introduction",
        "conclusion",
        "acknowledgments",
        "acknowledgements",
        "illustration credits",
        "index",
        "about the author",
        "sources",
        "references",
        "bibliography",
    }


def is_chapter_anchor(title: str, level: int, toc_titles: set[str]) -> bool:
    normalized = normalize_title_for_match(title)
    if normalized in toc_titles:
        return True
    if CHAPTER_LIKE_RE.match(title):
        return True
    if level <= 2 and is_major_front_or_back_matter(title):
        return True
    return False


def parse_sections(markdown: str, book_title: str, counter: TokenCounter) -> list[Section]:
    sections: list[Section] = []
    stack: list[tuple[int, str]] = []
    toc_titles = extract_toc_titles(markdown)
    current_level: int | None = None
    current_title: str | None = None
    current_path: list[str] = []
    current_lines: list[str] = []
    current_chapter_title: str | None = None
    pre_heading_lines: list[str] = []

    def flush_current() -> None:
        nonlocal current_level, current_title, current_path, current_lines, current_chapter_title
        if current_title is None:
            return
        body = "\n".join(current_lines).strip()
        raw_text = f"{'#' * current_level} {current_title}\n\n{body}".strip()
        section_id = f"s{len(sections) + 1:04d}"
        sections.append(
            Section(
                section_id=section_id,
                level=current_level,
                title=current_title,
                path=current_path[:],
                chapter_title=current_chapter_title or current_title or "Book",
                raw_text=raw_text,
                token_estimate=counter.count(raw_text),
            )
        )
        current_level = None
        current_title = None
        current_path = []
        current_lines = []

    for line in markdown.splitlines():
        match = HEADING_RE.match(line.strip())
        if not match:
            if current_title is None:
                pre_heading_lines.append(line)
            else:
                current_lines.append(line)
            continue

        if current_title is not None:
            flush_current()

        level = len(match.group(1))
        title = match.group(2).strip()
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, title))
        current_level = level
        current_title = title
        current_path = [item for _, item in stack]
        current_lines = []
        if is_chapter_anchor(title, level, toc_titles) or current_chapter_title is None:
            current_chapter_title = title

    flush_current()

    front_matter = "\n".join(pre_heading_lines).strip()
    if front_matter:
        sections.insert(
            0,
            Section(
                section_id="s0000",
                level=1,
                title="Front Matter",
                path=["Front Matter"],
                chapter_title="Front Matter",
                raw_text=front_matter,
                token_estimate=counter.count(front_matter),
            ),
        )

    if not sections:
        whole = markdown.strip()
        sections.append(
            Section(
                section_id="s0001",
                level=1,
                title=book_title,
                path=[book_title],
                chapter_title=book_title,
                raw_text=whole,
                token_estimate=counter.count(whole),
            )
        )
    return sections


def split_large_section(
    section: Section,
    counter: TokenCounter,
    target_chunk_tokens: int,
    max_chunk_tokens: int,
    start_index: int,
) -> list[Packet]:
    paragraphs = [part.strip() for part in re.split(r"\n{2,}", section.raw_text) if part.strip()]
    packets: list[Packet] = []
    current_parts: list[str] = []
    current_tokens = 0
    local_index = 1

    def flush() -> None:
        nonlocal current_parts, current_tokens, local_index
        if not current_parts:
            return
        raw_text = "\n\n".join(current_parts)
        packets.append(
            Packet(
                packet_id=f"p{start_index + len(packets):04d}",
                chapter_title=section.chapter_title,
                section_labels=[" > ".join(section.path) + f" (part {local_index})"],
                raw_text=raw_text,
                token_estimate=counter.count(raw_text),
            )
        )
        current_parts = []
        current_tokens = 0
        local_index += 1

    for paragraph in paragraphs:
        tokens = counter.count(paragraph)
        if current_parts and current_tokens + tokens > target_chunk_tokens:
            flush()
        current_parts.append(paragraph)
        current_tokens += tokens
        if current_tokens >= max_chunk_tokens:
            flush()
    flush()
    return packets


def build_packets(
    sections: list[Section],
    counter: TokenCounter,
    target_chunk_tokens: int,
    max_chunk_tokens: int,
) -> list[Packet]:
    packets: list[Packet] = []
    current_chapter: str | None = None
    current_labels: list[str] = []
    current_texts: list[str] = []
    current_kinds: list[str] = []
    current_tokens = 0

    def flush() -> None:
        nonlocal current_chapter, current_labels, current_texts, current_kinds, current_tokens
        if not current_texts:
            return
        raw_text = "\n\n---\n\n".join(current_texts)
        packets.append(
            Packet(
                packet_id=f"p{len(packets) + 1:04d}",
                chapter_title=current_chapter or "Book",
                section_labels=current_labels[:],
                raw_text=raw_text,
                token_estimate=counter.count(raw_text),
            )
        )
        current_chapter = None
        current_labels = []
        current_texts = []
        current_kinds = []
        current_tokens = 0

    def has_doc_kind(kinds: list[str]) -> bool:
        return any(kind in {"reference", "endmatter"} for kind in kinds)

    def label_looks_supplemental(label: str) -> bool:
        normalized = normalize_title_for_match(label)
        return any(marker in normalized for marker in SUPPLEMENTAL_TITLE_MARKERS)

    for section in sections:
        section_kind = infer_section_kind(" > ".join(section.path), section.raw_text)
        if section.token_estimate > max_chunk_tokens:
            flush()
            packets.extend(
                split_large_section(
                    section,
                    counter=counter,
                    target_chunk_tokens=target_chunk_tokens,
                    max_chunk_tokens=max_chunk_tokens,
                    start_index=len(packets) + 1,
                )
            )
            continue

        label = " > ".join(section.path)
        if (
            current_texts
            and (
                section.chapter_title != current_chapter
                or current_tokens + section.token_estimate > target_chunk_tokens
            )
        ):
            flush()
        elif current_texts:
            current_has_doc = has_doc_kind(current_kinds)
            next_is_doc = section_kind in {"reference", "endmatter"}
            if current_has_doc != next_is_doc:
                flush()
            elif label_looks_supplemental(label) and not all(label_looks_supplemental(item) for item in current_labels):
                flush()
            elif not label_looks_supplemental(label) and any(label_looks_supplemental(item) for item in current_labels):
                flush()

        current_chapter = section.chapter_title
        current_labels.append(label)
        current_texts.append(section.raw_text)
        current_kinds.append(section_kind)
        current_tokens += section.token_estimate

    flush()
    return packets


def infer_packet_profile(packet: Packet) -> dict[str, Any]:
    kind = infer_section_kind(" ".join(packet.section_labels) or packet.chapter_title, packet.raw_text)
    priority_weight = priority_weight_for_kind(kind)
    return {
        "packet_id": packet.packet_id,
        "chapter_title": packet.chapter_title,
        "covered_sections": packet.section_labels[:],
        "packet_kind": kind,
        "priority_weight": priority_weight,
        "detail_guidance": detail_guidance_for_priority(priority_weight, kind),
        "role_in_book": role_for_kind(kind, packet.chapter_title, packet.section_labels),
    }


def dominant_kind(kinds: list[str]) -> str:
    if not kinds:
        return "mixed"
    counts = Counter(kinds)
    best = sorted(counts.items(), key=lambda item: (-item[1], -priority_weight_for_kind(item[0]), item[0]))
    return best[0][0]


def dominant_chapter_kind(packet_profiles: list[dict[str, Any]]) -> str:
    if not packet_profiles:
        return "mixed"
    relevant_packets = packet_profiles[:]
    non_documentary_packets = [
        profile for profile in packet_profiles if profile["packet_kind"] not in {"reference", "endmatter"}
    ]
    if non_documentary_packets:
        relevant_packets = non_documentary_packets

    weighted_scores: Counter[str] = Counter()
    for profile in relevant_packets:
        kind = profile["packet_kind"]
        weighted_scores[kind] += int(profile.get("priority_weight", priority_weight_for_kind(kind)))

    best = sorted(weighted_scores.items(), key=lambda item: (-item[1], -priority_weight_for_kind(item[0]), item[0]))
    return best[0][0]


def build_book_map(book_title: str, sections: list[Section], packets: list[Packet]) -> dict[str, Any]:
    packet_profiles = [infer_packet_profile(packet) for packet in packets]
    book_title_key = normalize_title_for_match(book_title)
    for profile in packet_profiles:
        if (
            normalize_title_for_match(profile["chapter_title"]) == book_title_key
            and profile["packet_kind"] in {"reference", "mixed"}
        ):
            profile["packet_kind"] = "framing"
            profile["priority_weight"] = priority_weight_for_kind("framing")
            profile["detail_guidance"] = detail_guidance_for_priority(profile["priority_weight"], "framing")
            profile["role_in_book"] = role_for_kind("framing", profile["chapter_title"], profile["covered_sections"])
    section_counts = Counter(section.chapter_title for section in sections)
    chapter_profiles: list[dict[str, Any]] = []

    chapter_order = unique_preserving_order([packet.chapter_title for packet in packets])
    for chapter_title in chapter_order:
        chapter_packets = [profile for profile in packet_profiles if profile["chapter_title"] == chapter_title]
        chapter_kind = dominant_chapter_kind(chapter_packets)
        priority_values = [profile["priority_weight"] for profile in chapter_packets] or [3]
        priority_weight = max(priority_values)
        chapter_profiles.append(
            {
                "chapter_title": chapter_title,
                "chapter_kind": chapter_kind,
                "priority_weight": priority_weight,
                "detail_guidance": detail_guidance_for_priority(priority_weight, chapter_kind),
                "packet_count": len(chapter_packets),
                "section_count": section_counts.get(chapter_title, len(chapter_packets)),
                "role_in_book": role_for_kind(
                    chapter_kind,
                    chapter_title,
                    [label for profile in chapter_packets for label in profile["covered_sections"]],
                ),
                "representative_sections": unique_preserving_order(
                    [label for profile in chapter_packets for label in profile["covered_sections"]]
                )[:5],
            }
        )

    overall_guidance = [
        "Give the most space to chapters or packets with priority 4-5; compress priority 1-2 material unless it frames the thesis.",
        "Treat reference and endmatter sections as documentation, not as core argumentative chapters.",
        "Use framing sections to explain the book's setup and transitions, but do not let them crowd out the main argument.",
        "Aim for a substantial study-guide summary, not jacket-copy brevity: core chapters should carry enough detail to reconstruct the book's real argument and examples.",
    ]
    return {
        "book_title": book_title,
        "chapter_profiles": chapter_profiles,
        "packet_profiles": packet_profiles,
        "overall_guidance": overall_guidance,
    }


def render_book_map_markdown(book_map: dict[str, Any]) -> str:
    lines = [f"# {book_map['book_title']} - Book Map", ""]
    lines.extend(["## Overall Guidance", ""])
    for item in book_map.get("overall_guidance", []):
        lines.append(f"- {item}")
    lines.extend(["", "## Chapter Profiles", ""])
    for chapter in book_map.get("chapter_profiles", []):
        lines.append(f"### {chapter['chapter_title']}")
        lines.append("")
        lines.append(f"- Kind: {chapter['chapter_kind']}")
        lines.append(f"- Priority weight: {chapter['priority_weight']}")
        lines.append(f"- Detail guidance: {chapter['detail_guidance']}")
        lines.append(f"- Packet count: {chapter['packet_count']}")
        lines.append(f"- Section count: {chapter['section_count']}")
        lines.append(f"- Role: {chapter['role_in_book']}")
        if chapter.get("representative_sections"):
            lines.append(f"- Representative sections: {', '.join(chapter['representative_sections'])}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def book_map_chapter_lookup(book_map: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not book_map:
        return {}
    return {item["chapter_title"]: item for item in book_map.get("chapter_profiles", [])}


def book_map_packet_lookup(book_map: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not book_map:
        return {}
    return {item["packet_id"]: item for item in book_map.get("packet_profiles", [])}


def compact_book_map(book_map: dict[str, Any] | None) -> dict[str, Any]:
    if not book_map:
        return {}
    return {
        "overall_guidance": book_map.get("overall_guidance", []),
        "chapter_profiles": [
            {
                "chapter_title": item["chapter_title"],
                "chapter_kind": item["chapter_kind"],
                "priority_weight": item["priority_weight"],
                "detail_guidance": item.get("detail_guidance", ""),
                "role_in_book": item["role_in_book"],
            }
            for item in book_map.get("chapter_profiles", [])
        ],
    }


def collect_ambiguity_flags(items: list[dict[str, Any]]) -> list[str]:
    flags: list[str] = []
    for item in items:
        for value in item.get("ambiguity_flags", []) or []:
            if isinstance(value, str):
                flags.append(value)
    return unique_preserving_order(flags)


def ambiguity_digest(items: list[dict[str, Any]], *, label_key: str) -> list[dict[str, Any]]:
    digest: list[dict[str, Any]] = []
    for item in items:
        flags = [flag for flag in item.get("ambiguity_flags", []) or [] if isinstance(flag, str) and flag.strip()]
        if flags:
            digest.append({label_key: item.get(label_key, ""), "ambiguity_flags": unique_preserving_order(flags)})
    return digest


def enrich_chunk_summary(payload: dict[str, Any], book_map: dict[str, Any] | None = None) -> dict[str, Any]:
    enriched = dict(payload)
    packet_profile = book_map_packet_lookup(book_map).get(str(enriched.get("packet_id", "")))
    chapter_profile = book_map_chapter_lookup(book_map).get(str(enriched.get("chapter_title", "")))
    kind = str(
        enriched.get("packet_kind")
        or (packet_profile or {}).get("packet_kind")
        or (chapter_profile or {}).get("chapter_kind")
        or "mixed"
    )
    enriched.setdefault("packet_kind", kind)
    enriched.setdefault("role_in_book", (packet_profile or {}).get("role_in_book") or role_for_kind(kind, enriched.get("chapter_title", "Book"), enriched.get("covered_sections", [])))
    enriched.setdefault("primary_claims", list(enriched.get("main_points") or []))
    enriched.setdefault("methods_or_principles", [])
    enriched.setdefault("story_events", list(enriched.get("examples_or_evidence") or []))
    enriched.setdefault(
        "priority_weight",
        int((packet_profile or {}).get("priority_weight") or (chapter_profile or {}).get("priority_weight") or priority_weight_for_kind(kind)),
    )
    return enriched


def enrich_chapter_summary(payload: dict[str, Any], book_map: dict[str, Any] | None = None) -> dict[str, Any]:
    enriched = dict(payload)
    chapter_profile = book_map_chapter_lookup(book_map).get(str(enriched.get("chapter_title", "")))
    kind = str(enriched.get("chapter_kind") or (chapter_profile or {}).get("chapter_kind") or "mixed")
    summary = str(enriched.get("summary") or "")
    major_points = list(enriched.get("major_points") or [])
    notable_examples = list(enriched.get("notable_examples") or [])
    enriched.setdefault("chapter_kind", kind)
    enriched.setdefault("chapter_thesis", leading_sentence(summary) or leading_sentence(" ".join(major_points)))
    enriched.setdefault(
        "priority_weight",
        int((chapter_profile or {}).get("priority_weight") or priority_weight_for_kind(kind)),
    )
    enriched.setdefault("primary_claims", major_points[:])
    enriched.setdefault("methods_or_principles", [])
    enriched.setdefault("story_progression", notable_examples[:])
    enriched.setdefault("what_to_preserve_in_final", unique_preserving_order(major_points + notable_examples)[:6])
    return enriched


def prompt_contract(prompt_file: Path) -> str:
    return prompt_file.read_text(encoding="utf-8").strip()


def chunk_stage_instructions(contract: str) -> str:
    return f"""{contract}

Stage: local packet summary.

Return strict JSON. Work only from the packet supplied by the user message.
Capture what the packet argues, illustrates, defines, or sets up.
Use the inferred packet and chapter profile in the user message as weighting guidance, not as
license to invent content.
If the packet looks like front matter, table of contents, sources, or metadata, summarize it as
such instead of pretending it is argumentative prose.
For core-argument packets, preserve claims, methods, and representative examples.
For case-study packets, preserve chronology, pivots, and concrete episodes.
For framing or reference packets, compress aggressively and focus on what role they play in the
book.
Keep the summary compact but specific."""


def chapter_stage_instructions(contract: str) -> str:
    return f"""{contract}

Stage: chapter synthesis.

Return strict JSON. Work only from the packet summaries supplied by the user message.
Merge repeated ideas, preserve chapter progression, and keep the result grounded in the packet
summaries rather than inventing new interpretation.
Use the inferred chapter profile to decide proportion: emphasize thesis-carrying material,
compress framing and documentation, and clearly distinguish the main chapter claim from local
supporting details.
Treat this as chapter-level study notes, not a blurb: a serious reader should come away knowing
what the chapter contributes, how it moves, and which examples or methods make it distinctive."""


def final_stage_instructions(contract: str) -> str:
    return f"""{contract}

Stage: final book synthesis.

Return strict JSON. Produce a polished, publication-quality summary that still reads as an
evidence-bound synthesis rather than a review.
Aim for a substantial summary, closer to a serious study guide than to jacket copy.
The executive summary should be readable prose, not notes.
Chapter summaries should be concise but specific, and for priority 4-5 chapters they should
carry enough detail that a serious reader could reconstruct the book's distinctive claims,
methods, chronology, and representative examples.
Use the supplied book map to keep proportion and carry the book's real structure into the final
summary.
Do not add admiration, criticism, or outside context unless it is explicitly present in the
provided chapter summaries.
Open by naming what kind of book this is and what central wager or mission holds its parts
together."""


def verify_stage_instructions(contract: str) -> str:
    return f"""{contract}

Stage: verification and revision.

Return strict JSON. Compare the draft book summary against the chapter summaries.
Use the book map to catch proportion errors, chapter omissions, and places where front matter or
reference material has been given too much weight.
List unsupported claims or overstatements in `issues`, then return a revised final summary that
is more faithful and still polished.
Prefer removing drift over preserving flair.
If the draft is too thin to stand in for a serious reading experience, deepen the core chapters
with grounded detail rather than adding gloss."""


def build_chunk_user_text(
    book_title: str,
    packet: Packet,
    *,
    book_map: dict[str, Any] | None = None,
) -> str:
    packet_profile = book_map_packet_lookup(book_map).get(packet.packet_id)
    chapter_profile = book_map_chapter_lookup(book_map).get(packet.chapter_title)
    return textwrap.dedent(
        f"""\
        Book title: {book_title}
        Chapter anchor: {packet.chapter_title}
        Packet id: {packet.packet_id}
        Inferred packet profile:
        {json.dumps(packet_profile or infer_packet_profile(packet), ensure_ascii=False, indent=2)}

        Inferred chapter profile:
        {json.dumps(chapter_profile or {}, ensure_ascii=False, indent=2)}

        Covered sections:
        {chr(10).join(f"- {label}" for label in packet.section_labels)}

        Source packet:
        {packet.raw_text}
        """
    ).strip()


def build_chapter_user_text(
    book_title: str,
    chapter_title: str,
    packet_summaries: list[dict[str, Any]],
    *,
    book_map: dict[str, Any] | None = None,
) -> str:
    chapter_profile = book_map_chapter_lookup(book_map).get(chapter_title)
    return textwrap.dedent(
        f"""\
        Book title: {book_title}
        Chapter title: {chapter_title}
        Inferred chapter profile:
        {json.dumps(chapter_profile or {}, ensure_ascii=False, indent=2)}

        Inherited ambiguity flags:
        {json.dumps(collect_ambiguity_flags(packet_summaries), ensure_ascii=False, indent=2)}

        Packet summaries:
        {json.dumps(packet_summaries, ensure_ascii=False, indent=2)}
        """
    ).strip()


def build_final_user_text(
    book_title: str,
    chapter_summaries: list[dict[str, Any]],
    *,
    book_map: dict[str, Any] | None = None,
) -> str:
    return textwrap.dedent(
        f"""\
        Book title: {book_title}
        Book map:
        {json.dumps(compact_book_map(book_map), ensure_ascii=False, indent=2)}

        Chapter ambiguity digest:
        {json.dumps(ambiguity_digest(chapter_summaries, label_key='chapter_title'), ensure_ascii=False, indent=2)}

        Chapter summaries:
        {json.dumps(chapter_summaries, ensure_ascii=False, indent=2)}
        """
    ).strip()


def build_verify_user_text(
    draft_summary: dict[str, Any],
    chapter_summaries: list[dict[str, Any]],
    *,
    book_map: dict[str, Any] | None = None,
) -> str:
    return textwrap.dedent(
        f"""\
        Book map:
        {json.dumps(compact_book_map(book_map), ensure_ascii=False, indent=2)}

        Chapter ambiguity digest:
        {json.dumps(ambiguity_digest(chapter_summaries, label_key='chapter_title'), ensure_ascii=False, indent=2)}

        Draft final summary:
        {json.dumps(draft_summary, ensure_ascii=False, indent=2)}

        Grounding chapter summaries:
        {json.dumps(chapter_summaries, ensure_ascii=False, indent=2)}
        """
    ).strip()


def render_markdown(summary: dict[str, Any], issues: list[dict[str, str]] | None = None) -> str:
    lines = [f"# {summary['book_title']} - Faithful Summary", ""]
    lines.extend(["## Executive Summary", "", summary["executive_summary"].strip(), ""])
    lines.extend(["## Central Thesis", "", summary["central_thesis"].strip(), ""])
    lines.extend(["## Structure", "", summary["structural_overview"].strip(), ""])
    lines.extend(["## Chapter-by-Chapter Summary", ""])
    for item in summary["chapter_summaries"]:
        lines.extend([f"### {item['chapter_title']}", "", item["summary"].strip(), ""])
    lines.extend(["## Major Themes", ""])
    for item in summary["major_themes"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.extend(["## Notable Examples", ""])
    for item in summary["notable_examples"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.extend(["## Practical Takeaways", ""])
    for item in summary["practical_takeaways"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.extend(["## Faithfulness Notes", ""])
    for item in summary["faithfulness_notes"]:
        lines.append(f"- {item}")
    if issues:
        lines.extend(["", "## Verification Issues Addressed", ""])
        for issue in issues:
            lines.append(f"- [{issue['severity']}] {issue['problem']} -> {issue['correction']}")
    return "\n".join(lines).strip() + "\n"


def summarise_file(
    path: Path,
    *,
    output_dir: Path,
    cache_root: Path,
    prompt_file: Path,
    profile_name: str,
    overrides: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    profile = {**PROFILES[profile_name], **{k: v for k, v in overrides.items() if v is not None}}
    counter = TokenCounter()
    markdown = path.read_text(encoding="utf-8")
    book_title = extract_book_title(markdown, path)
    sections = parse_sections(markdown, book_title, counter)
    packets = build_packets(
        sections,
        counter=counter,
        target_chunk_tokens=profile["target_chunk_tokens"],
        max_chunk_tokens=profile["max_chunk_tokens"],
    )
    book_map = build_book_map(book_title, sections, packets)

    run_stats = {
        "source_path": str(path),
        "book_title": book_title,
        "profile": profile_name,
        "token_counter": counter.mode,
        "section_count": len(sections),
        "packet_count": len(packets),
        "source_token_estimate": counter.count(markdown),
        "packet_token_estimate": sum(packet.token_estimate for packet in packets),
        "models": {
            "chunk_model": profile["chunk_model"],
            "chapter_model": profile["chapter_model"],
            "final_model": profile["final_model"],
            "verify_model": profile["verify_model"],
        },
        "book_map_chapter_count": len(book_map["chapter_profiles"]),
    }

    if dry_run:
        return {
            "mode": "dry_run",
            "stats": run_stats,
            "book_map": book_map,
            "sections": [asdict(section) for section in sections[:20]],
            "packets": [asdict(packet) for packet in packets[:20]],
        }

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required unless you run with --dry-run.")

    contract = prompt_contract(prompt_file)
    cache = CacheStore(cache_root / slugify(path.stem))
    client = ResponsesClient(api_key=api_key, cache=cache)

    chunk_summaries: list[dict[str, Any]] = []
    for packet in packets:
        parsed = client.call_json(
            stage=f"chunk:{packet.packet_id}",
            model=profile["chunk_model"],
            instructions=chunk_stage_instructions(contract),
            user_text=build_chunk_user_text(book_title, packet, book_map=book_map),
            schema_name="book_chunk_summary",
            schema=CHUNK_SCHEMA,
            reasoning_effort=profile["chunk_reasoning_effort"],
        )
        parsed["packet_id"] = packet.packet_id
        parsed["packet_token_estimate"] = packet.token_estimate
        chunk_summaries.append(enrich_chunk_summary(parsed, book_map))

    chapter_to_packets: dict[str, list[dict[str, Any]]] = {}
    for item in chunk_summaries:
        chapter_to_packets.setdefault(item["chapter_title"], []).append(item)

    chapter_summaries: list[dict[str, Any]] = []
    for chapter_title, items in chapter_to_packets.items():
        parsed = client.call_json(
            stage=f"chapter:{chapter_title}",
            model=profile["chapter_model"],
            instructions=chapter_stage_instructions(contract),
            user_text=build_chapter_user_text(book_title, chapter_title, items, book_map=book_map),
            schema_name="book_chapter_summary",
            schema=CHAPTER_SCHEMA,
            reasoning_effort=profile["chapter_reasoning_effort"],
        )
        chapter_summaries.append(enrich_chapter_summary(parsed, book_map))

    final_summary = client.call_json(
        stage="final",
        model=profile["final_model"],
        instructions=final_stage_instructions(contract),
        user_text=build_final_user_text(book_title, chapter_summaries, book_map=book_map),
        schema_name="book_final_summary",
        schema=FINAL_SCHEMA,
        reasoning_effort=profile["final_reasoning_effort"],
    )

    verification_issues: list[dict[str, str]] = []
    if profile["verify"]:
        verified = client.call_json(
            stage="verify",
            model=profile["verify_model"],
            instructions=verify_stage_instructions(contract),
            user_text=build_verify_user_text(final_summary, chapter_summaries, book_map=book_map),
            schema_name="book_verified_summary",
            schema=VERIFY_SCHEMA,
            reasoning_effort=profile["verify_reasoning_effort"],
        )
        verification_issues = verified.pop("issues", [])
        final_summary = verified

    output_dir.mkdir(parents=True, exist_ok=True)
    output_base = output_dir / slugify(path.stem)
    markdown_path = output_base.with_suffix(".summary.md")
    json_path = output_base.with_suffix(".summary.json")

    markdown_path.write_text(render_markdown(final_summary, verification_issues), encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "stats": run_stats,
                "sections": [asdict(section) for section in sections],
                "packets": [asdict(packet) for packet in packets],
                "book_map": book_map,
                "chunk_summaries": chunk_summaries,
                "chapter_summaries": chapter_summaries,
                "verification_issues": verification_issues,
                "final_summary": final_summary,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "mode": "run",
        "stats": run_stats,
        "markdown_output": str(markdown_path),
        "json_output": str(json_path),
        "verification_issue_count": len(verification_issues),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Structure-first OpenAI book summarizer for OCR-derived Markdown books."
    )
    parser.add_argument("paths", nargs="+", help="Markdown files to summarize")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="compromise")
    parser.add_argument("--prompt-file", type=Path, default=PROMPT_FILE)
    parser.add_argument("--output-dir", type=Path, default=Path("summaries"))
    parser.add_argument("--cache-dir", type=Path, default=Path(".summary_cache"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chunk-model")
    parser.add_argument("--chapter-model")
    parser.add_argument("--final-model")
    parser.add_argument("--verify-model")
    parser.add_argument("--target-chunk-tokens", type=int)
    parser.add_argument("--max-chunk-tokens", type=int)
    parser.add_argument("--disable-verify", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    overrides = {
        "chunk_model": args.chunk_model,
        "chapter_model": args.chapter_model,
        "final_model": args.final_model,
        "verify_model": args.verify_model,
        "target_chunk_tokens": args.target_chunk_tokens,
        "max_chunk_tokens": args.max_chunk_tokens,
        "verify": False if args.disable_verify else None,
    }

    results = []
    for raw_path in args.paths:
        path = Path(raw_path)
        result = summarise_file(
            path,
            output_dir=args.output_dir,
            cache_root=args.cache_dir,
            prompt_file=args.prompt_file,
            profile_name=args.profile,
            overrides=overrides,
            dry_run=args.dry_run,
        )
        results.append(result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())

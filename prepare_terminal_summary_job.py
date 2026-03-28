#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path
from typing import Any

from summarize_book import (
    CHAPTER_SCHEMA,
    CHUNK_SCHEMA,
    FINAL_SCHEMA,
    LEGACY_CHAPTER_SCHEMA,
    LEGACY_CHUNK_SCHEMA,
    Packet,
    Section,
    VERIFY_SCHEMA,
    TokenCounter,
    build_book_map,
    build_chapter_user_text,
    build_final_user_text,
    build_chunk_user_text,
    build_packets,
    build_verify_user_text,
    chapter_stage_instructions,
    chunk_stage_instructions,
    enrich_chapter_summary,
    enrich_chunk_summary,
    extract_book_title,
    final_stage_instructions,
    parse_sections,
    prompt_contract,
    render_book_map_markdown,
    render_markdown,
    slugify,
    validate_payload_or_raise,
    verify_stage_instructions,
)


DEFAULT_JOBS_DIR = Path("summary_jobs")
PROMPT_FILE = Path(__file__).resolve().parent / "prompts" / "book_summary_compromise.md"

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "because",
    "by",
    "for",
    "from",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "more",
    "not",
    "of",
    "on",
    "or",
    "so",
    "than",
    "that",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "to",
    "through",
    "was",
    "we",
    "what",
    "when",
    "which",
    "with",
    "you",
}

SUPPLEMENTAL_MARKERS = (
    "sources",
    "recommended reading",
    "reading",
    "timeline",
    "about the author",
    "want more",
    "appreciation",
    "bonus",
)

EVALUATIVE_MARKERS = (
    "brilliant",
    "visionary",
    "genius",
    "remarkable",
    "inspiring",
    "iconic",
    "legendary",
    "masterful",
    "extraordinary",
    "amazing",
)

BOOK_FORM_MARKERS = (
    "compilation",
    "compiled",
    "curated",
    "manual",
    "guide",
    "edited",
    "public words",
    "public statements",
)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def json_template_for_schema(schema: dict[str, Any]) -> Any:
    expected_type = schema.get("type")
    if expected_type == "object":
        return {
            key: json_template_for_schema(subschema)
            for key, subschema in schema.get("properties", {}).items()
        }
    if expected_type == "array":
        return []
    if expected_type == "string":
        enum_values = schema.get("enum")
        if enum_values:
            return " | ".join(str(item) for item in enum_values)
        return ""
    if expected_type == "integer":
        minimum = schema.get("minimum")
        if isinstance(minimum, int):
            return minimum
        return 0
    return None


def render_output_contract(schema_name: str, schema: dict[str, Any], template_payload: Any | None = None) -> str:
    template = template_payload if template_payload is not None else json_template_for_schema(schema)
    return f"""## Output Contract

Return only strict JSON matching this schema name and key structure.
Do not add keys, remove keys, or wrap the JSON in commentary.

Schema name: `{schema_name}`

Use this JSON template and replace the placeholder values:

```json
{json.dumps(template, ensure_ascii=False, indent=2)}
```"""


def stage_prompt(
    instructions: str,
    schema_name: str,
    schema: dict[str, Any],
    user_text: str,
    *,
    template_payload: Any | None = None,
) -> str:
    return f"""# System Instructions

{instructions}

{render_output_contract(schema_name, schema, template_payload=template_payload)}

# User Input

{user_text}
"""


def save_manifest(job_dir: Path, manifest: dict[str, Any]) -> None:
    write_json(job_dir / "manifest.json", manifest)


def load_manifest(job_dir: Path) -> dict[str, Any]:
    return load_json(job_dir / "manifest.json")


def is_stage_output_path(path: Path) -> bool:
    return path.suffix == ".json" and not path.name.endswith(".template.json")


def stage_output_paths(outputs_dir: Path) -> list[Path]:
    return sorted(path for path in outputs_dir.glob("*.json") if is_stage_output_path(path))


def collect_stage_outputs(outputs_dir: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in stage_output_paths(outputs_dir):
        payload = load_json(path)
        if isinstance(payload, dict):
            items.append(payload)
    return items


def template_path_for_output(path: Path) -> Path:
    return path.with_name(f"{path.stem}.template.json")


def write_template(path: Path, payload: Any) -> None:
    write_json(template_path_for_output(path), payload)


def chapter_output_name(chapter_title: str) -> str:
    safe = slugify(chapter_title)
    return safe if safe else "chapter"


def packet_profile_for_id(book_map: dict[str, Any], packet_id: str) -> dict[str, Any]:
    return next(
        (item for item in book_map.get("packet_profiles", []) if item.get("packet_id") == packet_id),
        {},
    )


def chapter_profile_for_title(book_map: dict[str, Any], chapter_title: str) -> dict[str, Any]:
    return next(
        (item for item in book_map.get("chapter_profiles", []) if item.get("chapter_title") == chapter_title),
        {},
    )


def materialize_book_map(manifest: dict[str, Any]) -> dict[str, Any]:
    if isinstance(manifest.get("book_map"), dict):
        return manifest["book_map"]
    sections = [Section(**item) for item in manifest.get("sections", [])]
    packets = [Packet(**item) for item in manifest.get("packets", [])]
    return build_book_map(manifest["book_title"], sections, packets)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()).casefold()


def significant_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", normalize_text(text))
        if len(token) >= 4 and token not in STOPWORDS
    }


def shares_grounding_tokens(text: str, candidates: list[str]) -> bool:
    source_tokens = significant_tokens(text)
    if not source_tokens:
        return True
    for candidate in candidates:
        if source_tokens & significant_tokens(candidate):
            return True
    return False


def label_looks_supplemental(label: str) -> bool:
    normalized = normalize_text(label)
    return any(marker in normalized for marker in SUPPLEMENTAL_MARKERS)


def review_book_map(book_map: dict[str, Any]) -> dict[str, Any]:
    warnings: list[str] = []
    for packet in book_map.get("packet_profiles", []):
        labels = packet.get("covered_sections", []) or []
        has_supplemental = any(label_looks_supplemental(label) for label in labels)
        has_non_supplemental = any(not label_looks_supplemental(label) for label in labels)
        if has_supplemental and has_non_supplemental:
            warnings.append(
                f"{packet['packet_id']}: packet mixes likely supplemental/endmatter sections with core chapter material; consider splitting earlier"
            )
        if packet.get("packet_kind") == "mixed" and int(packet.get("priority_weight", 3)) >= 3:
            warnings.append(
                f"{packet['packet_id']}: packet is still classified as mixed at priority {packet.get('priority_weight')}; review weighting before summarizing"
            )
    for chapter in book_map.get("chapter_profiles", []):
        if chapter.get("chapter_kind") == "mixed" and int(chapter.get("priority_weight", 3)) >= 3:
            warnings.append(
                f"{chapter['chapter_title']}: chapter remains mixed in the map; summaries may need extra proportion judgment"
            )
        if chapter.get("chapter_kind") in {"reference", "endmatter"} and int(chapter.get("priority_weight", 3)) >= 4:
            warnings.append(
                f"{chapter['chapter_title']}: chapter is classified as documentary despite high priority; review upstream packet splitting or weighting"
            )
    return {"warning_count": len(warnings), "warnings": warnings}


def sync_book_map_metadata(manifest: dict[str, Any], book_map: dict[str, Any], review: dict[str, Any]) -> None:
    manifest["book_map"] = book_map
    manifest["packet_profiles"] = list(book_map.get("packet_profiles", []))
    manifest["chapter_profiles"] = list(book_map.get("chapter_profiles", []))
    manifest["map_review"] = review


def write_map_review(job_dir: Path, manifest: dict[str, Any], book_map: dict[str, Any]) -> dict[str, Any]:
    review = review_book_map(book_map)
    write_json(job_dir / "map_review.json", review)
    write_text(job_dir / "MAP_REVIEW.md", render_map_review_markdown(book_map, review))
    sync_book_map_metadata(manifest, book_map, review)
    return review


def render_map_review_markdown(book_map: dict[str, Any], review: dict[str, Any]) -> str:
    lines = [f"# {book_map['book_title']} - Map Review", ""]
    if review.get("warnings"):
        lines.extend(
            [
                "## Warnings",
                "",
                "These are pre-summary heuristics to review before completing chunk prompts.",
                "",
            ]
        )
        for warning in review["warnings"]:
            lines.append(f"- {warning}")
    else:
        lines.extend(["No map review warnings were detected.", ""])
    lines.extend(
        [
            "",
            "## Guidance",
            "",
            "- Use these warnings as workflow guidance, not as evidence about the book itself.",
            "- If a packet mixes core material with supplemental sections, give the core material the space and compress the supplemental sections aggressively.",
            "- Re-run `python3 prepare_terminal_summary_job.py review-map \"<JOB_DIR>\"` after changing the workflow code or re-initializing a job.",
            "",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def load_stage_payload(
    path: Path,
    *,
    schema_options: list[tuple[str, dict[str, Any]]],
    context: str,
) -> tuple[dict[str, Any], str]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"{context} must be a JSON object: {path}")
    matched = validate_payload_or_raise(payload, schema_options, context)
    return payload, matched


def schema_projection(payload: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    properties = schema.get("properties", {})
    return {key: payload[key] for key in properties if key in payload}


def chunk_template_payload(packet: Packet, book_map: dict[str, Any]) -> dict[str, Any]:
    profile = packet_profile_for_id(book_map, packet.packet_id)
    return {
        "chapter_title": packet.chapter_title,
        "covered_sections": packet.section_labels[:],
        "summary": "",
        "main_points": [],
        "examples_or_evidence": [],
        "ambiguity_flags": [],
        "confidence": "medium",
        "packet_kind": profile.get("packet_kind", "mixed"),
        "role_in_book": profile.get("role_in_book", ""),
        "primary_claims": [],
        "methods_or_principles": [],
        "story_events": [],
        "priority_weight": int(profile.get("priority_weight", 3)),
    }


def chapter_template_payload(chapter_title: str, book_map: dict[str, Any]) -> dict[str, Any]:
    profile = chapter_profile_for_title(book_map, chapter_title)
    return {
        "chapter_title": chapter_title,
        "summary": "",
        "major_points": [],
        "notable_examples": [],
        "ambiguity_flags": [],
        "confidence": "medium",
        "chapter_kind": profile.get("chapter_kind", "mixed"),
        "chapter_thesis": "",
        "priority_weight": int(profile.get("priority_weight", 3)),
        "primary_claims": [],
        "methods_or_principles": [],
        "story_progression": [],
        "what_to_preserve_in_final": [],
    }


def final_template_payload(book_title: str, chapter_entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "book_title": book_title,
        "executive_summary": "",
        "central_thesis": "",
        "structural_overview": "",
        "chapter_summaries": [
            {
                "chapter_title": entry["chapter_title"],
                "summary": "",
            }
            for entry in chapter_entries
        ],
        "major_themes": [],
        "notable_examples": [],
        "practical_takeaways": [],
        "faithfulness_notes": [],
    }


def verify_template_payload(book_title: str, chapter_entries: list[dict[str, Any]]) -> dict[str, Any]:
    payload = final_template_payload(book_title, chapter_entries)
    return {"issues": [], **payload}


def word_count(value: Any) -> int:
    return len(str(value or "").split())


def string_list(values: Any) -> list[str]:
    return [str(item).strip() for item in values or [] if str(item).strip()]


def chapter_grounding_texts(payload: dict[str, Any]) -> list[str]:
    return [
        str(payload.get("summary", "")),
        str(payload.get("chapter_thesis", "")),
        *string_list(payload.get("major_points")),
        *string_list(payload.get("primary_claims")),
        *string_list(payload.get("methods_or_principles")),
        *string_list(payload.get("story_progression")),
        *string_list(payload.get("notable_examples")),
        *string_list(payload.get("what_to_preserve_in_final")),
    ]


def final_grounding_texts(payload: dict[str, Any]) -> list[str]:
    chapter_summaries = payload.get("chapter_summaries", []) or []
    return [
        str(payload.get("executive_summary", "")),
        str(payload.get("central_thesis", "")),
        str(payload.get("structural_overview", "")),
        *[str(item.get("summary", "")) for item in chapter_summaries if isinstance(item, dict)],
        *string_list(payload.get("major_themes")),
        *string_list(payload.get("notable_examples")),
        *string_list(payload.get("practical_takeaways")),
        *string_list(payload.get("faithfulness_notes")),
    ]


def mentions_uncertainty(faithfulness_notes: list[str]) -> bool:
    haystack = " ".join(faithfulness_notes).casefold()
    return any(
        marker in haystack
        for marker in [
            "ocr",
            "unclear",
            "ambigu",
            "truncat",
            "damaged",
            "noisy",
            "illegible",
            "run together",
        ]
    )


def has_marker(text: str, markers: tuple[str, ...]) -> bool:
    normalized = normalize_text(text)
    return any(marker in normalized for marker in markers)


def contains_evaluative_language(texts: list[str]) -> bool:
    return any(has_marker(text, EVALUATIVE_MARKERS) for text in texts if text.strip())


def quality_warnings_for_chunk(payload: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    summary_words = word_count(payload.get("summary", ""))
    priority_weight = int(payload.get("priority_weight", 3) or 3)
    packet_kind = str(payload.get("packet_kind", "mixed"))
    if payload.get("packet_kind") in {"reference", "endmatter"} and summary_words > 180:
        warnings.append("reference/endmatter chunk summary looks too long for its priority")
    if priority_weight >= 4 and summary_words < 90:
        warnings.append("high-priority chunk summary looks too short for its weighting")
    if priority_weight >= 4 and len(string_list(payload.get("main_points"))) < 3:
        warnings.append("high-priority chunk should usually preserve at least three main points")
    if packet_kind == "core_argument" and not string_list(payload.get("primary_claims")):
        warnings.append("core-argument chunk is missing primary_claims")
    if packet_kind == "core_argument" and not string_list(payload.get("methods_or_principles")):
        warnings.append("core-argument chunk is missing methods_or_principles")
    if packet_kind == "case_study" and not string_list(payload.get("story_events")):
        warnings.append("case-study chunk is missing story_events")
    if packet_kind in {"reference", "endmatter"} and len(string_list(payload.get("main_points"))) > 4:
        warnings.append("reference/endmatter chunk preserves too many points for compressed material")
    if not payload.get("covered_sections"):
        warnings.append("chunk output is missing covered_sections content")
    return warnings


def quality_warnings_for_chapter(payload: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    chapter_kind = str(payload.get("chapter_kind", "mixed"))
    priority_weight = int(payload.get("priority_weight", 3) or 3)
    summary_words = word_count(payload.get("summary", ""))
    if chapter_kind in {"reference", "endmatter"} and summary_words > 220:
        warnings.append("reference/endmatter chapter summary looks too long for its priority")
    if priority_weight >= 4 and summary_words < 140:
        warnings.append("high-priority chapter summary looks too short for its weighting")
    if priority_weight >= 4 and len(string_list(payload.get("major_points"))) < 3:
        warnings.append("high-priority chapter should usually preserve at least three major points")
    if chapter_kind == "case_study" and not string_list(payload.get("story_progression")):
        warnings.append("case-study chapter is missing story_progression")
    if chapter_kind == "core_argument" and not string_list(payload.get("methods_or_principles")):
        warnings.append("core-argument chapter is missing methods_or_principles")
    if chapter_kind in {"reference", "endmatter"} and len(string_list(payload.get("major_points"))) > 5:
        warnings.append("reference/endmatter chapter preserves too many major points for compressed material")
    if not payload.get("what_to_preserve_in_final"):
        warnings.append("chapter output is missing what_to_preserve_in_final guidance")
    return warnings


def final_quality_warnings(
    payload: dict[str, Any],
    chapter_summaries: list[dict[str, Any]],
    book_map: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    executive_summary = str(payload.get("executive_summary", ""))
    structural_overview = str(payload.get("structural_overview", ""))
    final_chapters = {
        str(item.get("chapter_title", "")): item
        for item in payload.get("chapter_summaries", [])
        if isinstance(item, dict) and item.get("chapter_title")
    }
    final_text_pool = final_grounding_texts(payload)
    chapter_lookup = {item["chapter_title"]: item for item in chapter_summaries}
    source_example_pool = [
        *[example for item in chapter_summaries for example in string_list(item.get("notable_examples"))],
        *[example for item in chapter_summaries for example in string_list(item.get("story_progression"))],
    ]

    if word_count(executive_summary) < 180:
        warnings.append("executive_summary looks too brief for a publication-quality book synthesis")
    if word_count(structural_overview) < 70:
        warnings.append("structural_overview should more clearly explain how the book is organized")
    if len(string_list(payload.get("major_themes"))) < 5:
        warnings.append("major_themes looks too thin; preserve more of the book's recurring ideas")
    if len(string_list(payload.get("notable_examples"))) < 6:
        warnings.append("notable_examples looks too thin; preserve more representative episodes or examples")
    if len(string_list(payload.get("practical_takeaways"))) < 5:
        warnings.append("practical_takeaways looks too thin for a substantial final summary")

    if contains_evaluative_language(
        [
            executive_summary,
            str(payload.get("central_thesis", "")),
            structural_overview,
            *[str(item.get("summary", "")) for item in final_chapters.values()],
            *string_list(payload.get("major_themes")),
            *string_list(payload.get("practical_takeaways")),
        ]
    ):
        warnings.append("final summary contains evaluative language that may drift into admiration or critique")

    form_signaled_upstream = any(
        has_marker(" ".join(chapter_grounding_texts(chapter)), BOOK_FORM_MARKERS) for chapter in chapter_summaries
    )
    if form_signaled_upstream and not has_marker(executive_summary, BOOK_FORM_MARKERS):
        warnings.append("executive_summary should identify what kind of book this is when the source material does so")

    for chapter in chapter_summaries:
        chapter_title = str(chapter.get("chapter_title", ""))
        final_item = final_chapters.get(chapter_title)
        priority_weight = int(chapter.get("priority_weight", 3) or 3)
        if final_item is None:
            warnings.append(f"final summary is missing chapter coverage for {chapter_title}")
            continue
        if priority_weight <= 2 and word_count(final_item.get("summary", "")) > 170:
            warnings.append(f"{chapter_title}: low-priority chapter summary looks over-expanded in the final synthesis")
        if priority_weight >= 4 and word_count(final_item.get("summary", "")) < 110:
            warnings.append(f"{chapter_title}: high-priority chapter summary looks too compressed in the final synthesis")
        if not shares_grounding_tokens(final_item.get("summary", ""), chapter_grounding_texts(chapter)):
            warnings.append(f"{chapter_title}: final chapter summary may not be grounded in the chapter-stage evidence")

        preserve_items = string_list(chapter.get("what_to_preserve_in_final"))
        missing_preserve = [
            item
            for item in preserve_items
            if not shares_grounding_tokens(item, [str(final_item.get("summary", "")), *final_text_pool])
        ]
        if priority_weight >= 4 and len(missing_preserve) >= 2:
            warnings.append(
                f"{chapter_title}: final synthesis may be under-preserving key chapter material ({'; '.join(missing_preserve[:2])})"
            )
        elif preserve_items and len(missing_preserve) == len(preserve_items):
            warnings.append(f"{chapter_title}: none of the chapter preservation cues appear in the final synthesis")

    for chapter_title in final_chapters:
        if chapter_title not in chapter_lookup:
            warnings.append(f"final summary includes an unexpected chapter title: {chapter_title}")

    for example in string_list(payload.get("notable_examples")):
        if source_example_pool and not shares_grounding_tokens(example, source_example_pool):
            warnings.append(f"final notable example may not be grounded in chapter examples: {example}")

    ambiguity_present = any(string_list(item.get("ambiguity_flags")) for item in chapter_summaries)
    if ambiguity_present and not mentions_uncertainty(string_list(payload.get("faithfulness_notes"))):
        warnings.append("final faithfulness_notes should acknowledge chapter-level ambiguity or OCR uncertainty")

    chapter_profiles = {item["chapter_title"]: item for item in book_map.get("chapter_profiles", [])}
    if len(final_chapters) != len(chapter_profiles):
        warnings.append("final summary chapter count does not match the chapter profiles in the book map")

    return warnings


def make_job(
    source_path: Path,
    jobs_dir: Path,
    prompt_file: Path,
    target_chunk_tokens: int,
    max_chunk_tokens: int,
    overwrite: bool,
) -> dict[str, Any]:
    markdown = source_path.read_text(encoding="utf-8")
    book_title = extract_book_title(markdown, source_path)
    counter = TokenCounter()
    sections = parse_sections(markdown, book_title, counter)
    packets = build_packets(
        sections,
        counter=counter,
        target_chunk_tokens=target_chunk_tokens,
        max_chunk_tokens=max_chunk_tokens,
    )
    book_map = build_book_map(book_title, sections, packets)

    book_slug = slugify(source_path.stem)
    job_dir = jobs_dir / book_slug
    if job_dir.exists() and overwrite:
        shutil.rmtree(job_dir)
    job_dir.mkdir(parents=True, exist_ok=True)

    contract = prompt_contract(prompt_file)
    write_text(job_dir / "contract.md", contract + "\n")
    write_text(job_dir / "BOOK_MAP.md", render_book_map_markdown(book_map))
    write_json(job_dir / "book_map.json", book_map)

    prompts_dir = job_dir / "prompts"
    outputs_dir = job_dir / "outputs"
    chunk_prompts_dir = prompts_dir / "chunks"
    chunk_outputs_dir = outputs_dir / "chunks"
    chapter_prompts_dir = prompts_dir / "chapters"
    chapter_outputs_dir = outputs_dir / "chapters"
    final_prompt_dir = prompts_dir / "final"
    final_output_dir = outputs_dir / "final"
    verify_prompt_dir = prompts_dir / "verify"
    verify_output_dir = outputs_dir / "verify"

    for directory in [
        chunk_prompts_dir,
        chunk_outputs_dir,
        chapter_prompts_dir,
        chapter_outputs_dir,
        final_prompt_dir,
        final_output_dir,
        verify_prompt_dir,
        verify_output_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    manifest = {
        "job_version": 3,
        "source_path": str(source_path),
        "book_title": book_title,
        "book_slug": book_slug,
        "prompt_file": str(prompt_file),
        "token_counter": counter.mode,
        "target_chunk_tokens": target_chunk_tokens,
        "max_chunk_tokens": max_chunk_tokens,
        "section_count": len(sections),
        "packet_count": len(packets),
        "sections": [section.__dict__ for section in sections],
        "packets": [packet.__dict__ for packet in packets],
        "status": {
            "chunks_prepared": True,
            "chapters_prepared": False,
            "final_prepared": False,
            "verify_prepared": False,
            "assembled": False,
        },
    }
    review = write_map_review(job_dir, manifest, book_map)
    save_manifest(job_dir, manifest)

    chunk_instructions = chunk_stage_instructions(contract)
    for packet in packets:
        output_path = chunk_outputs_dir / f"{packet.packet_id}.json"
        prompt_text = stage_prompt(
            instructions=chunk_instructions,
            schema_name="book_chunk_summary",
            schema=CHUNK_SCHEMA,
            user_text=build_chunk_user_text(book_title, packet, book_map=book_map),
            template_payload=chunk_template_payload(packet, book_map),
        )
        write_text(chunk_prompts_dir / f"{packet.packet_id}.md", prompt_text)
        write_template(output_path, chunk_template_payload(packet, book_map))

    runbook = f"""# Terminal LLM Summary Job

Book: {book_title}
Source: {source_path}

Read these first:

- `{job_dir / 'RUNBOOK.md'}`
- `{job_dir / 'manifest.json'}`
- `{job_dir / 'BOOK_MAP.md'}`
- `{job_dir / 'MAP_REVIEW.md'}`

Workflow helpers:

- `python3 prepare_terminal_summary_job.py status "{job_dir}"`
- `python3 prepare_terminal_summary_job.py run-next "{job_dir}"`
- `python3 prepare_terminal_summary_job.py review-map "{job_dir}"`

## Stage 1: Chunk Summaries

There are {len(packets)} chunk prompt files in:

`{chunk_prompts_dir}`

For each prompt file:

1. Paste the full file into your terminal LLM.
2. Save the model's JSON-only reply into:
   `{chunk_outputs_dir}/<same-name>.json`
3. Use the matching template file in:
   `{chunk_outputs_dir}/<same-name>.template.json`
   as a prefilled skeleton.

When all chunk outputs are saved, run:

```bash
python3 prepare_terminal_summary_job.py build-chapters "{job_dir}"
python3 prepare_terminal_summary_job.py audit-job "{job_dir}"
```

## Stage 2: Chapter Summaries

After running `build-chapters`, repeat the same process for chapter prompts:

`{chapter_prompts_dir}`

Save outputs to:

`{chapter_outputs_dir}`

Each chapter output also has a prefilled template in:

`{chapter_outputs_dir}/<same-name>.template.json`

Then run:

```bash
python3 prepare_terminal_summary_job.py build-final "{job_dir}"
python3 prepare_terminal_summary_job.py audit-job "{job_dir}"
```

## Stage 3: Final Summary

Paste the final prompt from:

`{final_prompt_dir}/final.md`

Save the JSON reply to:

`{final_output_dir}/final.json`

There is also a prefilled template at:

`{final_output_dir}/final.template.json`

Then run:

```bash
python3 prepare_terminal_summary_job.py build-verify "{job_dir}"
python3 prepare_terminal_summary_job.py audit-job "{job_dir}"
```

## Stage 4: Verification

Paste:

`{verify_prompt_dir}/verify.md`

Save the JSON reply to:

`{verify_output_dir}/verify.json`

There is also a prefilled template at:

`{verify_output_dir}/verify.template.json`

Then assemble the final Markdown output:

```bash
python3 prepare_terminal_summary_job.py assemble "{job_dir}" --final-path "<OUTPUT_SUMMARY.md>" --clean
python3 prepare_terminal_summary_job.py audit-job "{job_dir}"
```
"""
    write_text(job_dir / "RUNBOOK.md", runbook)
    return {
        "job_dir": str(job_dir),
        "packet_count": len(packets),
        "book_title": book_title,
        "map_review_warning_count": review["warning_count"],
    }


def build_chapters(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    review = write_map_review(job_dir, manifest, book_map)
    contract = prompt_contract(Path(manifest["prompt_file"]))
    chunk_outputs_dir = job_dir / "outputs" / "chunks"
    chunk_paths = stage_output_paths(chunk_outputs_dir)
    if len(chunk_paths) != len(manifest["packets"]):
        raise RuntimeError(
            f"Expected {len(manifest['packets'])} chunk outputs, found {len(chunk_paths)} in {chunk_outputs_dir}"
        )

    packet_lookup = {packet["packet_id"]: packet for packet in manifest["packets"]}
    chunk_summaries: list[dict[str, Any]] = []
    for path in chunk_paths:
        payload, _matched = load_stage_payload(
            path,
            schema_options=[
                ("book_chunk_summary", CHUNK_SCHEMA),
                ("legacy_book_chunk_summary", LEGACY_CHUNK_SCHEMA),
            ],
            context=f"Chunk output {path}",
        )
        packet_id = payload.get("packet_id") or path.stem
        if not packet_id or packet_id not in packet_lookup:
            raise RuntimeError("Each chunk output must be saved under its packet id filename, like p0001.json")
        payload["packet_id"] = packet_id
        payload = enrich_chunk_summary(payload, book_map)
        validate_payload_or_raise(
            schema_projection(payload, CHUNK_SCHEMA),
            [("book_chunk_summary", CHUNK_SCHEMA)],
            f"Enriched chunk output {path}",
        )
        chunk_summaries.append(payload)

    chapter_to_packets: dict[str, list[dict[str, Any]]] = {}
    for payload in sorted(chunk_summaries, key=lambda item: item["packet_id"]):
        chapter_to_packets.setdefault(payload["chapter_title"], []).append(payload)

    chapter_prompts_dir = job_dir / "prompts" / "chapters"
    chapter_prompts_dir.mkdir(parents=True, exist_ok=True)
    instructions = chapter_stage_instructions(contract)

    chapter_entries: list[dict[str, Any]] = []
    for index, (chapter_title, items) in enumerate(chapter_to_packets.items(), 1):
        name = f"c{index:03d}-{chapter_output_name(chapter_title)}"
        output_path = job_dir / "outputs" / "chapters" / f"{name}.json"
        prompt_text = stage_prompt(
            instructions=instructions,
            schema_name="book_chapter_summary",
            schema=CHAPTER_SCHEMA,
            user_text=build_chapter_user_text(
                manifest["book_title"],
                chapter_title,
                items,
                book_map=book_map,
            ),
            template_payload=chapter_template_payload(chapter_title, book_map),
        )
        write_text(chapter_prompts_dir / f"{name}.md", prompt_text)
        write_template(output_path, chapter_template_payload(chapter_title, book_map))
        chapter_entries.append({"chapter_title": chapter_title, "prompt_name": name, "packet_count": len(items)})

    manifest["status"]["chapters_prepared"] = True
    manifest["chapters"] = chapter_entries
    save_manifest(job_dir, manifest)
    return {
        "job_dir": str(job_dir),
        "chapter_prompt_count": len(chapter_entries),
        "map_review_warning_count": review["warning_count"],
    }


def build_final(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    write_map_review(job_dir, manifest, book_map)
    contract = prompt_contract(Path(manifest["prompt_file"]))
    chapter_outputs_dir = job_dir / "outputs" / "chapters"
    chapter_summaries: list[dict[str, Any]] = []
    for path in stage_output_paths(chapter_outputs_dir):
        payload, _matched = load_stage_payload(
            path,
            schema_options=[
                ("book_chapter_summary", CHAPTER_SCHEMA),
                ("legacy_book_chapter_summary", LEGACY_CHAPTER_SCHEMA),
            ],
            context=f"Chapter output {path}",
        )
        payload = enrich_chapter_summary(payload, book_map)
        validate_payload_or_raise(payload, [("book_chapter_summary", CHAPTER_SCHEMA)], f"Enriched chapter output {path}")
        chapter_summaries.append(payload)
    expected = len(manifest.get("chapters", []))
    if expected == 0:
        raise RuntimeError("No chapters recorded in manifest. Run build-chapters first.")
    if len(chapter_summaries) != expected:
        raise RuntimeError(
            f"Expected {expected} chapter outputs, found {len(chapter_summaries)} in {chapter_outputs_dir}"
        )

    prompt_text = stage_prompt(
        instructions=final_stage_instructions(contract),
        schema_name="book_final_summary",
        schema=FINAL_SCHEMA,
        user_text=build_final_user_text(manifest["book_title"], chapter_summaries, book_map=book_map),
        template_payload=final_template_payload(manifest["book_title"], manifest["chapters"]),
    )
    write_text(job_dir / "prompts" / "final" / "final.md", prompt_text)
    write_template(
        job_dir / "outputs" / "final" / "final.json",
        final_template_payload(manifest["book_title"], manifest["chapters"]),
    )
    manifest["status"]["final_prepared"] = True
    save_manifest(job_dir, manifest)
    return {"job_dir": str(job_dir), "final_prompt": str(job_dir / 'prompts' / 'final' / 'final.md')}


def build_verify(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    write_map_review(job_dir, manifest, book_map)
    contract = prompt_contract(Path(manifest["prompt_file"]))
    final_output_path = job_dir / "outputs" / "final" / "final.json"
    chapter_outputs_dir = job_dir / "outputs" / "chapters"
    if not final_output_path.exists():
        raise RuntimeError(f"Missing final output: {final_output_path}")
    draft_summary, _matched = load_stage_payload(
        final_output_path,
        schema_options=[("book_final_summary", FINAL_SCHEMA)],
        context=f"Final output {final_output_path}",
    )
    chapter_summaries: list[dict[str, Any]] = []
    for path in stage_output_paths(chapter_outputs_dir):
        payload, _matched = load_stage_payload(
            path,
            schema_options=[
                ("book_chapter_summary", CHAPTER_SCHEMA),
                ("legacy_book_chapter_summary", LEGACY_CHAPTER_SCHEMA),
            ],
            context=f"Chapter output {path}",
        )
        payload = enrich_chapter_summary(payload, book_map)
        validate_payload_or_raise(payload, [("book_chapter_summary", CHAPTER_SCHEMA)], f"Enriched chapter output {path}")
        chapter_summaries.append(payload)

    prompt_text = stage_prompt(
        instructions=verify_stage_instructions(contract),
        schema_name="book_verified_summary",
        schema=VERIFY_SCHEMA,
        user_text=build_verify_user_text(draft_summary, chapter_summaries, book_map=book_map),
        template_payload=verify_template_payload(manifest["book_title"], manifest.get("chapters", [])),
    )
    write_text(job_dir / "prompts" / "verify" / "verify.md", prompt_text)
    write_template(
        job_dir / "outputs" / "verify" / "verify.json",
        verify_template_payload(manifest["book_title"], manifest.get("chapters", [])),
    )
    manifest["status"]["verify_prepared"] = True
    save_manifest(job_dir, manifest)
    return {"job_dir": str(job_dir), "verify_prompt": str(job_dir / 'prompts' / 'verify' / 'verify.md')}


def path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def cleanup_job_dir(job_dir: Path, keep_paths: set[Path]) -> str:
    if not job_dir.exists():
        return "job_dir_already_missing"

    resolved_keep = {path.resolve() for path in keep_paths}
    if resolved_keep and all(not path_is_relative_to(path, job_dir) for path in resolved_keep):
        shutil.rmtree(job_dir)
        return "removed_job_dir"

    def should_keep(path: Path) -> bool:
        resolved = path.resolve()
        return any(keep == resolved or path_is_relative_to(keep, path) for keep in resolved_keep)

    def prune(path: Path) -> None:
        if path.is_dir():
            for child in list(path.iterdir()):
                if should_keep(child):
                    prune(child)
                else:
                    if child.is_dir():
                        shutil.rmtree(child)
                    else:
                        child.unlink()
            if path != job_dir and not any(path.iterdir()):
                path.rmdir()

    prune(job_dir)
    return "removed_one_time_use_artifacts"


def assemble(
    job_dir: Path,
    *,
    final_path: Path | None = None,
    json_path: Path | None = None,
    clean: bool = False,
    keep_provenance_zip: bool = False,
) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    manifest["book_map"] = materialize_book_map(manifest)
    verify_output = job_dir / "outputs" / "verify" / "verify.json"
    final_output = job_dir / "outputs" / "final" / "final.json"
    if verify_output.exists():
        payload, _matched = load_stage_payload(
            verify_output,
            schema_options=[("book_verified_summary", VERIFY_SCHEMA)],
            context=f"Verify output {verify_output}",
        )
        issues = payload.get("issues", [])
        summary = {k: v for k, v in payload.items() if k != "issues"}
    elif final_output.exists():
        payload, _matched = load_stage_payload(
            final_output,
            schema_options=[("book_final_summary", FINAL_SCHEMA)],
            context=f"Final output {final_output}",
        )
        issues = []
        summary = payload
    else:
        raise RuntimeError("Need either outputs/verify/verify.json or outputs/final/final.json to assemble.")

    out_dir = job_dir / "assembled"
    out_dir.mkdir(parents=True, exist_ok=True)
    assembled_markdown = out_dir / "summary.md"
    assembled_json = out_dir / "summary.json"
    write_text(assembled_markdown, render_markdown(summary, issues))
    write_json(assembled_json, {"summary": summary, "issues": issues})

    markdown_output = final_path or assembled_markdown
    if markdown_output != assembled_markdown:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(assembled_markdown, markdown_output)

    if json_path is None and final_path is not None:
        json_path = final_path.with_suffix(".json")
    json_output = json_path or assembled_json
    if json_output != assembled_json:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(assembled_json, json_output)

    manifest["status"]["assembled"] = True
    save_manifest(job_dir, manifest)

    provenance_zip = None
    if keep_provenance_zip:
        archive_base = job_dir.parent / f"{job_dir.name}-provenance"
        provenance_zip = shutil.make_archive(str(archive_base), "zip", root_dir=job_dir)

    cleanup_result = None
    if clean:
        cleanup_result = cleanup_job_dir(job_dir, {markdown_output, json_output})

    result: dict[str, Any] = {
        "job_dir": str(job_dir),
        "markdown_output": str(markdown_output),
        "json_output": str(json_output),
    }
    if provenance_zip:
        result["provenance_zip"] = provenance_zip
    if cleanup_result:
        result["cleanup"] = cleanup_result
    return result


def next_work_item(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    chunk_prompts_dir = job_dir / "prompts" / "chunks"
    chunk_outputs_dir = job_dir / "outputs" / "chunks"
    chapter_prompts_dir = job_dir / "prompts" / "chapters"
    chapter_outputs_dir = job_dir / "outputs" / "chapters"
    final_prompt_path = job_dir / "prompts" / "final" / "final.md"
    final_output_path = job_dir / "outputs" / "final" / "final.json"
    verify_prompt_path = job_dir / "prompts" / "verify" / "verify.md"
    verify_output_path = job_dir / "outputs" / "verify" / "verify.json"
    assembled_markdown = job_dir / "assembled" / "summary.md"

    chunk_prompts = sorted(chunk_prompts_dir.glob("*.md"))
    chunk_outputs = {path.stem for path in stage_output_paths(chunk_outputs_dir)}
    for prompt_path in chunk_prompts:
        if prompt_path.stem not in chunk_outputs:
            output_path = chunk_outputs_dir / f"{prompt_path.stem}.json"
            return {
                "stage": "chunks",
                "action_type": "complete_prompt",
                "prompt_path": str(prompt_path),
                "output_path": str(output_path),
                "template_path": str(template_path_for_output(output_path)),
            }

    if not manifest.get("status", {}).get("chapters_prepared"):
        return {
            "stage": "chapters",
            "action_type": "run_command",
            "command": f'python3 prepare_terminal_summary_job.py build-chapters "{job_dir}"',
        }

    chapter_prompts = sorted(chapter_prompts_dir.glob("*.md"))
    chapter_outputs = {path.stem for path in stage_output_paths(chapter_outputs_dir)}
    for prompt_path in chapter_prompts:
        if prompt_path.stem not in chapter_outputs:
            output_path = chapter_outputs_dir / f"{prompt_path.stem}.json"
            return {
                "stage": "chapters",
                "action_type": "complete_prompt",
                "prompt_path": str(prompt_path),
                "output_path": str(output_path),
                "template_path": str(template_path_for_output(output_path)),
            }

    if not final_prompt_path.exists():
        return {
            "stage": "final",
            "action_type": "run_command",
            "command": f'python3 prepare_terminal_summary_job.py build-final "{job_dir}"',
        }

    if not final_output_path.exists():
        return {
            "stage": "final",
            "action_type": "complete_prompt",
            "prompt_path": str(final_prompt_path),
            "output_path": str(final_output_path),
            "template_path": str(template_path_for_output(final_output_path)),
        }

    if not verify_prompt_path.exists():
        return {
            "stage": "verify",
            "action_type": "run_command",
            "command": f'python3 prepare_terminal_summary_job.py build-verify "{job_dir}"',
        }

    if not verify_output_path.exists():
        return {
            "stage": "verify",
            "action_type": "complete_prompt",
            "prompt_path": str(verify_prompt_path),
            "output_path": str(verify_output_path),
            "template_path": str(template_path_for_output(verify_output_path)),
        }

    if not assembled_markdown.exists():
        return {
            "stage": "assemble",
            "action_type": "run_command",
            "command": f'python3 prepare_terminal_summary_job.py assemble "{job_dir}"',
        }

    return {"stage": "done", "action_type": "complete", "message": "All workflow stages are complete."}


def job_status(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    map_review = manifest.get("map_review", {})
    chunk_prompts_dir = job_dir / "prompts" / "chunks"
    chunk_outputs_dir = job_dir / "outputs" / "chunks"
    chapter_prompts_dir = job_dir / "prompts" / "chapters"
    chapter_outputs_dir = job_dir / "outputs" / "chapters"
    final_output_path = job_dir / "outputs" / "final" / "final.json"
    verify_output_path = job_dir / "outputs" / "verify" / "verify.json"
    assembled_markdown = job_dir / "assembled" / "summary.md"

    return {
        "job_dir": str(job_dir),
        "book_title": manifest["book_title"],
        "map_review_warning_count": int(map_review.get("warning_count", 0)),
        "chunk_progress": {
            "completed": len(stage_output_paths(chunk_outputs_dir)),
            "total": len(list(chunk_prompts_dir.glob("*.md"))),
        },
        "chapter_progress": {
            "completed": len(stage_output_paths(chapter_outputs_dir)),
            "total": len(list(chapter_prompts_dir.glob("*.md"))),
        },
        "final_output_present": final_output_path.exists(),
        "verify_output_present": verify_output_path.exists(),
        "assembled_output_present": assembled_markdown.exists(),
        "status_flags": manifest.get("status", {}),
        "next_action": next_work_item(job_dir),
    }


def refresh_map_review(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    review = write_map_review(job_dir, manifest, book_map)
    save_manifest(job_dir, manifest)
    return {
        "job_dir": str(job_dir),
        "map_review_warning_count": review["warning_count"],
        "map_review_path": str(job_dir / "MAP_REVIEW.md"),
        "map_review_json_path": str(job_dir / "map_review.json"),
    }


def audit_job(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    review = write_map_review(job_dir, manifest, book_map)
    save_manifest(job_dir, manifest)
    warnings: list[str] = []
    stats: dict[str, Any] = {"job_dir": str(job_dir)}
    stats["map_review_warning_count"] = review["warning_count"]

    chunk_outputs_dir = job_dir / "outputs" / "chunks"
    chunk_paths = stage_output_paths(chunk_outputs_dir)
    stats["chunk_output_count"] = len(chunk_paths)
    stats["expected_chunk_count"] = len(manifest.get("packets", []))
    if len(chunk_paths) != stats["expected_chunk_count"]:
        warnings.append(
            f"chunk output count mismatch: expected {stats['expected_chunk_count']}, found {len(chunk_paths)}"
        )
    legacy_chunk_count = 0
    for path in chunk_paths:
        payload, matched = load_stage_payload(
            path,
            schema_options=[
                ("book_chunk_summary", CHUNK_SCHEMA),
                ("legacy_book_chunk_summary", LEGACY_CHUNK_SCHEMA),
            ],
            context=f"Chunk output {path}",
        )
        if matched != "book_chunk_summary":
            legacy_chunk_count += 1
        packet_id = payload.get("packet_id") or path.stem
        payload["packet_id"] = packet_id
        enriched = enrich_chunk_summary(payload, book_map)
        validate_payload_or_raise(
            schema_projection(enriched, CHUNK_SCHEMA),
            [("book_chunk_summary", CHUNK_SCHEMA)],
            f"Enriched chunk output {path}",
        )
        for note in quality_warnings_for_chunk(enriched):
            warnings.append(f"{path.name}: {note}")
    if legacy_chunk_count:
        warnings.append(f"{legacy_chunk_count} chunk outputs still use the legacy schema and are being auto-enriched")

    chapter_outputs_dir = job_dir / "outputs" / "chapters"
    chapter_paths = stage_output_paths(chapter_outputs_dir)
    stats["chapter_output_count"] = len(chapter_paths)
    stats["expected_chapter_count"] = len(manifest.get("chapters", []))
    if stats["expected_chapter_count"] and len(chapter_paths) != stats["expected_chapter_count"]:
        warnings.append(
            f"chapter output count mismatch: expected {stats['expected_chapter_count']}, found {len(chapter_paths)}"
        )
    legacy_chapter_count = 0
    chapter_summaries: list[dict[str, Any]] = []
    for path in chapter_paths:
        payload, matched = load_stage_payload(
            path,
            schema_options=[
                ("book_chapter_summary", CHAPTER_SCHEMA),
                ("legacy_book_chapter_summary", LEGACY_CHAPTER_SCHEMA),
            ],
            context=f"Chapter output {path}",
        )
        if matched != "book_chapter_summary":
            legacy_chapter_count += 1
        enriched = enrich_chapter_summary(payload, book_map)
        validate_payload_or_raise(
            enriched,
            [("book_chapter_summary", CHAPTER_SCHEMA)],
            f"Enriched chapter output {path}",
        )
        chapter_summaries.append(enriched)
        for note in quality_warnings_for_chapter(enriched):
            warnings.append(f"{path.name}: {note}")
    if legacy_chapter_count:
        warnings.append(
            f"{legacy_chapter_count} chapter outputs still use the legacy schema and are being auto-enriched"
        )

    final_output = job_dir / "outputs" / "final" / "final.json"
    if final_output.exists():
        final_payload, _matched = load_stage_payload(
            final_output,
            schema_options=[("book_final_summary", FINAL_SCHEMA)],
            context=f"Final output {final_output}",
        )
        if len(final_payload.get("chapter_summaries", [])) != len(manifest.get("chapters", [])):
            warnings.append("final summary chapter count does not match prepared chapter count")
        for note in final_quality_warnings(final_payload, chapter_summaries, book_map):
            warnings.append(f"final.json: {note}")
    else:
        warnings.append("final output is missing")

    verify_output = job_dir / "outputs" / "verify" / "verify.json"
    if verify_output.exists():
        verify_payload, _matched = load_stage_payload(
            verify_output,
            schema_options=[("book_verified_summary", VERIFY_SCHEMA)],
            context=f"Verify output {verify_output}",
        )
        stats["verification_issue_count"] = len(verify_payload.get("issues", []))
        if final_output.exists():
            verified_summary = {key: value for key, value in verify_payload.items() if key != "issues"}
            if verify_payload.get("issues") and verified_summary == final_payload:
                warnings.append("verify output lists issues but leaves the final summary unchanged")
    else:
        warnings.append("verify output is missing")

    stats["warning_count"] = len(warnings)
    stats["warnings"] = warnings
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare a terminal-LLM book-summary job.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Create a new summarization job with chunk prompts")
    init.add_argument("source_path")
    init.add_argument("--jobs-dir", type=Path, default=DEFAULT_JOBS_DIR)
    init.add_argument("--prompt-file", type=Path, default=PROMPT_FILE)
    init.add_argument("--target-chunk-tokens", type=int, default=6500)
    init.add_argument("--max-chunk-tokens", type=int, default=8500)
    init.add_argument("--overwrite", action="store_true")

    build_chapters_cmd = subparsers.add_parser("build-chapters", help="Create chapter prompts from chunk outputs")
    build_chapters_cmd.add_argument("job_dir", type=Path)

    build_final_cmd = subparsers.add_parser("build-final", help="Create final summary prompt from chapter outputs")
    build_final_cmd.add_argument("job_dir", type=Path)

    build_verify_cmd = subparsers.add_parser("build-verify", help="Create verification prompt from final output")
    build_verify_cmd.add_argument("job_dir", type=Path)

    review_map_cmd = subparsers.add_parser("review-map", help="Refresh the book-map review files for a job")
    review_map_cmd.add_argument("job_dir", type=Path)

    status_cmd = subparsers.add_parser("status", help="Show progress and the next recommended step for a job")
    status_cmd.add_argument("job_dir", type=Path)

    run_next_cmd = subparsers.add_parser("run-next", help="Report the next prompt or command to advance a job")
    run_next_cmd.add_argument("job_dir", type=Path)

    assemble_cmd = subparsers.add_parser("assemble", help="Render final markdown summary from saved outputs")
    assemble_cmd.add_argument("job_dir", type=Path)
    assemble_cmd.add_argument("--final-path", type=Path)
    assemble_cmd.add_argument("--json-path", type=Path)
    assemble_cmd.add_argument("--clean", action="store_true")
    assemble_cmd.add_argument("--keep-provenance-zip", action="store_true")

    audit_cmd = subparsers.add_parser("audit-job", help="Validate saved stage outputs and report workflow warnings")
    audit_cmd.add_argument("job_dir", type=Path)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "init":
        result = make_job(
            source_path=Path(args.source_path),
            jobs_dir=args.jobs_dir,
            prompt_file=args.prompt_file,
            target_chunk_tokens=args.target_chunk_tokens,
            max_chunk_tokens=args.max_chunk_tokens,
            overwrite=args.overwrite,
        )
    elif args.command == "build-chapters":
        result = build_chapters(args.job_dir)
    elif args.command == "build-final":
        result = build_final(args.job_dir)
    elif args.command == "build-verify":
        result = build_verify(args.job_dir)
    elif args.command == "review-map":
        result = refresh_map_review(args.job_dir)
    elif args.command == "status":
        result = job_status(args.job_dir)
    elif args.command == "run-next":
        result = next_work_item(args.job_dir)
    elif args.command == "assemble":
        result = assemble(
            args.job_dir,
            final_path=args.final_path,
            json_path=args.json_path,
            clean=args.clean,
            keep_provenance_zip=args.keep_provenance_zip,
        )
    elif args.command == "audit-job":
        result = audit_job(args.job_dir)
    else:
        raise RuntimeError(f"Unknown command: {args.command}")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

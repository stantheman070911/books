#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def stage_prompt(instructions: str, schema_name: str, schema: dict[str, Any], user_text: str) -> str:
    return f"""# System Instructions

{instructions}

## Output Contract

Return only strict JSON matching this schema name and shape.

Schema name: `{schema_name}`

```json
{json.dumps(schema, ensure_ascii=False, indent=2)}
```

# User Input

{user_text}
"""


def save_manifest(job_dir: Path, manifest: dict[str, Any]) -> None:
    write_json(job_dir / "manifest.json", manifest)


def load_manifest(job_dir: Path) -> dict[str, Any]:
    return load_json(job_dir / "manifest.json")


def collect_stage_outputs(outputs_dir: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in sorted(outputs_dir.glob("*.json")):
        payload = load_json(path)
        if isinstance(payload, dict):
            items.append(payload)
    return items


def chapter_output_name(chapter_title: str) -> str:
    safe = slugify(chapter_title)
    return safe if safe else "chapter"


def materialize_book_map(manifest: dict[str, Any]) -> dict[str, Any]:
    if isinstance(manifest.get("book_map"), dict):
        return manifest["book_map"]
    sections = [Section(**item) for item in manifest.get("sections", [])]
    packets = [Packet(**item) for item in manifest.get("packets", [])]
    return build_book_map(manifest["book_title"], sections, packets)


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


def quality_warnings_for_chunk(payload: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    summary_words = len(str(payload.get("summary", "")).split())
    if payload.get("packet_kind") in {"reference", "endmatter"} and summary_words > 180:
        warnings.append("reference/endmatter chunk summary looks too long for its priority")
    if not payload.get("covered_sections"):
        warnings.append("chunk output is missing covered_sections content")
    return warnings


def quality_warnings_for_chapter(payload: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if payload.get("chapter_kind") in {"reference", "endmatter"} and len(str(payload.get("summary", "")).split()) > 220:
        warnings.append("reference/endmatter chapter summary looks too long for its priority")
    if not payload.get("what_to_preserve_in_final"):
        warnings.append("chapter output is missing what_to_preserve_in_final guidance")
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
        "job_version": 2,
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
        "book_map": book_map,
        "status": {
            "chunks_prepared": True,
            "chapters_prepared": False,
            "final_prepared": False,
            "verify_prepared": False,
            "assembled": False,
        },
    }
    save_manifest(job_dir, manifest)

    chunk_instructions = chunk_stage_instructions(contract)
    for packet in packets:
        prompt_text = stage_prompt(
            instructions=chunk_instructions,
            schema_name="book_chunk_summary",
            schema=CHUNK_SCHEMA,
            user_text=build_chunk_user_text(book_title, packet, book_map=book_map),
        )
        write_text(chunk_prompts_dir / f"{packet.packet_id}.md", prompt_text)

    runbook = f"""# Terminal LLM Summary Job

Book: {book_title}
Source: {source_path}

Read these first:

- `{job_dir / 'RUNBOOK.md'}`
- `{job_dir / 'manifest.json'}`
- `{job_dir / 'BOOK_MAP.md'}`

## Stage 1: Chunk Summaries

There are {len(packets)} chunk prompt files in:

`{chunk_prompts_dir}`

For each prompt file:

1. Paste the full file into your terminal LLM.
2. Save the model's JSON-only reply into:
   `{chunk_outputs_dir}/<same-name>.json`

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

Then assemble the final Markdown output:

```bash
python3 prepare_terminal_summary_job.py assemble "{job_dir}"
python3 prepare_terminal_summary_job.py audit-job "{job_dir}"
```
"""
    write_text(job_dir / "RUNBOOK.md", runbook)
    return {"job_dir": str(job_dir), "packet_count": len(packets), "book_title": book_title}


def build_chapters(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    manifest["book_map"] = book_map
    contract = prompt_contract(Path(manifest["prompt_file"]))
    chunk_outputs_dir = job_dir / "outputs" / "chunks"
    chunk_paths = sorted(chunk_outputs_dir.glob("*.json"))
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
        )
        name = f"c{index:03d}-{chapter_output_name(chapter_title)}"
        write_text(chapter_prompts_dir / f"{name}.md", prompt_text)
        chapter_entries.append({"chapter_title": chapter_title, "prompt_name": name, "packet_count": len(items)})

    manifest["status"]["chapters_prepared"] = True
    manifest["chapters"] = chapter_entries
    save_manifest(job_dir, manifest)
    return {"job_dir": str(job_dir), "chapter_prompt_count": len(chapter_entries)}


def build_final(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    manifest["book_map"] = book_map
    contract = prompt_contract(Path(manifest["prompt_file"]))
    chapter_outputs_dir = job_dir / "outputs" / "chapters"
    chapter_summaries: list[dict[str, Any]] = []
    for path in sorted(chapter_outputs_dir.glob("*.json")):
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
    )
    write_text(job_dir / "prompts" / "final" / "final.md", prompt_text)
    manifest["status"]["final_prepared"] = True
    save_manifest(job_dir, manifest)
    return {"job_dir": str(job_dir), "final_prompt": str(job_dir / 'prompts' / 'final' / 'final.md')}


def build_verify(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    manifest["book_map"] = book_map
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
    for path in sorted(chapter_outputs_dir.glob("*.json")):
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
    )
    write_text(job_dir / "prompts" / "verify" / "verify.md", prompt_text)
    manifest["status"]["verify_prepared"] = True
    save_manifest(job_dir, manifest)
    return {"job_dir": str(job_dir), "verify_prompt": str(job_dir / 'prompts' / 'verify' / 'verify.md')}


def assemble(job_dir: Path) -> dict[str, Any]:
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
    write_text(out_dir / "summary.md", render_markdown(summary, issues))
    write_json(out_dir / "summary.json", {"summary": summary, "issues": issues})
    manifest["status"]["assembled"] = True
    save_manifest(job_dir, manifest)
    return {"job_dir": str(job_dir), "markdown_output": str(out_dir / "summary.md")}


def audit_job(job_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(job_dir)
    book_map = materialize_book_map(manifest)
    warnings: list[str] = []
    stats: dict[str, Any] = {"job_dir": str(job_dir)}

    chunk_outputs_dir = job_dir / "outputs" / "chunks"
    chunk_paths = sorted(chunk_outputs_dir.glob("*.json"))
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
    chapter_paths = sorted(chapter_outputs_dir.glob("*.json"))
    stats["chapter_output_count"] = len(chapter_paths)
    stats["expected_chapter_count"] = len(manifest.get("chapters", []))
    if stats["expected_chapter_count"] and len(chapter_paths) != stats["expected_chapter_count"]:
        warnings.append(
            f"chapter output count mismatch: expected {stats['expected_chapter_count']}, found {len(chapter_paths)}"
        )
    legacy_chapter_count = 0
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

    assemble_cmd = subparsers.add_parser("assemble", help="Render final markdown summary from saved outputs")
    assemble_cmd.add_argument("job_dir", type=Path)

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
    elif args.command == "assemble":
        result = assemble(args.job_dir)
    elif args.command == "audit-job":
        result = audit_job(args.job_dir)
    else:
        raise RuntimeError(f"Unknown command: {args.command}")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

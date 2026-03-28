You are in a repo that contains an upgraded staged book-summary workflow.

Your job is to produce a faithful, substantial, publication-quality summary of a Markdown book by
using the local preparation scripts and completing the staged pipeline end to end.

Target book:
<BOOK_FILE>

Files to use:
- `prepare_terminal_summary_job.py`
- `prompts/book_summary_compromise.md`
- `summarize_book.py`

Goal:
Produce a strong final summary without wasting context on a naive one-shot full-book pass. Use the
staged workflow all the way through:
book map -> map review -> chunk summaries -> chapter summaries -> final synthesis -> verification
-> assembled Markdown output.

A good book summary is not jacket copy and not a pile of notes. It should be substantial enough
that a serious reader can understand:
- what kind of book this is
- the book's real thesis or central wager
- how the structure builds that case
- the major arguments, methods, and representative examples
- what is framing or endmatter versus what is core
- where fidelity is limited by OCR or source damage

Core rules:
- Stay grounded only in the source text or the prior stage outputs provided by the workflow.
- Do not add outside knowledge, biography, critique, admiration, hype, or interpretation not
  supported by the book text.
- Treat `BOOK_MAP.md`, `MAP_REVIEW.md`, packet profiles, and chapter profiles as weighting and
  proportion guidance only, not as evidence.
- If OCR damage makes something unclear, record it in `ambiguity_flags` or faithfulness notes
  instead of guessing.
- Keep output proportional: major arguments and central chapters deserve materially more space than
  front matter, framing, metadata, source notes, bibliography, reading lists, and other back
  matter.
- For core-argument sections, preserve claims, methods, principles, and representative examples.
- For case-study sections, preserve chronology, pivots, decisions, and concrete episodes.
- For framing, reference, and endmatter sections, compress aggressively and describe their role
  honestly.
- Whenever a prompt file asks for strict JSON, write valid JSON only.
- Save outputs to the exact job output paths expected by the workflow.
- Do not skip the verification stage.
- If you find a quality problem, fix the earliest relevant staged JSON output and rebuild
  downstream prompts instead of hand-waving over the issue.
- Prefer schema-compliant current outputs. Do not rely on legacy fields if the prompt asks for
  richer ones.

Process:
1. Initialize the job:
   `python3 prepare_terminal_summary_job.py init "<BOOK_FILE>" --overwrite`

2. Determine the job directory:
   - Read the command output or derive the slug.
   - Use:
     `<JOB_DIR>`

3. Read the generated files in `<JOB_DIR>/`:
   - `RUNBOOK.md`
   - `manifest.json`
   - `BOOK_MAP.md`
   - `MAP_REVIEW.md`
   - `book_map.json` if useful
   - `map_review.json` if useful

4. Check workflow status before starting:
   - `python3 prepare_terminal_summary_job.py status "<JOB_DIR>"`
   - `python3 prepare_terminal_summary_job.py run-next "<JOB_DIR>"`

5. Complete all chunk prompts:
   - For each file in `<JOB_DIR>/prompts/chunks/*.md`:
     - read the prompt file
     - use the matching template in `<JOB_DIR>/outputs/chunks/*.template.json` as a scaffold
     - produce a JSON response matching the schema exactly
     - save it to `<JOB_DIR>/outputs/chunks/<same-basename>.json`

6. Build chapter prompts:
   `python3 prepare_terminal_summary_job.py build-chapters "<JOB_DIR>"`

7. Audit and status-check the job:
   - `python3 prepare_terminal_summary_job.py audit-job "<JOB_DIR>"`
   - `python3 prepare_terminal_summary_job.py status "<JOB_DIR>"`
   - If audit flags chunk issues, fix the relevant chunk JSON outputs first, then rerun
     `build-chapters` and audit again.

8. Complete all chapter prompts:
   - For each file in `<JOB_DIR>/prompts/chapters/*.md`:
     - read the prompt file
     - use the matching template in `<JOB_DIR>/outputs/chapters/*.template.json`
     - produce a JSON response matching the schema exactly
     - save it to `<JOB_DIR>/outputs/chapters/<same-basename>.json`

9. Build the final synthesis prompt:
   `python3 prepare_terminal_summary_job.py build-final "<JOB_DIR>"`

10. Audit and status-check the job:
    - `python3 prepare_terminal_summary_job.py audit-job "<JOB_DIR>"`
    - `python3 prepare_terminal_summary_job.py status "<JOB_DIR>"`
    - If audit flags chapter issues, fix the relevant chapter JSON outputs first, then rerun
      `build-final` and audit again.

11. Complete the final prompt:
    - Read `<JOB_DIR>/prompts/final/final.md`
    - Use `<JOB_DIR>/outputs/final/final.template.json` as a scaffold
    - Produce valid JSON only
    - Save it to `<JOB_DIR>/outputs/final/final.json`

12. Build the verification prompt:
    `python3 prepare_terminal_summary_job.py build-verify "<JOB_DIR>"`

13. Audit and status-check the job:
    - `python3 prepare_terminal_summary_job.py audit-job "<JOB_DIR>"`
    - `python3 prepare_terminal_summary_job.py status "<JOB_DIR>"`
    - If audit or your own review suggests drift, thinness, overstatement, unsupported claims, or
      bad proportion, fix the relevant upstream stage output, rebuild the downstream prompt, and
      continue.

14. Complete the verification prompt:
    - Read `<JOB_DIR>/prompts/verify/verify.md`
    - Use `<JOB_DIR>/outputs/verify/verify.template.json` as a scaffold
    - Produce valid JSON only
    - Save it to `<JOB_DIR>/outputs/verify/verify.json`

15. Assemble the final deliverable:
    `python3 prepare_terminal_summary_job.py assemble "<JOB_DIR>" --final-path "<FINAL_SUMMARY_MD>" --clean`

16. If you want to preserve full provenance before cleanup:
    `python3 prepare_terminal_summary_job.py assemble "<JOB_DIR>" --final-path "<FINAL_SUMMARY_MD>" --keep-provenance-zip --clean`

17. If the job directory still exists, run a final audit before reporting back:
    `python3 prepare_terminal_summary_job.py audit-job "<JOB_DIR>"`

18. Return:
   - the job directory
   - the final summary Markdown path
   - the final summary JSON path
   - a short note on ambiguity or OCR limitations that affected fidelity
   - a short note on any audit warnings that remain

Quality bar:
- The final summary should read like a serious, polished, substantial book summary.
- It must preserve the book’s real thesis, structure, major arguments, methods, and
  representative examples.
- It must be more faithful than a shortcut summary and more efficient than reading the whole book
  in one giant pass.
- It must reflect the workflow’s weighting and detail guidance instead of giving equal space to
  everything.
- High-priority core chapters should be detailed enough that a serious reader can reconstruct what
  the book is really saying.
- Framing and endmatter should stay compressed and honest.
- It must survive the verification stage without unsupported claims, inflated emphasis, or
  avoidable thinness.

System goal

A file appears in a watched directory. Your existing watcher pattern detects it, starts a Temporal workflow, the pipeline ingests and standardizes vendor spend data, runs AI QA, collates analysis tables, generates savings recommendations, writes the memo automatically, and exposes the results through MCP for querying.

Architecture

Use your existing file-observer implementation for /incoming. Do not rebuild that piece.

The runtime is:

watcher -> Temporal workflow -> activities/services -> Postgres -> report artifacts + MCP query surface

Redis can stay in the stack if you already use it for caching, locks, or transient coordination, but Temporal is the workflow system. Temporal workers poll task queues, activities are the failure-prone units, and task queues can be split to separate lighter data work from heavier AI work.

MVP phases

Phase 1: Vendor ingestion
This is the first shipping target.

Scope:

detect file

create analysis_run

parse CSV/XLSX

normalize headers

map core fields

load raw rows into Postgres

record file metadata and row counts

Output:

raw rows stored

inferred column mapping

run status and audit trail

Tests:

parses CSV and XLSX

handles missing optional columns

rejects missing required spend fields

idempotent reprocessing by checksum/run key

Phase 2: Analysis
This is the second shipping target.

Scope:

canonicalize vendor names

spend by vendor

spend by category

top vendor concentration

long-tail spend

fragmented categories

duplicate/alias vendor candidates

Output tables:

vendor_spend_summary

category_spend_summary

tail_spend_summary

fragmented_categories

vendor_alias_candidates

Tests:

deterministic outputs for fixture datasets

long-tail threshold logic

duplicate vendor merge logic

category aggregation correctness

Phase 3: Communication generation
This is the third shipping target.

Scope:

AI QA on cleaned raw data

AI QA on collated outputs

recommendation generation in four buckets:

renegotiate

consolidate

eliminate

replace with automation

automatic CEO/CFO memo generation

long-tail action generation:

keep

justify

consolidate

eliminate

Output artifacts:

savings_opportunities

memo_outputs

long_tail_actions

Tests:

prompt contract tests

memo schema validation

no empty memo on valid runs

opportunity rows generated for standard fixtures

Phase 4: Queue/orchestration hardening
Do this after the first three work.

Scope:

Temporal workflow

activity retries and timeouts

AI task queue separation

run-event logging

failure handling and resume behavior

Temporal strongly recommends setting activity timeouts, especially Start-To-Close, because that is how worker crashes and stalled execution get detected for retries.

Tests:

integration tests with Temporal test server

activity failure retry tests

workflow resumes/fails cleanly

duplicate file detection

Pipeline

Use one workflow per run.

Workflow steps:

register_source_file

ingest_file

infer_and_apply_column_mapping

clean_and_standardize

ai_qa_raw

collate_spend_views

ai_qa_collated

analyze_opportunities

generate_long_tail_actions

generate_memo

build_mcp_context

finalize_run

Task queues

Keep it simple:

vendor-spend-core

vendor-spend-ai

Temporal workers can poll separate queues, which is useful for isolating heavier AI activities from core data processing.

Database tables

Core:

analysis_runs

source_files

raw_spend_rows

column_mappings

Analysis:

normalized_vendors

vendor_spend_summary

category_spend_summary

tail_spend_summary

fragmented_categories

vendor_alias_candidates

Communication:

qa_findings

savings_opportunities

long_tail_actions

memo_outputs

MCP/query:

mcp_contexts

run_events

No-UI MCP layer

Add an MCP server as a thin query surface over the run outputs.

Keep it narrow. Do not expose raw arbitrary SQL. Expose resources and tools around the finished analysis.

Suggested MCP resources:

run://latest/summary

run://{run_id}/summary

run://{run_id}/memo

run://{run_id}/opportunities

run://{run_id}/long-tail

run://{run_id}/qa-findings

Suggested MCP tools:

get_run_summary(run_id)

list_top_vendors(run_id, limit=10)

list_fragmented_categories(run_id)

get_long_tail_actions(run_id, action=None)

get_memo(run_id)

ask_run(run_id, question)

For MCP, keep transport minimal and local first. The MCP ecosystem is actively evolving its HTTP transport and authorization model, so for speed and stability this should be a thin internal server over your materialized outputs, not a complex remote platform component.

Testing strategy

This should be built test-first around fixtures.

Test layers:

unit tests for cleaning, mapping, normalization, thresholds

integration tests for Postgres writes and analysis outputs

Temporal integration tests for workflow execution

prompt/output contract tests for AI stages

Temporal’s Python docs recommend most tests be integration tests, using the test server and time-skipping where useful.

Repo shape

vendor-spend-strategist/
  watcher/
    # use existing internal watcher pattern here
    observe_incoming.py

  workflows/
    spend_analysis_workflow.py

  activities/
    register_source_file.py
    ingest_file.py
    infer_and_apply_column_mapping.py
    clean_and_standardize.py
    ai_qa_raw.py
    collate_spend_views.py
    ai_qa_collated.py
    analyze_opportunities.py
    generate_long_tail_actions.py
    generate_memo.py
    build_mcp_context.py
    finalize_run.py

  services/
    postgres.py
    file_loader.py
    vendor_normalizer.py
    analysis_engine.py
    memo_engine.py
    llm_client.py

  mcp/
    server.py
    resources.py
    tools.py

  db/
    schema.sql
    views.sql

  tests/
    fixtures/
    unit/
    integration/
    temporal/

  prompts/
    qa_raw.txt
    qa_collated.txt
    opportunities.txt
    memo.txt
    long_tail.txt

  sample_data/
  README.md

Build order

Weeknight-fast order:

file ingestion only

analysis tables only

memo generation only

Temporal orchestration

MCP query layer

AI QA polish

That gets you to MVP fastest.

What to reuse from internal projects

Explicitly in the spec:

folder observer should reuse your current internal file-watching pattern

any existing Postgres connection/session pattern should be reused

any existing artifact/report writing pattern should be reused

any stable LLM wrapper should be reused

That is the correct way to move quickly: reuse proven plumbing, build only the domain-specific layers.

Positioning line

“I built an automated vendor-spend analysis pipeline that watches for new spend files, runs deterministic analysis plus AI QA, generates executive recommendations and a CEO/CFO memo, and exposes the results through MCP for direct querying.”
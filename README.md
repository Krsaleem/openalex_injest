# OpenAlex S3 → Postgres (relational)

Mirrors OpenAlex's official public snapshot (published specifically for
bulk download — not scraping the live API) into Postgres, as a **proper
relational schema** — real typed columns, real foreign keys, not a JSON
blob cache. Targets the same Supabase database as the `scholarmatch` app,
into 14 new tables (8 entities + 6 join tables) — completely independent
of that app's own member data (User/Profile/Employment/Education/Work are
untouched, no FKs either way).

## Tables

- `openalex_topics`, `openalex_institutions`, `openalex_publishers`,
  `openalex_funders` — no dependencies
- `openalex_sources` — FK to `openalex_publishers` (`host_organization`,
  only when it's a real publisher id, not an institution-hosted source)
- `openalex_authors` — FK to `openalex_institutions` (last known institution)
- `openalex_awards` — FK to `openalex_funders`
- `openalex_works` — FK to `openalex_sources` (primary publication venue)
- `openalex_author_affiliations` — author ↔ institution, with the years
  affiliated (real `integer[]` column)
- `openalex_author_topics` — author ↔ topic, with OpenAlex's per-topic
  work count for that author
- `openalex_work_authorships` — work ↔ author, one row per authorship
  (institution = affiliation *at the time of that publication*, can
  differ from the author's current last-known institution)
- `openalex_work_topics` — work ↔ topic, with OpenAlex's relevance score
- `openalex_work_awards` — work ↔ award, from that work's own `awards[]`
  field (NOT from the award's own `funded_outputs[]` — verified live that
  array is capped at 100 while the real count can be in the hundreds of
  thousands, so it's an unreliable join source)
- `openalex_work_funders` — work ↔ funder, from that work's own
  `funders[]` field — kept separate from `openalex_work_awards` since a
  work's funders list is often populated even with no specific award
  number attached

Full column list: `schema.sql` (reference only — the tables are actually
created by `scholarmatch`'s own Prisma migration; kept here in sync so
the shape `load.mjs` targets is documented in one place).

## Real scale — checked live against the actual manifest before building this

| Entity | Records | Compressed size |
|---|---|---|
| topics | 4,516 | ~0 GB |
| sources | 283,287 | 0.4 GB |
| institutions | 127,784 | 0.2 GB |
| publishers | 10,703 | ~0 GB |
| funders | 45,640 | ~0 GB |
| authors | 119,129,660 | 74.3 GB |
| awards | 18,991,361 | 5.0 GB |
| works | 510,372,821 | 665.7 GB |

**Test with `topics` or `institutions` first** — both finish in seconds and
prove the whole pipeline (S3 → gunzip → parse → extract → COPY → guarded
insert) works before you commit to `authors` (74GB, real hours) or `works`
(665GB compressed — plan for a lot of free disk and a long run).

## Load order matters

```
funders → publishers → topics → sources → institutions → authors → awards → works
```

`funders`/`publishers` go first since `sources` and `awards` each guard a
link back to one of them (a link only gets set if the row it points to
already exists — see below); `works` goes last since it guards links to
authors, awards, AND funders all at once.

Join-table rows, and the `sources`→publisher / `awards`→funder links, only
insert/set once their parent(s) already exist — an `EXISTS`-guarded
`INSERT ... SELECT`, not a raw insert that would trip the real FK
constraint. So loading `works` before `authors`/`awards` finish doesn't
error, it just silently skips rows whose parent isn't there yet.
**Re-run the same `--entity works` command again** after `authors`/
`awards`/`funders` finish to backfill those (every insert is
`ON CONFLICT DO NOTHING`, so re-running is always safe, never duplicates).

## Setup

```bash
npm install
```

**Connection string must be the Session pooler, not the Transaction
pooler** (the one `scholarmatch`'s own `DATABASE_URL` uses, port 6543,
`pgbouncer=true`). Transaction-mode pooling recycles the connection
between statements, which breaks the `CREATE TEMP TABLE` this script
relies on staying alive across many `COPY`/`INSERT`/`TRUNCATE` calls —
get the Session pooler string from Supabase's dashboard instead (same
place, different mode/port).

```bash
export PGHOST=aws-0-us-east-1.pooler.supabase.com
export PGPORT=5432
export PGDATABASE=postgres
export PGUSER=postgres.kqauxfvstsmhvknbulos
export PGPASSWORD=your-real-password-here   # NOT the literal text "your-real-password-here" -- your actual password
```

Password is the same one already in `scholarmatch/.env.local`'s
`DATABASE_URL` line (between the `:` after the username and the `@`
before the host) — same database role, this is just a different pooler
port/mode to reach it.

## Run

```bash
node load.mjs --entity topics        # test this first
node load.mjs --entity institutions
node load.mjs --entity funders
node load.mjs --entity publishers
node load.mjs --entity sources
node load.mjs --entity authors
node load.mjs --entity awards
node load.mjs --entity works
node load.mjs --entity works          # re-run to backfill any authorships/
                                       # awards/funders skipped while
                                       # authors/awards was mid-load
```

## Starting over

To wipe all 14 tables and reload from scratch:

```bash
node load.mjs --truncate-all --confirm
```

Requires `--confirm` or it just prints what it would do and exits — this
is destructive (deletes every row in all 14 `openalex_*` tables). Does
**not** touch anything else — the existing `openalex` table (real member
enrichment), `User`/`Profile`/`Employment`/`Education`/`Work`, are all a
completely separate part of the schema and are never referenced by this
command.

## Notes

- **IDs are stored bare** (`"A5028125522"`), not the full OpenAlex URI
  (`"https://openalex.org/A5028125522"`) — this applies to every id column
  AND every foreign-key column that references one (institution_id,
  author_id, topic_id, work_id, source_id, publisher_id, funder_id,
  award_id). `stripOpenAlexId()` in `load.mjs` does this on every
  `extract*()` function; verified against a real author record before
  shipping.
- **`sources.publisher_id`** is only set for sources actually hosted by a
  publisher (`host_organization` starts with `P`, e.g. Elsevier) — a
  source hosted by an institution instead (`host_organization` starts
  with `I`, e.g. a university repository) always has this null, by
  design, verified live on real records of both kinds.
- **`orcid_id` is null for most authors, by design, not a bug** — the
  bulk `authors` entity is OpenAlex's *entire* author universe (119.1M),
  not filtered to only ones with an ORCID iD. Only ~7.7% of all OpenAlex
  authors have one at all (9.2M `has_orcid:true` / 119.1M total, per an
  earlier live check against the API). Verify after loading with:
  `SELECT count(*) filter (where orcid_id is not null) * 100.0 / count(*) FROM openalex_authors;`
  — should land close to that ~7.7%.
- **`created_at`/`updated_at`** exist on all 9 tables, defaulting to `now()`
  at insert time. Since every insert is `ON CONFLICT DO NOTHING` (rows are
  never updated in place, only inserted once), `updated_at` will always
  equal `created_at` under the current loader — that's expected, not a
  bug, unless the insert logic changes to actually update existing rows.
- Real S3 layout is `s3://openalex/data/jsonl/<entity>/updated_date=.../part_*.gz`
  plus an authoritative `data/jsonl/manifest.json` listing every file and
  its record count — this is what `load.mjs` reads instead of paginating
  `ListObjectsV2` itself.
- 4 concurrent files at a time (`p-limit`), each gunzipped/parsed via
  Node streams so no single file is ever fully buffered in memory.
- Postgres's `COPY` doesn't support `ON CONFLICT` (that's an `INSERT`-only
  feature) — every batch goes into per-connection `TEMP TABLE`s (real
  typed columns, matching the destination tables) via `COPY`, then one or
  more `INSERT ... SELECT ... ON CONFLICT DO NOTHING` moves rows into the
  real tables.
- Nested arrays (`affiliations[]`, `topics[]`, `authorships[]`) are
  extracted into their own join-table rows during load, not stored as
  JSON — that's the whole point of the relational rewrite.
- Anonymous S3 access needs both a no-op signer AND explicit dummy
  credentials (verified live — a no-op signer alone still throws
  `CredentialsProviderError`, since the SDK tries to resolve real
  credentials before it ever gets to signing).
- Verified the extraction logic (`extractAuthor`, etc.) against a real
  OpenAlex author record before shipping — see the assertions this
  produced were correct for orcid parsing, the Postgres array literal
  format for `years`, and topic/affiliation counts.

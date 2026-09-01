#!/usr/bin/env node
// OpenAlex S3 snapshot -> proper relational Postgres tables (real typed
// columns + real foreign keys, not a JSON blob cache -- see
// prisma/schema.prisma in the scholarmatch project for the matching
// model definitions).
//
// Real S3 layout, verified live against the actual bucket (the commonly-
// assumed "s3://openalex/data/<entity>/..." layout is WRONG -- it's
// "s3://openalex/data/jsonl/<entity>/updated_date=.../part_*.gz", with an
// authoritative manifest.json listing every file + its record count):
//   s3://openalex/data/jsonl/manifest.json
//   s3://openalex/data/jsonl/<entity>/updated_date=YYYY-MM-DD/part_NNNN.gz
//
// Real scale, per that manifest: authors = 119.1M records / 74.3GB
// compressed, works = 510.4M records / 665.7GB compressed, awards =
// 19.0M records / 5.0GB compressed. Test with topics or institutions
// first (both tiny) before running authors/works/awards.
//
// Referential integrity: join-table rows (author<->institution,
// author<->topic, work<->author, work<->topic, work<->award,
// work<->funder) and cross-entity FKs (source->publisher, award->funder)
// are only inserted/set when their parent row(s) already exist
// (EXISTS-guarded INSERT ... SELECT), so real FK constraints on those
// tables should never actually reject a row -- rows/links whose parent
// doesn't exist yet (e.g. works loaded before their author, or a
// merged/deprecated OpenAlex id) are silently skipped rather than
// crashing the batch. Load order matters:
// funders -> publishers -> topics -> sources -> institutions -> authors
// -> awards -> works. (funders/publishers first since sources and
// awards each guard a link back to one of them; works last since it
// guards links to authors, awards, AND funders all at once.)
//
// Postgres's COPY protocol does NOT support ON CONFLICT (that's an
// INSERT feature) -- every batch goes into per-connection TEMP TABLEs via
// COPY first, then one or more `INSERT ... SELECT ... ON CONFLICT DO
// NOTHING` moves rows into the real tables. TEMP TABLEs are automatically
// isolated per connection, which is what makes 4 concurrent COPY streams
// (each needs an exclusive connection for the stream's duration) safe to
// run against the same entity at once.
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import zlib from "zlib";
import readline from "readline";
import pLimit from "p-limit";

const BUCKET = "openalex";
const REGION = "us-east-1";
const MANIFEST_KEY = "data/jsonl/manifest.json";
const CONCURRENCY = 4;
const BATCH_SIZE = 1000;

const DB_CONFIG = {
  host: process.env.PGHOST || "localhost",
  port: Number(process.env.PGPORT) || 5432,
  database: process.env.PGDATABASE || "openalex",
  user: process.env.PGUSER || "postgres",
  password: process.env.PGPASSWORD || "yourpassword",
};

const ENTITY_ORDER = ["funders", "publishers", "topics", "sources", "institutions", "authors", "awards", "works"];

// All 14 real tables, for --truncate-all. Order doesn't matter within one
// TRUNCATE statement (Postgres truncates every listed table together,
// atomically) -- CASCADE is just a safety net in case a table here is
// ever referenced by something not in this list.
const ALL_TABLES = [
  "openalex_author_affiliations",
  "openalex_author_topics",
  "openalex_work_authorships",
  "openalex_work_topics",
  "openalex_work_awards",
  "openalex_work_funders",
  "openalex_authors",
  "openalex_works",
  "openalex_awards",
  "openalex_topics",
  "openalex_sources",
  "openalex_institutions",
  "openalex_publishers",
  "openalex_funders",
];

// AWS SDK v3 has no simple "--no-sign-request" flag like the CLI. Verified
// live: a no-op signer alone is NOT enough -- the client still tries to
// resolve real credentials first and throws CredentialsProviderError
// before it ever gets to signing. Explicit dummy credentials stop that
// lookup; the no-op signer stops it from actually SigV4-signing with them
// (which would produce an invalid signature otherwise).
const s3 = new S3Client({
  region: REGION,
  credentials: { accessKeyId: "anonymous", secretAccessKey: "anonymous" },
  signer: { sign: async (request) => request },
});

function parseArgs() {
  const args = process.argv.slice(2);

  if (args.includes("--truncate-all")) {
    if (!args.includes("--confirm")) {
      console.error(
        "--truncate-all deletes every row in all 9 openalex_* tables (existing member data in `openalex`/User/Profile/etc. is untouched -- only these 9). Re-run with --confirm to actually do it:\n" +
          "  node load.mjs --truncate-all --confirm"
      );
      process.exit(1);
    }
    return { truncateAll: true };
  }

  const idx = args.indexOf("--entity");
  const entity = idx !== -1 ? args[idx + 1] : null;
  if (!entity || !ENTITY_ORDER.includes(entity)) {
    console.error(`Usage: node load.mjs --entity <${ENTITY_ORDER.join("|")}>`);
    console.error(`   or: node load.mjs --truncate-all --confirm`);
    process.exit(1);
  }
  return { entity };
}

async function truncateAll() {
  await withShortClient(async (client) => {
    console.log(`Truncating: ${ALL_TABLES.join(", ")}`);
    await client.query(`TRUNCATE TABLE ${ALL_TABLES.join(", ")} CASCADE`);
  });
  console.log("Done -- all 9 tables are now empty.");
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf-8");
}

async function getFileList(entity) {
  const { Body } = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: MANIFEST_KEY }));
  const manifest = JSON.parse(await streamToString(Body));
  const found = manifest.entities.find((e) => e.entity === entity);
  if (!found) throw new Error(`No manifest entry for entity "${entity}"`);
  return found.files; // [{ url: "s3://openalex/...", meta: { record_count, content_length } }]
}

// ---- CSV helpers for COPY ----

function csvField(value) {
  if (value === null || value === undefined) return ""; // empty, unquoted -> COPY reads it as NULL
  return `"${String(value).replace(/"/g, '""')}"`;
}

function pgIntArray(arr) {
  if (!Array.isArray(arr) || arr.length === 0) return "{}";
  return `{${arr.join(",")}}`;
}

async function copyRows(client, table, columns, rows) {
  if (rows.length === 0) return;
  const copyStream = client.query(copyFrom(`COPY ${table} (${columns.join(",")}) FROM STDIN WITH (FORMAT csv)`));
  const csv = rows.map((row) => row.map(csvField).join(",") + "\n").join("");
  await new Promise((resolve, reject) => {
    copyStream.on("error", reject);
    copyStream.on("finish", resolve);
    copyStream.end(csv);
  });
}

// ---- Per-entity extraction: raw OpenAlex JSON -> real columns ----

// OpenAlex ids in the raw JSON are full URIs ("https://openalex.org/A5028125522")
// -- stored bare ("A5028125522") in every id/FK column, on both the main
// row's own id AND every reference to another entity's id (institution
// id, author id, topic id, source id, work id), so a plain string
// comparison works for joins/lookups instead of needing the full URI.
// Idempotent on an already-bare id (no "/" -> returned unchanged).
export function stripOpenAlexId(url) {
  if (!url) return null;
  const parts = String(url).split("/");
  return parts[parts.length - 1];
}

export function extractTopic(r) {
  return [stripOpenAlexId(r.id), r.display_name ?? null, r.field?.display_name ?? null, r.subfield?.display_name ?? null, r.domain?.display_name ?? null, r.works_count ?? null, r.cited_by_count ?? null];
}

// host_organization is a full OpenAlex URI for either a publisher ("P...")
// or an institution ("I..." -- self-hosted/repository sources); only the
// publisher case is captured (openalex_publishers has no institution
// counterpart table here), verified live that both prefixes really occur.
export function isPublisherId(strippedId) {
  return typeof strippedId === "string" && strippedId.startsWith("P");
}

export function extractSource(r) {
  const hostId = stripOpenAlexId(r.host_organization);
  return [
    stripOpenAlexId(r.id),
    r.display_name ?? null,
    r.type ?? null,
    r.issn_l ?? null,
    r.country_code ?? null,
    r.works_count ?? null,
    r.cited_by_count ?? null,
    isPublisherId(hostId) ? hostId : null,
  ];
}

export function extractPublisher(r) {
  return [
    stripOpenAlexId(r.id),
    r.display_name ?? null,
    r.country_codes?.[0] ?? null,
    r.works_count ?? null,
    r.cited_by_count ?? null,
  ];
}

export function extractFunder(r) {
  return [
    stripOpenAlexId(r.id),
    r.display_name ?? null,
    r.country_code ?? null,
    r.description ?? null,
    r.homepage_url ?? null,
    r.awards_count ?? null,
    r.works_count ?? null,
    r.cited_by_count ?? null,
    r.summary_stats?.h_index ?? null,
    r.summary_stats?.i10_index ?? null,
  ];
}

// funded_outputs[]/funded_outputs_count deliberately NOT used -- verified
// live the inline array is capped at 100 while the real count can be in
// the hundreds of thousands for a large grant, so it's an unreliable
// join source. The real work<->award link is built from each WORK's own
// `awards[]` field instead (see extractWork below).
export function extractAward(r) {
  return [
    stripOpenAlexId(r.id),
    r.display_name ?? null,
    r.description ?? null,
    r.funder_award_id ?? null,
    stripOpenAlexId(r.funder?.id),
    r.amount ?? null,
    r.currency ?? null,
    r.funding_type ?? null,
    r.start_year ?? null,
    r.end_year ?? null,
    r.doi ?? null,
  ];
}

export function extractInstitution(r) {
  return [stripOpenAlexId(r.id), r.display_name ?? null, r.type ?? null, r.country_code ?? null, r.ror ?? null, r.works_count ?? null, r.cited_by_count ?? null];
}

export function extractOrcid(orcidUrl) {
  return orcidUrl ? orcidUrl.replace("https://orcid.org/", "") : null;
}

export function extractAuthor(r) {
  const id = stripOpenAlexId(r.id);
  const main = [
    id,
    extractOrcid(r.orcid),
    r.display_name ?? null,
    r.works_count ?? null,
    r.cited_by_count ?? null,
    r.summary_stats?.h_index ?? null,
    r.summary_stats?.i10_index ?? null,
    stripOpenAlexId(r.last_known_institutions?.[0]?.id),
  ];
  const affiliations = (r.affiliations ?? [])
    .filter((a) => a.institution?.id)
    .map((a) => [id, stripOpenAlexId(a.institution.id), pgIntArray(a.years)]);
  const topics = (r.topics ?? []).filter((t) => t.id).map((t) => [id, stripOpenAlexId(t.id), t.count ?? null]);
  return { main, affiliations, topics };
}

export function extractWork(r) {
  const id = stripOpenAlexId(r.id);
  const main = [
    id,
    r.title ?? r.display_name ?? null,
    r.publication_year ?? null,
    r.publication_date ?? null,
    r.type ?? null,
    r.doi ?? null,
    r.cited_by_count ?? null,
    stripOpenAlexId(r.primary_location?.source?.id),
  ];
  const authorships = (r.authorships ?? [])
    .filter((a) => a.author?.id)
    .map((a) => [id, stripOpenAlexId(a.author.id), stripOpenAlexId(a.institutions?.[0]?.id), a.author_position ?? null]);
  const topics = (r.topics ?? []).filter((t) => t.id).map((t) => [id, stripOpenAlexId(t.id), t.score ?? null]);
  // Real fields, verified live on both the API and the raw S3 record --
  // this is the reliable join source for work<->award/work<->funder (see
  // extractAward's comment on why the award's own funded_outputs isn't).
  const awards = (r.awards ?? []).filter((a) => a.id).map((a) => [id, stripOpenAlexId(a.id), a.funder_award_id ?? null]);
  const funders = (r.funders ?? []).filter((f) => f.id).map((f) => [id, stripOpenAlexId(f.id)]);
  return { main, authorships, topics, awards, funders };
}

// ---- Per-entity flush: staging tables + EXISTS-guarded inserts ----

// [column, type] pairs -- staging tables use these REAL types (not text)
// so COPY parses each field with normal type input rules and the final
// INSERT...SELECT needs no casting, same types on both sides.
// sources and awards are NOT here even though they're otherwise simple --
// each guards one FK (source->publisher, award->funder) against a row
// that might not exist yet, so they get their own bespoke flush function
// below (flushSources/flushAwards), same treatment as authors/works.
const SIMPLE_ENTITY = {
  topics: {
    table: "openalex_topics",
    columns: [["id", "text"], ["display_name", "text"], ["field_name", "text"], ["subfield_name", "text"], ["domain_name", "text"], ["works_count", "integer"], ["cited_by_count", "integer"]],
    extract: extractTopic,
  },
  institutions: {
    table: "openalex_institutions",
    columns: [["id", "text"], ["display_name", "text"], ["type", "text"], ["country_code", "text"], ["ror_id", "text"], ["works_count", "integer"], ["cited_by_count", "integer"]],
    extract: extractInstitution,
  },
  publishers: {
    table: "openalex_publishers",
    columns: [["id", "text"], ["display_name", "text"], ["country_code", "text"], ["works_count", "integer"], ["cited_by_count", "integer"]],
    extract: extractPublisher,
  },
  funders: {
    table: "openalex_funders",
    columns: [["id", "text"], ["display_name", "text"], ["country_code", "text"], ["description", "text"], ["homepage_url", "text"], ["awards_count", "integer"], ["works_count", "integer"], ["cited_by_count", "integer"], ["h_index", "integer"], ["i10_index", "integer"]],
    extract: extractFunder,
  },
};

async function ensureStagingTables(client, entity) {
  if (entity in SIMPLE_ENTITY) {
    const cfg = SIMPLE_ENTITY[entity];
    await client.query(`CREATE TEMP TABLE stage_${entity} (${cfg.columns.map(([n, t]) => `${n} ${t}`).join(",")}) ON COMMIT PRESERVE ROWS`);
    return;
  }
  if (entity === "sources") {
    await client.query(`CREATE TEMP TABLE stage_sources (id text, display_name text, type text, issn_l text, country_code text, works_count integer, cited_by_count integer, publisher_id text) ON COMMIT PRESERVE ROWS`);
    return;
  }
  if (entity === "authors") {
    await client.query(`CREATE TEMP TABLE stage_authors_main (id text, orcid_id text, display_name text, works_count integer, cited_by_count integer, h_index integer, i10_index integer, last_known_institution_id text) ON COMMIT PRESERVE ROWS`);
    await client.query(`CREATE TEMP TABLE stage_authors_affiliations (author_id text, institution_id text, years integer[]) ON COMMIT PRESERVE ROWS`);
    await client.query(`CREATE TEMP TABLE stage_authors_topics (author_id text, topic_id text, count integer) ON COMMIT PRESERVE ROWS`);
    return;
  }
  if (entity === "awards") {
    await client.query(`CREATE TEMP TABLE stage_awards (id text, display_name text, description text, funder_award_id text, funder_id text, amount double precision, currency text, funding_type text, start_year integer, end_year integer, doi text) ON COMMIT PRESERVE ROWS`);
    return;
  }
  if (entity === "works") {
    await client.query(`CREATE TEMP TABLE stage_works_main (id text, title text, publication_year integer, publication_date date, type text, doi text, cited_by_count integer, primary_source_id text) ON COMMIT PRESERVE ROWS`);
    await client.query(`CREATE TEMP TABLE stage_works_authorships (work_id text, author_id text, institution_id text, author_position text) ON COMMIT PRESERVE ROWS`);
    await client.query(`CREATE TEMP TABLE stage_works_topics (work_id text, topic_id text, score double precision) ON COMMIT PRESERVE ROWS`);
    await client.query(`CREATE TEMP TABLE stage_works_awards (work_id text, award_id text, funder_award_id text) ON COMMIT PRESERVE ROWS`);
    await client.query(`CREATE TEMP TABLE stage_works_funders (work_id text, funder_id text) ON COMMIT PRESERVE ROWS`);
  }
}

async function flushSimple(client, entity, records) {
  const cfg = SIMPLE_ENTITY[entity];
  const stage = `stage_${entity}`;
  const columnNames = cfg.columns.map(([n]) => n);
  await copyRows(client, stage, columnNames, records.map(cfg.extract));
  await client.query(`INSERT INTO ${cfg.table} (${columnNames.join(",")}) SELECT ${columnNames.join(",")} FROM ${stage} ON CONFLICT (id) DO NOTHING`);
  await client.query(`TRUNCATE ${stage}`);
}

async function flushSources(client, records) {
  const rows = records.map(extractSource);
  await copyRows(client, "stage_sources", ["id", "display_name", "type", "issn_l", "country_code", "works_count", "cited_by_count", "publisher_id"], rows);
  // publisher_id only kept if that publisher already exists -- publishers
  // load before sources in the real load order, so this only ever stays
  // null for a genuinely non-publisher host (an institution) or a
  // merged/deprecated publisher id, never a load-order race.
  await client.query(`
    INSERT INTO openalex_sources (id, display_name, type, issn_l, country_code, works_count, cited_by_count, publisher_id)
    SELECT s.id, s.display_name, s.type, s.issn_l, s.country_code, s.works_count::int, s.cited_by_count::int,
           CASE WHEN EXISTS (SELECT 1 FROM openalex_publishers p WHERE p.id = s.publisher_id) THEN s.publisher_id ELSE NULL END
    FROM stage_sources s
    ON CONFLICT (id) DO NOTHING
  `);
  await client.query("TRUNCATE stage_sources");
}

async function flushAwards(client, records) {
  const rows = records.map(extractAward);
  await copyRows(client, "stage_awards", ["id", "display_name", "description", "funder_award_id", "funder_id", "amount", "currency", "funding_type", "start_year", "end_year", "doi"], rows);
  // funder_id only kept if that funder already exists -- funders load
  // before awards in the real load order (see ENTITY_ORDER).
  await client.query(`
    INSERT INTO openalex_awards (id, display_name, description, funder_award_id, funder_id, amount, currency, funding_type, start_year, end_year, doi)
    SELECT s.id, s.display_name, s.description, s.funder_award_id,
           CASE WHEN EXISTS (SELECT 1 FROM openalex_funders f WHERE f.id = s.funder_id) THEN s.funder_id ELSE NULL END,
           s.amount::float8, s.currency, s.funding_type, s.start_year::int, s.end_year::int, s.doi
    FROM stage_awards s
    ON CONFLICT (id) DO NOTHING
  `);
  await client.query("TRUNCATE stage_awards");
}

async function flushAuthors(client, records) {
  const extracted = records.map(extractAuthor);
  await copyRows(client, "stage_authors_main", ["id", "orcid_id", "display_name", "works_count", "cited_by_count", "h_index", "i10_index", "last_known_institution_id"], extracted.map((e) => e.main));
  await copyRows(client, "stage_authors_affiliations", ["author_id", "institution_id", "years"], extracted.flatMap((e) => e.affiliations));
  await copyRows(client, "stage_authors_topics", ["author_id", "topic_id", "count"], extracted.flatMap((e) => e.topics));

  // Main row -- last_known_institution_id only kept if it already exists
  // (institutions loads before authors, but guard anyway rather than trip
  // the FK on a merged/deprecated id).
  await client.query(`
    INSERT INTO openalex_authors (id, orcid_id, display_name, works_count, cited_by_count, h_index, i10_index, last_known_institution_id)
    SELECT s.id, s.orcid_id, s.display_name, s.works_count::int, s.cited_by_count::int, s.h_index::int, s.i10_index::int,
           CASE WHEN EXISTS (SELECT 1 FROM openalex_institutions i WHERE i.id = s.last_known_institution_id) THEN s.last_known_institution_id ELSE NULL END
    FROM stage_authors_main s
    ON CONFLICT (id) DO NOTHING
  `);
  await client.query(`
    INSERT INTO openalex_author_affiliations (author_id, institution_id, years)
    SELECT s.author_id, s.institution_id, s.years::int[]
    FROM stage_authors_affiliations s
    WHERE EXISTS (SELECT 1 FROM openalex_authors a WHERE a.id = s.author_id)
      AND EXISTS (SELECT 1 FROM openalex_institutions i WHERE i.id = s.institution_id)
    ON CONFLICT (author_id, institution_id) DO NOTHING
  `);
  await client.query(`
    INSERT INTO openalex_author_topics (author_id, topic_id, count)
    SELECT s.author_id, s.topic_id, s.count::int
    FROM stage_authors_topics s
    WHERE EXISTS (SELECT 1 FROM openalex_authors a WHERE a.id = s.author_id)
      AND EXISTS (SELECT 1 FROM openalex_topics t WHERE t.id = s.topic_id)
    ON CONFLICT (author_id, topic_id) DO NOTHING
  `);

  await client.query("TRUNCATE stage_authors_main, stage_authors_affiliations, stage_authors_topics");
}

async function flushWorks(client, records) {
  const extracted = records.map(extractWork);
  await copyRows(client, "stage_works_main", ["id", "title", "publication_year", "publication_date", "type", "doi", "cited_by_count", "primary_source_id"], extracted.map((e) => e.main));
  await copyRows(client, "stage_works_authorships", ["work_id", "author_id", "institution_id", "author_position"], extracted.flatMap((e) => e.authorships));
  await copyRows(client, "stage_works_topics", ["work_id", "topic_id", "score"], extracted.flatMap((e) => e.topics));
  await copyRows(client, "stage_works_awards", ["work_id", "award_id", "funder_award_id"], extracted.flatMap((e) => e.awards));
  await copyRows(client, "stage_works_funders", ["work_id", "funder_id"], extracted.flatMap((e) => e.funders));

  await client.query(`
    INSERT INTO openalex_works (id, title, publication_year, publication_date, type, doi, cited_by_count, primary_source_id)
    SELECT s.id, s.title, s.publication_year::int, s.publication_date::date, s.type, s.doi, s.cited_by_count::int,
           CASE WHEN EXISTS (SELECT 1 FROM openalex_sources src WHERE src.id = s.primary_source_id) THEN s.primary_source_id ELSE NULL END
    FROM stage_works_main s
    ON CONFLICT (id) DO NOTHING
  `);
  // authorships reference authors -- if the referenced author isn't
  // loaded yet (e.g. you ran `works` before `authors` finished), this
  // authorship is silently skipped rather than failing the batch. Re-run
  // `works` again after `authors` finishes to pick those up (idempotent).
  await client.query(`
    INSERT INTO openalex_work_authorships (work_id, author_id, institution_id, author_position)
    SELECT s.work_id, s.author_id,
           CASE WHEN EXISTS (SELECT 1 FROM openalex_institutions i WHERE i.id = s.institution_id) THEN s.institution_id ELSE NULL END,
           s.author_position
    FROM stage_works_authorships s
    WHERE EXISTS (SELECT 1 FROM openalex_works w WHERE w.id = s.work_id)
      AND EXISTS (SELECT 1 FROM openalex_authors a WHERE a.id = s.author_id)
    ON CONFLICT (work_id, author_id) DO NOTHING
  `);
  await client.query(`
    INSERT INTO openalex_work_topics (work_id, topic_id, score)
    SELECT s.work_id, s.topic_id, s.score::float8
    FROM stage_works_topics s
    WHERE EXISTS (SELECT 1 FROM openalex_works w WHERE w.id = s.work_id)
      AND EXISTS (SELECT 1 FROM openalex_topics t WHERE t.id = s.topic_id)
    ON CONFLICT (work_id, topic_id) DO NOTHING
  `);
  // awards/funders reference the openalex_awards/openalex_funders tables --
  // if not loaded yet (e.g. you ran `works` before `awards`/`funders`
  // finished), these are silently skipped, same as authorships above.
  // Re-run `works` again after awards+funders finish to backfill them.
  await client.query(`
    INSERT INTO openalex_work_awards (work_id, award_id, funder_award_id)
    SELECT s.work_id, s.award_id, s.funder_award_id
    FROM stage_works_awards s
    WHERE EXISTS (SELECT 1 FROM openalex_works w WHERE w.id = s.work_id)
      AND EXISTS (SELECT 1 FROM openalex_awards a WHERE a.id = s.award_id)
    ON CONFLICT (work_id, award_id) DO NOTHING
  `);
  await client.query(`
    INSERT INTO openalex_work_funders (work_id, funder_id)
    SELECT s.work_id, s.funder_id
    FROM stage_works_funders s
    WHERE EXISTS (SELECT 1 FROM openalex_works w WHERE w.id = s.work_id)
      AND EXISTS (SELECT 1 FROM openalex_funders f WHERE f.id = s.funder_id)
    ON CONFLICT (work_id, funder_id) DO NOTHING
  `);

  await client.query("TRUNCATE stage_works_main, stage_works_authorships, stage_works_topics, stage_works_awards, stage_works_funders");
}

async function flushBatch(client, entity, records) {
  if (entity in SIMPLE_ENTITY) return flushSimple(client, entity, records);
  if (entity === "sources") return flushSources(client, records);
  if (entity === "authors") return flushAuthors(client, records);
  if (entity === "awards") return flushAwards(client, records);
  if (entity === "works") return flushWorks(client, records);
  throw new Error(`Unknown entity "${entity}"`);
}

async function loadFile(client, entity, file) {
  const key = file.url.replace(`s3://${BUCKET}/`, "");
  const { Body } = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  const rl = readline.createInterface({ input: Body.pipe(zlib.createGunzip()) });

  let batch = [];
  for await (const line of rl) {
    if (!line.trim()) continue;
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      continue; // skip a malformed line rather than abort the whole file
    }
    if (!record.id) continue;
    batch.push(record);
    if (batch.length >= BATCH_SIZE) {
      await flushBatch(client, entity, batch);
      batch = [];
    }
  }
  if (batch.length > 0) await flushBatch(client, entity, batch);
}

async function createClientPool(entity, size) {
  const clients = [];
  for (let i = 0; i < size; i++) {
    const client = new Client(DB_CONFIG);
    await client.connect();
    await ensureStagingTables(client, entity);
    clients.push(client);
  }
  return clients;
}

// Simple client checkout/return so pg-copy-streams always gets an
// exclusive connection for the full duration of one COPY stream, while
// p-limit(CONCURRENCY) still gates how many files are in flight at once.
function makePool(clients) {
  const available = [...clients];
  const waiters = [];
  return {
    acquire() {
      if (available.length > 0) return Promise.resolve(available.pop());
      return new Promise((resolve) => waiters.push(resolve));
    },
    release(client) {
      const waiter = waiters.shift();
      if (waiter) waiter(client);
      else available.push(client);
    },
  };
}

async function withShortClient(fn) {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

async function main() {
  const parsed = parseArgs();
  if (parsed.truncateAll) {
    return truncateAll();
  }
  const { entity } = parsed;
  console.log(`Fetching file list for "${entity}" from manifest.json...`);
  const files = await getFileList(entity);
  const totalRecords = files.reduce((sum, f) => sum + (f.meta?.record_count ?? 0), 0);
  console.log(`${entity}: ${files.length} files, ~${totalRecords.toLocaleString()} records`);
  if (entity === "works") {
    console.log("Note: authorships/topics/awards/funders referencing authors/awards/funders not yet loaded are skipped, not lost -- re-run `works` again after those finish to backfill them.");
  }
  if (entity === "sources") {
    console.log("Note: publisher_id is only set if that publisher is already loaded -- run `funders`/`publishers` before `sources` if you want that link filled in (see ENTITY_ORDER).");
  }

  const clients = await createClientPool(entity, CONCURRENCY);
  const pool = makePool(clients);
  const limit = pLimit(CONCURRENCY);

  let filesDone = 0;
  await Promise.all(
    files.map((file) =>
      limit(async () => {
        const client = await pool.acquire();
        try {
          await loadFile(client, entity, file);
          filesDone++;
          console.log(`[${entity}] ${filesDone}/${files.length} files loaded`);
        } finally {
          pool.release(client);
        }
      })
    )
  );

  for (const client of clients) await client.end();
  console.log(`${entity}: done.`);
}

// Only run the CLI when this file is executed directly (node load.mjs),
// not when imported as a module (e.g. for unit-testing the extract*
// functions in isolation).
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error("Load failed:", err);
    process.exit(1);
  });
}

-- Reference only -- these tables now live in prisma/schema.prisma in the
-- scholarmatch project (OpenAlexTopic/Source/Institution/Publisher/
-- Funder/Author/Award/Work + 6 join tables), created there via a
-- migration, not by running this file directly. Kept here so the shape
-- this script targets is documented in one place, in sync with that
-- schema.
--
-- Real typed columns, not JSON blobs -- meant to be queried/used directly.
-- Real foreign keys on the join tables; load.mjs only inserts a join row
-- once its parent(s) already exist (EXISTS-guarded INSERT), so these
-- constraints should never actually reject a row in practice.
--
-- id (and every FK column referencing one) is OpenAlex's BARE id, e.g.
-- "A5028125522" -- NOT the full "https://openalex.org/A5028125522" URI.
-- load.mjs's stripOpenAlexId() strips this on every extract*() function.

-- No dependencies
CREATE TABLE IF NOT EXISTS openalex_topics (
  id             text PRIMARY KEY,
  display_name   text,
  field_name     text,
  subfield_name  text,
  domain_name    text,
  works_count    integer,
  cited_by_count integer,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS openalex_institutions (
  id             text PRIMARY KEY,
  display_name   text,
  type           text,
  country_code   text,
  ror_id         text,
  works_count    integer,
  cited_by_count integer,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS openalex_publishers (
  id             text PRIMARY KEY,
  display_name   text,
  country_code   text,
  works_count    integer,
  cited_by_count integer,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS openalex_funders (
  id             text PRIMARY KEY,
  display_name   text,
  country_code   text,
  description    text,
  homepage_url   text,
  awards_count   integer,
  works_count    integer,
  cited_by_count integer,
  h_index        integer,
  i10_index      integer,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Depends on publishers (publisher_id, only set when host_organization
-- is a real publisher id, not an institution-hosted source)
CREATE TABLE IF NOT EXISTS openalex_sources (
  id             text PRIMARY KEY,
  display_name   text,
  type           text,
  issn_l         text,
  country_code   text,
  works_count    integer,
  cited_by_count integer,
  publisher_id   text REFERENCES openalex_publishers(id),
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Depends on funders
CREATE TABLE IF NOT EXISTS openalex_awards (
  id              text PRIMARY KEY,
  display_name    text,
  description     text,
  funder_award_id text,
  funder_id       text REFERENCES openalex_funders(id),
  amount          double precision,
  currency        text,
  funding_type    text,
  start_year      integer,
  end_year        integer,
  doi             text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Depends on institutions (last_known_institution_id)
CREATE TABLE IF NOT EXISTS openalex_authors (
  id                        text PRIMARY KEY,
  orcid_id                  text UNIQUE,
  display_name              text,
  works_count               integer,
  cited_by_count            integer,
  h_index                   integer,
  i10_index                 integer,
  last_known_institution_id text REFERENCES openalex_institutions(id),
  created_at                timestamptz NOT NULL DEFAULT now(),
  updated_at                timestamptz NOT NULL DEFAULT now()
);

-- Depends on sources (primary_source_id)
CREATE TABLE IF NOT EXISTS openalex_works (
  id                text PRIMARY KEY,
  title             text,
  publication_year  integer,
  publication_date  date,
  type              text,
  doi               text,
  cited_by_count    integer,
  primary_source_id text REFERENCES openalex_sources(id),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

-- Join tables

CREATE TABLE IF NOT EXISTS openalex_author_affiliations (
  id             text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  author_id      text NOT NULL REFERENCES openalex_authors(id) ON DELETE CASCADE,
  institution_id text NOT NULL REFERENCES openalex_institutions(id) ON DELETE CASCADE,
  years          integer[],
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (author_id, institution_id)
);

CREATE TABLE IF NOT EXISTS openalex_author_topics (
  id        text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  author_id text NOT NULL REFERENCES openalex_authors(id) ON DELETE CASCADE,
  topic_id  text NOT NULL REFERENCES openalex_topics(id) ON DELETE CASCADE,
  count     integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (author_id, topic_id)
);

CREATE TABLE IF NOT EXISTS openalex_work_authorships (
  id              text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  work_id         text NOT NULL REFERENCES openalex_works(id) ON DELETE CASCADE,
  author_id       text NOT NULL REFERENCES openalex_authors(id) ON DELETE CASCADE,
  institution_id  text REFERENCES openalex_institutions(id),
  author_position text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (work_id, author_id)
);

CREATE TABLE IF NOT EXISTS openalex_work_topics (
  id       text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  work_id  text NOT NULL REFERENCES openalex_works(id) ON DELETE CASCADE,
  topic_id text NOT NULL REFERENCES openalex_topics(id) ON DELETE CASCADE,
  score    double precision,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (work_id, topic_id)
);

-- work <-> award, from the WORK's own awards[] field -- NOT from the
-- award's own funded_outputs[], which is capped at 100 (verified live,
-- unreliable for a large grant with far more real outputs than that).
CREATE TABLE IF NOT EXISTS openalex_work_awards (
  id              text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  work_id         text NOT NULL REFERENCES openalex_works(id) ON DELETE CASCADE,
  award_id        text NOT NULL REFERENCES openalex_awards(id) ON DELETE CASCADE,
  funder_award_id text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (work_id, award_id)
);

-- work <-> funder, from the WORK's own funders[] field -- kept separate
-- from openalex_work_awards since a work's funders list is often
-- populated even with no specific award number attached.
CREATE TABLE IF NOT EXISTS openalex_work_funders (
  id         text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  work_id    text NOT NULL REFERENCES openalex_works(id) ON DELETE CASCADE,
  funder_id  text NOT NULL REFERENCES openalex_funders(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (work_id, funder_id)
);

CREATE INDEX IF NOT EXISTS idx_openalex_authors_last_known_institution ON openalex_authors (last_known_institution_id);
CREATE INDEX IF NOT EXISTS idx_openalex_works_primary_source ON openalex_works (primary_source_id);
CREATE INDEX IF NOT EXISTS idx_openalex_sources_publisher ON openalex_sources (publisher_id);
CREATE INDEX IF NOT EXISTS idx_openalex_awards_funder ON openalex_awards (funder_id);
CREATE INDEX IF NOT EXISTS idx_openalex_author_affiliations_institution ON openalex_author_affiliations (institution_id);
CREATE INDEX IF NOT EXISTS idx_openalex_author_topics_topic ON openalex_author_topics (topic_id);
CREATE INDEX IF NOT EXISTS idx_openalex_work_authorships_author ON openalex_work_authorships (author_id);
CREATE INDEX IF NOT EXISTS idx_openalex_work_topics_topic ON openalex_work_topics (topic_id);
CREATE INDEX IF NOT EXISTS idx_openalex_work_awards_award ON openalex_work_awards (award_id);
CREATE INDEX IF NOT EXISTS idx_openalex_work_funders_funder ON openalex_work_funders (funder_id);
CREATE INDEX IF NOT EXISTS idx_openalex_works_publication_year ON openalex_works (publication_year);

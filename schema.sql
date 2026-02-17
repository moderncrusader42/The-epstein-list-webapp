-- db/schema.sql
-- Bootstrap schema: create schema and tables only.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'app') THEN
    EXECUTE 'CREATE SCHEMA app';
  END IF;
END$$;

-- Canonical application users, linked from proposals/events and privileges.
CREATE TABLE IF NOT EXISTS app."user" (
  id         BIGSERIAL PRIMARY KEY,
  name     TEXT NOT NULL,
  username   TEXT,
  email     TEXT NOT NULL UNIQUE,
  is_active  BOOLEAN NOT NULL DEFAULT TRUE
);

-- Role flags controlling who can submit, review, and administer content.
CREATE TABLE IF NOT EXISTS app.user_privileges (
  email            TEXT PRIMARY KEY REFERENCES app."user"(email) ON UPDATE CASCADE ON DELETE CASCADE,
  base_user         BOOLEAN NOT NULL DEFAULT FALSE,
  reviewer          BOOLEAN NOT NULL DEFAULT FALSE,
  editor            BOOLEAN NOT NULL DEFAULT FALSE,
  admin             BOOLEAN NOT NULL DEFAULT FALSE,
  creator           BOOLEAN NOT NULL DEFAULT FALSE
);

-- Canonical person identities, so cards/tags can reference a stable person_id.
CREATE TABLE IF NOT EXISTS app.people (
  id          BIGSERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Shared title catalog (e.g., Defender, Carpenter) reused across many people.
CREATE TABLE IF NOT EXISTS app.people_titles (
  id          BIGSERIAL PRIMARY KEY,
  code        TEXT NOT NULL,
  label       TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Shared tag catalog reused across people and card filters.
CREATE TABLE IF NOT EXISTS app.people_tags (
  id          BIGSERIAL PRIMARY KEY,
  code        TEXT NOT NULL,
  label       TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Card-facing record: slug, person link, title link, and image location metadata.
CREATE TABLE IF NOT EXISTS app.people_cards (
  id          BIGSERIAL PRIMARY KEY,
  slug        TEXT NOT NULL UNIQUE,
  person_id   BIGINT NOT NULL REFERENCES app.people(id) ON UPDATE CASCADE ON DELETE CASCADE,
  bucket      TEXT NOT NULL,
  title_id    BIGINT NOT NULL REFERENCES app.people_titles(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  image_url   TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Article markdown body for each card, stored separately from card metadata.
CREATE TABLE IF NOT EXISTS app.people_articles (
  id          BIGSERIAL PRIMARY KEY,
  person_slug TEXT NOT NULL UNIQUE REFERENCES app.people_cards(slug) ON UPDATE CASCADE ON DELETE CASCADE,
  markdown    TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Many-to-many link between people and tags.
CREATE TABLE IF NOT EXISTS app.people_person_tags (
  person_id   BIGINT NOT NULL REFERENCES app.people(id) ON UPDATE CASCADE ON DELETE CASCADE,
  tag_id      BIGINT NOT NULL REFERENCES app.people_tags(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (person_id, tag_id)
);

-- Proposal lifecycle state: who proposed, current status, reviewer, and submitted payload.
-- Payload format:
-- - `proposal_scope='article'`: payload is raw markdown text.
-- - `proposal_scope='card'`: payload is JSON text:
--   {"name":"<name>","title":"<title>","tags":["<tag>", "..."],"image_url":"<url>"}
-- - `proposal_scope='card_article'`: payload is JSON text with both card and article:
--   {"card":{"name":"<name>","title":"<title>","tags":["<tag>"],"image_url":"<url>"},"article":"<markdown>"}
CREATE TABLE IF NOT EXISTS app.people_change_proposals (
  id                 BIGSERIAL PRIMARY KEY,
  person_slug        TEXT NOT NULL,
  person_id          BIGINT NOT NULL REFERENCES app.people(id) ON UPDATE CASCADE ON DELETE CASCADE,
  proposer_user_id   BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  proposal_scope     TEXT NOT NULL DEFAULT 'article',
  base_payload       TEXT NOT NULL,
  proposed_payload   TEXT NOT NULL,
  note               TEXT,
  status             TEXT NOT NULL DEFAULT 'pending',
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at        TIMESTAMPTZ,
  reviewer_user_id   BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  review_note        TEXT,
  report_triggered   INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT chk_people_change_scope
    CHECK (lower(proposal_scope) IN ('article', 'card', 'description', 'card_article')),
  CONSTRAINT chk_people_change_status
    CHECK (lower(status) IN ('pending', 'accepted', 'declined', 'reported')),
  CONSTRAINT chk_people_change_report_triggered
    CHECK (report_triggered IN (0, 1))
);

-- Optional audit timeline for actions taken on a proposal.
CREATE TABLE IF NOT EXISTS app.people_change_events (
  id           BIGSERIAL PRIMARY KEY,
  proposal_id  BIGINT NOT NULL REFERENCES app.people_change_proposals(id) ON DELETE CASCADE,
  event_type   TEXT NOT NULL,
  actor_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
  notes        TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Canonical theory identities for the independent Theories section.
CREATE TABLE IF NOT EXISTS app.theories (
  id          BIGSERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Shared theory title catalog.
CREATE TABLE IF NOT EXISTS app.theory_titles (
  id          BIGSERIAL PRIMARY KEY,
  code        TEXT NOT NULL,
  label       TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Shared theory tag catalog.
CREATE TABLE IF NOT EXISTS app.theory_tags (
  id          BIGSERIAL PRIMARY KEY,
  code        TEXT NOT NULL,
  label       TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Theory card metadata.
CREATE TABLE IF NOT EXISTS app.theory_cards (
  id          BIGSERIAL PRIMARY KEY,
  slug        TEXT NOT NULL UNIQUE,
  person_id   BIGINT NOT NULL REFERENCES app.theories(id) ON UPDATE CASCADE ON DELETE CASCADE,
  bucket      TEXT NOT NULL,
  title_id    BIGINT NOT NULL REFERENCES app.theory_titles(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  image_url   TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Theory markdown article content.
CREATE TABLE IF NOT EXISTS app.theory_articles (
  id          BIGSERIAL PRIMARY KEY,
  person_slug TEXT NOT NULL UNIQUE REFERENCES app.theory_cards(slug) ON UPDATE CASCADE ON DELETE CASCADE,
  markdown    TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Many-to-many link between theories and tags.
CREATE TABLE IF NOT EXISTS app.theory_person_tags (
  person_id   BIGINT NOT NULL REFERENCES app.theories(id) ON UPDATE CASCADE ON DELETE CASCADE,
  tag_id      BIGINT NOT NULL REFERENCES app.theory_tags(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (person_id, tag_id)
);

-- Proposal lifecycle for theory edits.
CREATE TABLE IF NOT EXISTS app.theory_change_proposals (
  id                 BIGSERIAL PRIMARY KEY,
  person_slug        TEXT NOT NULL,
  person_id          BIGINT NOT NULL REFERENCES app.theories(id) ON UPDATE CASCADE ON DELETE CASCADE,
  proposer_user_id   BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  proposal_scope     TEXT NOT NULL DEFAULT 'article',
  base_payload       TEXT NOT NULL,
  proposed_payload   TEXT NOT NULL,
  note               TEXT,
  status             TEXT NOT NULL DEFAULT 'pending',
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at        TIMESTAMPTZ,
  reviewer_user_id   BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  review_note        TEXT,
  report_triggered   INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT chk_theory_change_scope
    CHECK (lower(proposal_scope) IN ('article', 'card', 'description', 'card_article')),
  CONSTRAINT chk_theory_change_status
    CHECK (lower(status) IN ('pending', 'accepted', 'declined', 'reported')),
  CONSTRAINT chk_theory_change_report_triggered
    CHECK (report_triggered IN (0, 1))
);

-- Optional audit timeline for actions taken on theory proposals.
CREATE TABLE IF NOT EXISTS app.theory_change_events (
  id            BIGSERIAL PRIMARY KEY,
  proposal_id   BIGINT NOT NULL REFERENCES app.theory_change_proposals(id) ON DELETE CASCADE,
  event_type    TEXT NOT NULL,
  actor_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
  notes         TEXT,
  payload_json  TEXT NOT NULL DEFAULT '{}',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Source card metadata (folder-backed knowledge sources with quotas).
CREATE TABLE IF NOT EXISTS app.sources_cards (
  id                 BIGSERIAL PRIMARY KEY,
  slug               TEXT NOT NULL UNIQUE,
  name               TEXT NOT NULL,
  description_markdown TEXT NOT NULL DEFAULT '',
  bucket             TEXT NOT NULL,
  folder_prefix      TEXT NOT NULL UNIQUE,
  cover_media_url    TEXT NOT NULL DEFAULT '',
  max_bytes          BIGINT NOT NULL DEFAULT 1073741824,
  created_by_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_sources_cards_max_bytes
    CHECK (max_bytes > 0)
);

-- Shared source tag catalog reused by source cards and filters.
CREATE TABLE IF NOT EXISTS app.sources_tags (
  id          BIGSERIAL PRIMARY KEY,
  code        TEXT NOT NULL,
  label       TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Many-to-many link between sources and tags.
CREATE TABLE IF NOT EXISTS app.sources_card_tags (
  source_id   BIGINT NOT NULL REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
  tag_id      BIGINT NOT NULL REFERENCES app.sources_tags(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, tag_id)
);

-- Uploaded file metadata for each source folder in bucket storage.
CREATE TABLE IF NOT EXISTS app.sources_files (
  id                  BIGSERIAL PRIMARY KEY,
  source_id           BIGINT NOT NULL REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
  blob_path           TEXT NOT NULL UNIQUE,
  file_name           TEXT NOT NULL,
  origin_url          TEXT NOT NULL DEFAULT '',
  mime_type           TEXT,
  size_bytes          BIGINT NOT NULL DEFAULT 0,
  uploaded_by_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_sources_files_size_bytes
    CHECK (size_bytes >= 0)
);

-- Source edit proposals routed to review/approval, mirroring people proposal flow.
CREATE TABLE IF NOT EXISTS app.sources_change_proposals (
  id                 BIGSERIAL PRIMARY KEY,
  source_slug        TEXT NOT NULL,
  source_id          BIGINT REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
  proposer_user_id   BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  proposal_scope     TEXT NOT NULL DEFAULT 'source',
  base_payload       TEXT NOT NULL DEFAULT '',
  proposed_payload   TEXT NOT NULL DEFAULT '',
  note               TEXT,
  status             TEXT NOT NULL DEFAULT 'pending',
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at        TIMESTAMPTZ,
  reviewer_user_id   BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  review_note        TEXT,
  report_triggered   INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT chk_sources_change_scope
    CHECK (lower(proposal_scope) IN ('source')),
  CONSTRAINT chk_sources_change_status
    CHECK (lower(status) IN ('pending', 'accepted', 'declined', 'reported')),
  CONSTRAINT chk_sources_change_report_triggered
    CHECK (report_triggered IN (0, 1))
);

-- Optional audit timeline for actions taken on source proposals.
CREATE TABLE IF NOT EXISTS app.sources_change_events (
  id           BIGSERIAL PRIMARY KEY,
  proposal_id  BIGINT NOT NULL REFERENCES app.sources_change_proposals(id) ON DELETE CASCADE,
  event_type   TEXT NOT NULL,
  actor_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
  notes        TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_email_lower
  ON app."user" (lower(email));

CREATE INDEX IF NOT EXISTS idx_user_username_lower
  ON app."user" (lower(username));

CREATE INDEX IF NOT EXISTS idx_people_cards_bucket
  ON app.people_cards(bucket);

CREATE INDEX IF NOT EXISTS idx_people_cards_person_id
  ON app.people_cards(person_id);

CREATE INDEX IF NOT EXISTS idx_people_cards_title_id
  ON app.people_cards(title_id);

CREATE INDEX IF NOT EXISTS idx_people_titles_code
  ON app.people_titles(code);

CREATE INDEX IF NOT EXISTS idx_people_name
  ON app.people(name);

CREATE INDEX IF NOT EXISTS idx_people_tags_code
  ON app.people_tags(code);

CREATE INDEX IF NOT EXISTS idx_people_articles_person_slug
  ON app.people_articles(person_slug);

CREATE INDEX IF NOT EXISTS idx_people_person_tags_tag_id
  ON app.people_person_tags(tag_id);

CREATE INDEX IF NOT EXISTS idx_people_change_proposals_slug
  ON app.people_change_proposals(person_slug);

CREATE INDEX IF NOT EXISTS idx_people_change_proposals_person_id
  ON app.people_change_proposals(person_id);

CREATE INDEX IF NOT EXISTS idx_people_change_proposals_status
  ON app.people_change_proposals(status);

CREATE INDEX IF NOT EXISTS idx_people_change_proposals_created_at
  ON app.people_change_proposals(created_at);

CREATE INDEX IF NOT EXISTS idx_people_change_proposals_proposer_user_id
  ON app.people_change_proposals(proposer_user_id);

CREATE INDEX IF NOT EXISTS idx_people_change_proposals_reviewer_user_id
  ON app.people_change_proposals(reviewer_user_id);

CREATE INDEX IF NOT EXISTS idx_people_change_events_proposal
  ON app.people_change_events(proposal_id);

CREATE INDEX IF NOT EXISTS idx_people_change_events_created_at
  ON app.people_change_events(created_at);

CREATE INDEX IF NOT EXISTS idx_people_change_events_actor_user_id
  ON app.people_change_events(actor_user_id);

CREATE INDEX IF NOT EXISTS idx_theory_cards_bucket
  ON app.theory_cards(bucket);

CREATE INDEX IF NOT EXISTS idx_theory_cards_person_id
  ON app.theory_cards(person_id);

CREATE INDEX IF NOT EXISTS idx_theory_cards_title_id
  ON app.theory_cards(title_id);

CREATE INDEX IF NOT EXISTS idx_theory_titles_code
  ON app.theory_titles(code);

CREATE INDEX IF NOT EXISTS idx_theory_name
  ON app.theories(name);

CREATE INDEX IF NOT EXISTS idx_theory_tags_code
  ON app.theory_tags(code);

CREATE INDEX IF NOT EXISTS idx_theory_articles_person_slug
  ON app.theory_articles(person_slug);

CREATE INDEX IF NOT EXISTS idx_theory_person_tags_tag_id
  ON app.theory_person_tags(tag_id);

CREATE INDEX IF NOT EXISTS idx_theory_change_proposals_slug
  ON app.theory_change_proposals(person_slug);

CREATE INDEX IF NOT EXISTS idx_theory_change_proposals_person_id
  ON app.theory_change_proposals(person_id);

CREATE INDEX IF NOT EXISTS idx_theory_change_proposals_status
  ON app.theory_change_proposals(status);

CREATE INDEX IF NOT EXISTS idx_theory_change_proposals_created_at
  ON app.theory_change_proposals(created_at);

CREATE INDEX IF NOT EXISTS idx_theory_change_proposals_proposer_user_id
  ON app.theory_change_proposals(proposer_user_id);

CREATE INDEX IF NOT EXISTS idx_theory_change_proposals_reviewer_user_id
  ON app.theory_change_proposals(reviewer_user_id);

CREATE INDEX IF NOT EXISTS idx_theory_change_events_proposal
  ON app.theory_change_events(proposal_id);

CREATE INDEX IF NOT EXISTS idx_theory_change_events_created_at
  ON app.theory_change_events(created_at);

CREATE INDEX IF NOT EXISTS idx_theory_change_events_actor_user_id
  ON app.theory_change_events(actor_user_id);

CREATE INDEX IF NOT EXISTS idx_sources_cards_name
  ON app.sources_cards(name);

CREATE INDEX IF NOT EXISTS idx_sources_cards_bucket
  ON app.sources_cards(bucket);

CREATE INDEX IF NOT EXISTS idx_sources_cards_folder_prefix
  ON app.sources_cards(folder_prefix);

CREATE INDEX IF NOT EXISTS idx_sources_cards_created_by_user_id
  ON app.sources_cards(created_by_user_id);

CREATE INDEX IF NOT EXISTS idx_sources_tags_code
  ON app.sources_tags(code);

CREATE INDEX IF NOT EXISTS idx_sources_card_tags_tag_id
  ON app.sources_card_tags(tag_id);

CREATE INDEX IF NOT EXISTS idx_sources_files_source_id
  ON app.sources_files(source_id);

CREATE INDEX IF NOT EXISTS idx_sources_files_created_at
  ON app.sources_files(created_at);

CREATE INDEX IF NOT EXISTS idx_sources_files_uploaded_by_user_id
  ON app.sources_files(uploaded_by_user_id);

CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_source_slug
  ON app.sources_change_proposals(source_slug);

CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_source_id
  ON app.sources_change_proposals(source_id);

CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_status
  ON app.sources_change_proposals(status);

CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_created_at
  ON app.sources_change_proposals(created_at);

CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_proposer_user_id
  ON app.sources_change_proposals(proposer_user_id);

CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_reviewer_user_id
  ON app.sources_change_proposals(reviewer_user_id);

CREATE INDEX IF NOT EXISTS idx_sources_change_events_proposal
  ON app.sources_change_events(proposal_id);

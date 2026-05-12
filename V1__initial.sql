CREATE TABLE users (
    id              BIGSERIAL PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    password_salt   TEXT NOT NULL,
    name            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE papers (
    id                  BIGSERIAL PRIMARY KEY,
    title               TEXT NOT NULL,
    doi                 TEXT UNIQUE,
    publication_year    SMALLINT,
    publication_date    DATE,
    type                TEXT,
    language            TEXT,
    abstract            TEXT,
    is_open_access      BOOLEAN,
    cited_by_count      INTEGER DEFAULT 0,

    created_by_user_id  BIGINT REFERENCES users(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE user_favourite_papers (
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    paper_id    BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, paper_id)
);

CREATE TABLE authors (
    id              BIGSERIAL PRIMARY KEY,
    display_name    TEXT NOT NULL,
    orcid           TEXT UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE institutions (
    id              BIGSERIAL PRIMARY KEY,
    display_name    TEXT NOT NULL,
    ror             TEXT UNIQUE,
    country_code    CHAR(2),
    type            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE paper_authors (
    paper_id        BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    author_id       BIGINT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,

    author_order    INTEGER,
    is_corresponding BOOLEAN DEFAULT false,

    PRIMARY KEY (paper_id, author_id)
);

CREATE TABLE author_institutions (
    author_id        BIGINT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    institution_id   BIGINT NOT NULL REFERENCES institutions(id) ON DELETE CASCADE,

    PRIMARY KEY (author_id, institution_id)
);

-- Справочник источников внешних идентификаторов / ссылок
CREATE TABLE meta_sources (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    prefix      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE paper_meta_sources (
    paper_id        BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    meta_source_id  BIGINT NOT NULL REFERENCES meta_sources(id) ON DELETE CASCADE,

    external_id     TEXT NOT NULL,

    PRIMARY KEY (paper_id, meta_source_id),
    UNIQUE (meta_source_id, external_id)
);

-- Справочник ссылок доступа
CREATE TABLE landings (
    id              BIGSERIAL PRIMARY KEY,
    paper_id        BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,

    landing_url     TEXT NOT NULL,
    pdf_url         TEXT,
    license         TEXT,
    version         TEXT,
    is_best         BOOLEAN DEFAULT false,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (paper_id, landing_url)
);


-- OpenAlex taxonomy: domains -> fields -> subfields -> topics
CREATE TABLE domains (
    id              BIGSERIAL PRIMARY KEY,
    openalex_id     TEXT UNIQUE,
    name            TEXT NOT NULL UNIQUE
);

CREATE TABLE fields (
    id              BIGSERIAL PRIMARY KEY,
    domain_id       BIGINT REFERENCES domains(id) ON DELETE SET NULL,
    openalex_id     TEXT UNIQUE,
    name            TEXT NOT NULL
);

CREATE TABLE subfields (
    id              BIGSERIAL PRIMARY KEY,
    field_id        BIGINT REFERENCES fields(id) ON DELETE SET NULL,
    openalex_id     TEXT UNIQUE,
    name            TEXT NOT NULL
);

CREATE TABLE topics (
    id              BIGSERIAL PRIMARY KEY,
    subfield_id     BIGINT REFERENCES subfields(id) ON DELETE SET NULL,
    openalex_id     TEXT UNIQUE,
    name            TEXT NOT NULL
);

CREATE TABLE paper_topics (
    paper_id    BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    topic_id    BIGINT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,

    score       NUMERIC(6, 5),

    PRIMARY KEY (paper_id, topic_id)
);

CREATE TABLE keywords (
    id          BIGSERIAL PRIMARY KEY,
    value       TEXT NOT NULL UNIQUE
);

CREATE TABLE paper_keywords (
    paper_id    BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    keyword_id  BIGINT NOT NULL REFERENCES keywords(id) ON DELETE CASCADE,

    score       NUMERIC(6, 5),

    PRIMARY KEY (paper_id, keyword_id)
);

CREATE TABLE user_tracked_domains (
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    domain_id   BIGINT NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, domain_id)
);

CREATE TABLE user_tracked_keywords (
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    keyword_id  BIGINT NOT NULL REFERENCES keywords(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, keyword_id)
);

CREATE TABLE user_tracked_topics (
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    topic_id    BIGINT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, topic_id)
);

CREATE TABLE user_tracked_subfields (
    user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subfield_id  BIGINT NOT NULL REFERENCES subfields(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, subfield_id)
);


CREATE INDEX idx_papers_title ON papers USING gin (to_tsvector('simple', title));
CREATE INDEX idx_papers_year ON papers (publication_year);
CREATE INDEX idx_user_favourite_papers_paper_id ON user_favourite_papers (paper_id);
CREATE INDEX idx_authors_display_name ON authors (display_name);
CREATE INDEX idx_institutions_display_name ON institutions (display_name);

CREATE INDEX idx_paper_authors_author_id ON paper_authors (author_id);
CREATE INDEX idx_author_institutions_institution_id ON author_institutions (institution_id);

CREATE INDEX idx_paper_topics_topic_id ON paper_topics (topic_id);
CREATE INDEX idx_paper_keywords_keyword_id ON paper_keywords (keyword_id);

CREATE INDEX idx_landings_paper_id ON landings (paper_id);
CREATE INDEX idx_paper_meta_sources_external_id ON paper_meta_sources (external_id);
CREATE TABLE openalex_yearly_topic_stats (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT REFERENCES topics(id) ON DELETE SET NULL,
    stat_year date NOT NULL,
    works_count INTEGER NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, stat_year)
);

CREATE INDEX idx_yearly_stats_topic_id ON openalex_yearly_topic_stats (topic_id);

ALTER TABLE openalex_yearly_topic_stats 
ADD COLUMN artifical_pubdates_estimation INTEGER NOT NULL Default 0

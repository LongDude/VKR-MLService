from enum import StrEnum


class CachePolicy(StrEnum):
    LOCAL_ONLY = "local_only"
    LOCAL_FIRST_FETCH_MISSING = "local_first_fetch_missing"
    FORCE_EXTERNAL_REFRESH = "force_external_refresh"


class IndexingStatus(StrEnum):
    NOT_INDEXED = "not_indexed"
    PENDING = "pending"
    INDEXED = "indexed"
    FAILED = "failed"


class PaperSource(StrEnum):
    LOCAL_CACHE = "local_cache"
    EXTERNAL_OPENALEX = "external_openalex"
    NOT_FOUND = "not_found"


class WorkflowGranularity(StrEnum):
    WEEK = "week"
    MONTH = "month"


class TrackedEntityType(StrEnum):
    DOMAIN = "domain"
    FIELD = "field"
    SUBFIELD = "subfield"
    TOPIC = "topic"
    KEYWORD = "keyword"

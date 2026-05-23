from .associations import (
    AuthorInstitution,
    PaperAuthor,
    PaperKeyword,
    PaperTopic,
    UserFavouritePaper,
    UserTrackedDomain,
    UserTrackedField,
    UserTrackedKeyword,
    UserTrackedSubfield,
    UserTrackedTopic,
)
from .analytics import (
    OpenAlexMonthlyTopicStat,
    OpenAlexYearlyTopicStat,
    ResearchCluster,
    ResearchClusterPeriodStat,
    TopicQuarterReport,
    TopicQuarterReportItem,
    TopicQuarterReportPaper,
)
from .author import Author
from .base import Base
from .institution import Institution
from .keyword import Keyword
from .paper import Landing, Paper
from .topic import Domain, Field, Subfield, Topic
from .user import User

__all__ = [
    "Author",
    "AuthorInstitution",
    "Base",
    "Domain",
    "Field",
    "Institution",
    "Keyword",
    "Landing",
    "OpenAlexMonthlyTopicStat",
    "OpenAlexYearlyTopicStat",
    "Paper",
    "PaperAuthor",
    "PaperKeyword",
    "PaperTopic",
    "ResearchCluster",
    "ResearchClusterPeriodStat",
    "Subfield",
    "Topic",
    "TopicQuarterReport",
    "TopicQuarterReportItem",
    "TopicQuarterReportPaper",
    "User",
    "UserFavouritePaper",
    "UserTrackedDomain",
    "UserTrackedField",
    "UserTrackedKeyword",
    "UserTrackedSubfield",
    "UserTrackedTopic",
]

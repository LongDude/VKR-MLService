from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dto.charts import ChartDTO
from dto.common import BatchOperationResultDTO, DateRangeDTO, PaginationDTO, SortDTO
from dto.embeddings import EmbeddingResultDTO
from dto.errors import ErrorDetailDTO, ErrorResponseDTO
from dto.external import ExternalAuthorDTO, ExternalPaperDTO
from dto.papers import PaperCreateDTO, PaperResolveRequestDTO, PaperResolveResponseDTO
from dto.qdrant import QdrantPointDTO
from dto.tracked import AddTrackedEntityRequestDTO


def test_common_dto_defaults_and_validation() -> None:
    assert PaginationDTO().model_dump() == {"limit": 20, "offset": 0}
    assert DateRangeDTO().model_dump() == {"date_from": None, "date_to": None}
    assert SortDTO().sort_order == "desc"
    assert BatchOperationResultDTO().errors == []

    with pytest.raises(ValidationError):
        PaginationDTO(limit=101)


def test_error_response_wire_shape() -> None:
    response = ErrorResponseDTO(
        error=ErrorDetailDTO(
            code="entity_not_found",
            message="Paper not found",
            details={"paper_id": 123},
        )
    )

    assert response.model_dump() == {
        "error": {
            "code": "entity_not_found",
            "message": "Paper not found",
            "details": {"paper_id": 123},
        }
    }


def test_paper_resolve_contract_defaults() -> None:
    request = PaperResolveRequestDTO(doi="10.1000/demo")
    response = PaperResolveResponseDTO(
        paper=None,
        source="not_found",
        was_created_locally=False,
        indexing_status=None,
    )

    assert request.source_name == "openalex"
    assert request.fetch_missing is True
    assert request.persist_missing is True
    assert request.cache_policy == "local_first_fetch_missing"
    assert response.source == "not_found"


def test_required_dto_models_validate_minimal_payloads() -> None:
    assert PaperCreateDTO(title="A paper").cited_by_count == 0
    assert PaperCreateDTO(title="A paper", extracted_keywords=["graph"]).extracted_keywords == ["graph"]
    assert PaperCreateDTO(title="A paper", openalex_id="W1").openalex_id == "W1"
    assert ExternalPaperDTO(title="External", references_count=3).references_count == 3
    assert EmbeddingResultDTO(vector=[0.1, 0.2], model="local", dimension=2).dimension == 2
    assert QdrantPointDTO(id="p1", vector=[0.1], payload={"paper_id": 1}).id == "p1"
    assert AddTrackedEntityRequestDTO(entity_type="field", entity_id=10).entity_id == 10
    assert ChartDTO(chart_id="c1", chart_type="line", title="Trend").series == []


def test_list_defaults_are_not_shared_between_instances() -> None:
    first = ExternalPaperDTO(title="A")
    second = ExternalPaperDTO(title="B")

    first.authors.append(ExternalAuthorDTO(display_name="Ada"))

    assert len(first.authors) == 1
    assert second.authors == []

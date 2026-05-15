from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.analytics import PaperProcessingState

from .base import BaseRepository


class PaperProcessingStateRepository(BaseRepository):
    """Persist ML processing state for papers.

    The repository is intentionally PostgreSQL-first for bulk state updates: the
    production path uses ``INSERT ... ON CONFLICT`` by ``paper_id`` so parallel
    import/indexing workers can update the state row without racing through a
    select-then-insert sequence. SQLite and other dialects keep a small ORM
    fallback for tests and local smoke checks.
    """

    def get_by_paper_id(self, paper_id: int) -> PaperProcessingState | None:
        """Return processing state for one paper."""
        return self.session.get(PaperProcessingState, paper_id)

    def get_by_paper_ids(
        self,
        paper_ids: list[int],
    ) -> dict[int, PaperProcessingState]:
        """Return processing states keyed by paper id."""
        if not paper_ids:
            return {}
        stmt = select(PaperProcessingState).where(
            PaperProcessingState.paper_id.in_(set(paper_ids))
        )
        states = self.session.scalars(stmt).all()
        return {int(state.paper_id): state for state in states}

    def get_indexed_text_hashes(self, paper_ids: list[int]) -> dict[int, str]:
        """Return text hashes for papers currently marked as indexed."""
        if not paper_ids:
            return {}
        stmt = select(
            PaperProcessingState.paper_id,
            PaperProcessingState.text_hash,
        ).where(
            PaperProcessingState.paper_id.in_(set(paper_ids)),
            PaperProcessingState.embedding_status == "indexed",
            PaperProcessingState.text_hash.is_not(None),
            PaperProcessingState.qdrant_indexed_at.is_not(None),
        )
        return {
            int(paper_id): str(text_hash)
            for paper_id, text_hash in self.session.execute(stmt)
            if text_hash
        }

    def mark_loaded(self, paper_ids: Iterable[int]) -> None:
        """Mark successfully loaded papers as waiting for embedding/indexing."""
        unique_ids = self._unique_ids(paper_ids)
        if not unique_ids:
            return
        now = self._now()
        values = [
            {
                "paper_id": paper_id,
                "text_hash": None,
                "embedding_status": "pending",
                "embedding_error": None,
                "qdrant_indexed_at": None,
                "last_processed_at": now,
                "updated_at": now,
            }
            for paper_id in unique_ids
        ]
        self._upsert_values(
            values,
            update_fields={
                "text_hash": None,
                "embedding_status": "pending",
                "embedding_error": None,
                "qdrant_indexed_at": None,
                "last_processed_at": now,
                "updated_at": now,
            },
        )

    def mark_indexing_started(self, paper_ids: Iterable[int]) -> None:
        """Mark papers as actively being embedded or indexed."""
        unique_ids = self._unique_ids(paper_ids)
        if not unique_ids:
            return
        now = self._now()
        values = [
            {
                "paper_id": paper_id,
                "embedding_status": "processing",
                "embedding_error": None,
                "last_processed_at": now,
                "updated_at": now,
            }
            for paper_id in unique_ids
        ]
        self._upsert_values(
            values,
            update_fields={
                "embedding_status": "processing",
                "embedding_error": None,
                "last_processed_at": now,
                "updated_at": now,
            },
        )

    def mark_indexed(self, text_hashes_by_paper_id: dict[int, str]) -> None:
        """Mark papers as successfully indexed in Qdrant."""
        if not text_hashes_by_paper_id:
            return
        now = self._now()
        values = [
            {
                "paper_id": int(paper_id),
                "text_hash": text_hash,
                "embedding_status": "indexed",
                "embedding_error": None,
                "qdrant_indexed_at": now,
                "last_processed_at": now,
                "updated_at": now,
            }
            for paper_id, text_hash in text_hashes_by_paper_id.items()
        ]
        self._upsert_values(
            values,
            update_fields={
                "text_hash": None,
                "embedding_status": "indexed",
                "embedding_error": None,
                "qdrant_indexed_at": now,
                "last_processed_at": now,
                "updated_at": now,
            },
            use_excluded_text_hash=True,
        )

    def mark_failed(
        self,
        paper_ids: Iterable[int],
        error_message: str,
    ) -> None:
        """Mark papers whose embedding or Qdrant indexing failed."""
        unique_ids = self._unique_ids(paper_ids)
        if not unique_ids:
            return
        now = self._now()
        error = error_message[:4000]
        values = [
            {
                "paper_id": paper_id,
                "embedding_status": "failed",
                "embedding_error": error,
                "last_processed_at": now,
                "updated_at": now,
            }
            for paper_id in unique_ids
        ]
        self._upsert_values(
            values,
            update_fields={
                "embedding_status": "failed",
                "embedding_error": error,
                "last_processed_at": now,
                "updated_at": now,
            },
        )

    def _upsert_values(
        self,
        values: list[dict[str, object]],
        *,
        update_fields: dict[str, object],
        use_excluded_text_hash: bool = False,
    ) -> None:
        if self._is_postgresql():
            stmt = pg_insert(PaperProcessingState).values(values)
            update_values = dict(update_fields)
            if use_excluded_text_hash:
                update_values["text_hash"] = stmt.excluded.text_hash
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[PaperProcessingState.paper_id],
                    set_=update_values,
                )
            )
            return

        for value in values:
            paper_id = int(value["paper_id"])
            state = self.session.get(PaperProcessingState, paper_id)
            if state is None:
                create_values = dict(value)
                create_values.setdefault(
                    "created_at",
                    create_values.get("updated_at") or self._now(),
                )
                state = PaperProcessingState(**create_values)
                self.session.add(state)
                continue
            for field, field_value in value.items():
                if field == "paper_id":
                    continue
                setattr(state, field, field_value)

    def _unique_ids(self, paper_ids: Iterable[int]) -> list[int]:
        return list(dict.fromkeys(int(paper_id) for paper_id in paper_ids))

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)


__all__ = ["PaperProcessingStateRepository"]

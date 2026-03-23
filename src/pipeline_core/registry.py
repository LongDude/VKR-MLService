from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Type

from pipeline_core.contracts import StageName
from pipeline_handlers.base import BaseHandler


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[StageName, dict[str, Type[BaseHandler]]] = defaultdict(dict)

    def register(self, handler_cls: Type[BaseHandler]) -> None:
        if not issubclass(handler_cls, BaseHandler):
            raise TypeError("handler_cls must inherit from BaseHandler")
        handler_id = getattr(handler_cls, "handler_id", "")
        stage = getattr(handler_cls, "stage", None)
        if not handler_id:
            raise ValueError(f"Handler {handler_cls.__name__} must define handler_id")
        if not isinstance(stage, StageName):
            raise ValueError(f"Handler {handler_cls.__name__} must define valid stage")
        self._handlers[stage][handler_id] = handler_cls

    def register_many(self, classes: Iterable[Type[BaseHandler]]) -> None:
        for handler_cls in classes:
            self.register(handler_cls)

    def resolve_chain(
        self,
        stage: StageName,
        enabled_handlers: Optional[List[str]] = None,
        handler_configs: Optional[Dict[str, Dict]] = None,
    ) -> List[BaseHandler]:
        stage_map = self._handlers.get(stage, {})
        if not stage_map:
            return []

        handler_configs = handler_configs or {}
        if enabled_handlers is not None:
            missing = [handler_id for handler_id in enabled_handlers if handler_id not in stage_map]
            if missing:
                raise ValueError(f"Unknown handlers for stage {stage.value}: {', '.join(missing)}")
            selected = [stage_map[handler_id] for handler_id in enabled_handlers]
        else:
            selected = list(stage_map.values())

        instances = [handler_cls(config=handler_configs.get(handler_cls.handler_id, {})) for handler_cls in selected]
        instances.sort(key=lambda item: item.priority)
        return instances

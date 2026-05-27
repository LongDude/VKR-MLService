from __future__ import annotations

from dto.recommendations import UserProfileDTO
from ml.facades.user_profile import UserProfileFacade


class UserProfilePipeline:
    def __init__(self, facade: UserProfileFacade) -> None:
        self.facade = facade

    def recompute_user(
        self,
        user_id: int,
    ) -> UserProfileDTO:
        return self.facade.recompute_user_profile(user_id)


__all__ = ["UserProfilePipeline"]

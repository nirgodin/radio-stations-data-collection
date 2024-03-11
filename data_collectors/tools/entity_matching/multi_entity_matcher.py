from typing import Any, List, Optional, Callable

from genie_common.tools import logger
from spotipyio import EntityMatcher, MatchingEntity


class MultiEntityMatcher:
    def __init__(self, entity_matcher: EntityMatcher):
        self._entity_matcher = entity_matcher

    def match(self,
              entity: MatchingEntity,
              prioritized_candidates: List[Any],
              extract_fn: Callable[[Any], Optional[Any]]) -> Optional[Any]:
        for candidate in prioritized_candidates:
            is_matching, _ = self._entity_matcher.match(entity, candidate)

            if is_matching:
                return extract_fn(candidate)

        logger.info("Failed to match any of the provided candidates. Returning None")

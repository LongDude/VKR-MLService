from __future__ import annotations

from collections.abc import Iterable
from math import sqrt
from numbers import Real

from core.exceptions import InvalidRequestError


class VectorMathService:
    def cosine_similarity(
        self,
        a: list[float],
        b: list[float],
    ) -> float:
        vector_a = self._validate_vector(a, "a")
        vector_b = self._validate_vector(b, "b")
        self._validate_same_dimension([vector_a, vector_b])

        dot_product = sum(left * right for left, right in zip(vector_a, vector_b))
        norm_a = sqrt(sum(value * value for value in vector_a))
        norm_b = sqrt(sum(value * value for value in vector_b))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return self._clamp(dot_product / (norm_a * norm_b), -1.0, 1.0)

    def mean_vector(
        self,
        vectors: list[list[float]],
    ) -> list[float]:
        if not vectors:
            raise InvalidRequestError("Cannot calculate mean vector for an empty list")
        validated_vectors = [
            self._validate_vector(vector, f"vectors[{index}]")
            for index, vector in enumerate(vectors)
        ]
        self._validate_same_dimension(validated_vectors)

        dimension = len(validated_vectors[0])
        return [
            sum(vector[index] for vector in validated_vectors) / len(validated_vectors)
            for index in range(dimension)
        ]

    def streaming_mean_vector(
        self,
        vectors: Iterable[list[float]],
    ) -> list[float]:
        mean_vector: list[float] | None = None
        vector_count = 0
        for vector in vectors:
            mean_vector, vector_count = self.update_mean_vector(
                mean_vector,
                vector_count,
                vector,
            )
        if mean_vector is None:
            raise InvalidRequestError("Cannot calculate mean vector for an empty list")
        return mean_vector

    def update_mean_vector(
        self,
        current_mean: list[float] | None,
        current_count: int,
        vector: list[float],
    ) -> tuple[list[float], int]:
        if current_count < 0:
            raise InvalidRequestError(
                "current_count must be non-negative",
                details={"current_count": current_count},
            )

        validated_vector = self._validate_vector(vector, "vector")
        if current_mean is None:
            if current_count != 0:
                raise InvalidRequestError(
                    "current_count must be zero when current_mean is empty",
                    details={"current_count": current_count},
                )
            return validated_vector, 1

        if current_count == 0:
            raise InvalidRequestError(
                "current_count must be positive when current_mean is provided",
                details={"current_count": current_count},
            )

        validated_mean = self._validate_vector(current_mean, "current_mean")
        self._validate_same_dimension([validated_mean, validated_vector])
        next_count = current_count + 1
        return [
            mean_value + (vector_value - mean_value) / next_count
            for mean_value, vector_value in zip(validated_mean, validated_vector)
        ], next_count

    def weighted_mean_vector(
        self,
        weighted_vectors: list[tuple[list[float], float]],
    ) -> list[float]:
        if not weighted_vectors:
            raise InvalidRequestError(
                "Cannot calculate weighted mean vector for an empty list"
            )

        vectors: list[list[float]] = []
        weights: list[float] = []
        for index, (vector, weight) in enumerate(weighted_vectors):
            if not isinstance(weight, Real) or isinstance(weight, bool):
                raise InvalidRequestError(
                    "Vector weight must be numeric",
                    details={"index": index, "weight": weight},
                )
            vectors.append(
                self._validate_vector(vector, f"weighted_vectors[{index}][0]")
            )
            weights.append(float(weight))

        self._validate_same_dimension(vectors)
        total_weight = sum(weights)
        if total_weight <= 0.0:
            raise InvalidRequestError(
                "Total vector weight must be positive",
                details={"total_weight": total_weight},
            )

        dimension = len(vectors[0])
        return [
            sum(vector[index] * weight for vector, weight in zip(vectors, weights))
            / total_weight
            for index in range(dimension)
        ]

    def normalize_score(
        self,
        value: float,
        min_value: float,
        max_value: float,
    ) -> float:
        if max_value < min_value:
            raise InvalidRequestError(
                "max_value must be greater than or equal to min_value",
                details={"min_value": min_value, "max_value": max_value},
            )
        if max_value == min_value:
            return 0.0
        return self._clamp((value - min_value) / (max_value - min_value), 0.0, 1.0)

    def semantic_drift(
        self,
        current_vector: list[float],
        previous_vector: list[float],
    ) -> float:
        return 1.0 - self.cosine_similarity(current_vector, previous_vector)

    def _validate_vector(self, vector: list[float], name: str) -> list[float]:
        if not vector:
            raise InvalidRequestError(
                "Vector must not be empty",
                details={"name": name},
            )
        if not all(
            isinstance(value, Real) and not isinstance(value, bool) for value in vector
        ):
            raise InvalidRequestError(
                "Vector must contain only numeric values",
                details={"name": name},
            )
        return [float(value) for value in vector]

    def _validate_same_dimension(self, vectors: list[list[float]]) -> None:
        dimensions = {len(vector) for vector in vectors}
        if len(dimensions) > 1:
            raise InvalidRequestError(
                "Vector dimensions must match",
                details={"dimensions": sorted(dimensions)},
            )

    def _clamp(self, value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, value))


__all__ = ["VectorMathService"]

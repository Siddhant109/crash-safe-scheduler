def calculate_backoff(
    attempt_number: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> float:
    if attempt_number < 1:
        raise ValueError(
            "attempt_number must be at least 1"
        )

    delay = base_delay * (2 ** (attempt_number - 1))

    return min(delay, max_delay)
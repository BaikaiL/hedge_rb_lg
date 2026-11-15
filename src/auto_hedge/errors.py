def failure_reason(exc: BaseException) -> str:
    """Report application errors without echoing SDK payloads or credentials."""
    if isinstance(exc, BaseExceptionGroup):
        return "; ".join(failure_reason(e) for e in exc.exceptions)
    if type(exc) in (RuntimeError, ValueError, KeyError):
        return str(exc)
    return type(exc).__name__

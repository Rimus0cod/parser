from __future__ import annotations

from typing import Any


class MemoryRedis:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def set(  # noqa: ARG002
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self._data:
            return False
        self._data[key] = value
        return True

    def delete(self, key: str) -> int:
        if key not in self._data:
            return 0
        del self._data[key]
        return 1


class AsyncCursorStub:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.rows: list[tuple[Any, ...]] = []
        self.row: tuple[Any, ...] | None = None

    async def __aenter__(self) -> "AsyncCursorStub":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:  # noqa: ANN401
        return None

    async def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.executed.append((sql, params))

    async def executemany(self, sql: str, params: list[tuple[Any, ...]]) -> None:
        self.executed.append((sql, tuple(params)))

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class AsyncConnectionStub:
    def __init__(self, cursor: AsyncCursorStub) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> "AsyncConnectionStub":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:  # noqa: ANN401
        return None

    def cursor(self) -> AsyncCursorStub:
        return self._cursor


class AsyncPoolStub:
    def __init__(self, cursor: AsyncCursorStub) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> "AsyncPoolStub":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:  # noqa: ANN401
        return None

    def acquire(self) -> AsyncConnectionStub:
        return AsyncConnectionStub(self._cursor)

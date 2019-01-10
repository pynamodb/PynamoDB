from typing import Any, Callable, Dict, Iterator, Text, TypeVar, Optional


_T = TypeVar('_T')


class RateLimiter(object):
    _rate_limit: float
    _consumed: float
    _time_of_last_acquire: float
    _time_module: Any
    def __init__(self, rate_limit: float, time_module: Any = ...) -> None: ...
    def consume(self, units: int) -> None: ...
    def acquire(self) -> None: ...
    @property
    def rate_limit(self) -> float: ...
    @property.setter
    def rate_limit(self, rate_limit: float) -> None: ...


class PageIterator(Iterator[_T]):
    _operation: Callable
    _args: Any
    _kwargs: Dict[Text, Any]
    _first_iteration: bool
    _total_scanned_count: int
    _rate_limiter: Optional[RateLimiter]
    def __init__(
        self,
        operation: Callable,
        args: Any,
        kwargs: Dict[Text, Any],
        rate_limit: Optional[float] = ...,
    ) -> None: ...
    def __iter__(self) -> Iterator[_T]: ...
    def __next__(self) -> _T: ...
    @property
    def total_scanned_count(self) -> int: ...


class ResultIterator(Iterator[_T]):
    page_iter: PageIterator
    _first_iteration: bool
    _map_fn: Optional[Callable]
    _limit: Optional[int]
    _total_count: int
    def __init__(
        self,
        operation: Callable,
        args: Any,
        kwargs: Dict[Text, Any],
        map_fn: Optional[Callable] = ...,
        limit: Optional[int] = ...,
        rate_limit: Optional[float] = ...,
    ) -> None: ...
    def __iter__(self) -> Iterator[_T]: ...
    def __next__(self) -> _T: ...
    def next(self) -> _T: ...
    @property
    def total_count(self) -> int: ...

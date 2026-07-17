"""Small distributional-arithmetic prototype.

The package models random variables as lazy expression graphs. Reusing the same
source preserves dependence during particle evaluation, while ``iid()`` creates
an independent source with the same marginal distribution.
"""

from dataclasses import dataclass
import math
import operator
import random
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID, uuid4

Number = Union[int, float]


class Distribution:
    """Base class for scalar marginal distributions."""

    def sample(self, size: int, rng: random.Random) -> List[float]:
        raise NotImplementedError


@dataclass(frozen=True)
class NormalDistribution(Distribution):
    mu: float
    sigma: float

    def __post_init__(self) -> None:
        if self.sigma < 0:
            raise ValueError("sigma must be non-negative")

    def sample(self, size: int, rng: random.Random) -> List[float]:
        if self.sigma == 0:
            return [self.mu] * size
        return [rng.gauss(self.mu, self.sigma) for _ in range(size)]


@dataclass(frozen=True)
class LogNormalDistribution(Distribution):
    mu: float
    sigma: float

    def __post_init__(self) -> None:
        if self.sigma < 0:
            raise ValueError("sigma must be non-negative")

    def sample(self, size: int, rng: random.Random) -> List[float]:
        if self.sigma == 0:
            value = math.exp(self.mu)
            return [value] * size
        return [rng.lognormvariate(self.mu, self.sigma) for _ in range(size)]


@dataclass(frozen=True)
class EmpiricalDistribution(Distribution):
    samples: Tuple[float, ...]

    def __init__(self, samples: Iterable[Number]):
        values = tuple(float(sample) for sample in samples)
        if not values:
            raise ValueError("Empirical requires at least one sample")
        object.__setattr__(self, "samples", values)

    def sample(self, size: int, rng: random.Random) -> List[float]:
        return [rng.choice(self.samples) for _ in range(size)]


class Expr:
    pass


@dataclass(frozen=True)
class Constant(Expr):
    value: float


@dataclass(frozen=True)
class Source(Expr):
    distribution: Distribution
    source_id: UUID
    name: Optional[str] = None


@dataclass(frozen=True)
class UnaryExpr(Expr):
    function: Callable[[float], float]
    operand: Expr
    name: str


@dataclass(frozen=True)
class BinaryExpr(Expr):
    function: Callable[[float, float], float]
    left: Expr
    right: Expr
    name: str


def _as_expr(value: Union[Number, "RandomVariable"]) -> Expr:
    if isinstance(value, RandomVariable):
        return value.expr
    return Constant(float(value))


def _apply_binary(
    function: Callable[[float, float], float], left: List[float], right: List[float]
) -> List[float]:
    return [function(a, b) for a, b in zip(left, right)]


@dataclass(frozen=True)
class EvaluationResult:
    samples: Tuple[float, ...]
    method: str = "particles"

    def mean(self) -> float:
        return sum(self.samples) / len(self.samples)

    def variance(self) -> float:
        mean = self.mean()
        return sum((sample - mean) ** 2 for sample in self.samples) / len(self.samples)

    def std(self) -> float:
        return math.sqrt(self.variance())

    def quantile(self, q: Union[float, Iterable[float]]) -> Union[float, List[float]]:
        if isinstance(q, (list, tuple)):
            return [_quantile(self.samples, float(probability)) for probability in q]
        return _quantile(self.samples, float(q))


@dataclass(frozen=True)
class RandomVariable:
    expr: Expr

    def __add__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return RandomVariable(
            BinaryExpr(operator.add, self.expr, _as_expr(other), "add")
        )

    def __radd__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return self + other

    def __sub__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return RandomVariable(
            BinaryExpr(operator.sub, self.expr, _as_expr(other), "subtract")
        )

    def __rsub__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return RandomVariable(
            BinaryExpr(operator.sub, _as_expr(other), self.expr, "subtract")
        )

    def __mul__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return RandomVariable(
            BinaryExpr(operator.mul, self.expr, _as_expr(other), "multiply")
        )

    def __rmul__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return self * other

    def __truediv__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return RandomVariable(
            BinaryExpr(operator.truediv, self.expr, _as_expr(other), "divide")
        )

    def __rtruediv__(self, other: Union[Number, "RandomVariable"]) -> "RandomVariable":
        return RandomVariable(
            BinaryExpr(operator.truediv, _as_expr(other), self.expr, "divide")
        )

    def __neg__(self) -> "RandomVariable":
        return RandomVariable(UnaryExpr(operator.neg, self.expr, "negative"))

    def __lt__(self, other: Union[Number, "RandomVariable"]) -> "Event":
        return Event(operator.lt, self.expr, _as_expr(other), "lt")

    def __le__(self, other: Union[Number, "RandomVariable"]) -> "Event":
        return Event(operator.le, self.expr, _as_expr(other), "le")

    def __gt__(self, other: Union[Number, "RandomVariable"]) -> "Event":
        return Event(operator.gt, self.expr, _as_expr(other), "gt")

    def __ge__(self, other: Union[Number, "RandomVariable"]) -> "Event":
        return Event(operator.ge, self.expr, _as_expr(other), "ge")

    def sample(self, size: int, seed: Optional[int] = None) -> List[float]:
        return list(self.evaluate(size=size, seed=seed).samples)

    def evaluate(
        self, *, method: str = "auto", size: int = 10000, seed: Optional[int] = None
    ) -> EvaluationResult:
        if method not in ("auto", "particles"):
            raise NotImplementedError("only particle evaluation is implemented")
        rng = random.Random(seed)
        return EvaluationResult(tuple(_evaluate_samples(self.expr, size, rng, {})))

    def mean(self, *, size: int = 10000, seed: Optional[int] = 0) -> float:
        return self.evaluate(size=size, seed=seed).mean()

    def variance(self, *, size: int = 10000, seed: Optional[int] = 0) -> float:
        return self.evaluate(size=size, seed=seed).variance()

    def std(self, *, size: int = 10000, seed: Optional[int] = 0) -> float:
        return self.evaluate(size=size, seed=seed).std()

    def quantile(
        self,
        q: Union[float, Iterable[float]],
        *,
        size: int = 10000,
        seed: Optional[int] = 0,
    ) -> Union[float, List[float]]:
        return self.evaluate(size=size, seed=seed).quantile(q)

    def iid(self) -> "RandomVariable":
        if not isinstance(self.expr, Source):
            raise TypeError("iid() currently supports source variables only")
        return RandomVariable(Source(self.expr.distribution, uuid4(), self.expr.name))

    independent_copy = iid


@dataclass(frozen=True)
class Event:
    function: Callable[[float, float], bool]
    left: Expr
    right: Expr
    name: str

    def probability(self, *, size: int = 10000, seed: Optional[int] = 0) -> float:
        rng = random.Random(seed)
        source_samples: Dict[UUID, List[float]] = {}
        left = _evaluate_samples(self.left, size, rng, source_samples)
        right = _evaluate_samples(self.right, size, rng, source_samples)
        return sum(1 for a, b in zip(left, right) if self.function(a, b)) / size

    def __and__(self, other: "Event") -> "CompoundEvent":
        return CompoundEvent(operator.and_, self, other)

    def __or__(self, other: "Event") -> "CompoundEvent":
        return CompoundEvent(operator.or_, self, other)


@dataclass(frozen=True)
class CompoundEvent:
    function: Callable[[bool, bool], bool]
    left: Union[Event, "CompoundEvent"]
    right: Union[Event, "CompoundEvent"]

    def probability(self, *, size: int = 10000, seed: Optional[int] = 0) -> float:
        rng = random.Random(seed)
        source_samples: Dict[UUID, List[float]] = {}
        outcomes = _evaluate_event(self, size, rng, source_samples)
        return sum(outcomes) / size


def _evaluate_event(
    event: Union[Event, CompoundEvent],
    size: int,
    rng: random.Random,
    source_samples: Dict[UUID, List[float]],
) -> List[bool]:
    if isinstance(event, Event):
        left = _evaluate_samples(event.left, size, rng, source_samples)
        right = _evaluate_samples(event.right, size, rng, source_samples)
        return [event.function(a, b) for a, b in zip(left, right)]
    left = _evaluate_event(event.left, size, rng, source_samples)
    right = _evaluate_event(event.right, size, rng, source_samples)
    return [event.function(a, b) for a, b in zip(left, right)]


def _evaluate_samples(
    expr: Expr, size: int, rng: random.Random, source_samples: Dict[UUID, List[float]]
) -> List[float]:
    if size <= 0:
        raise ValueError("size must be positive")
    if isinstance(expr, Constant):
        return [expr.value] * size
    if isinstance(expr, Source):
        if expr.source_id not in source_samples:
            source_samples[expr.source_id] = expr.distribution.sample(size, rng)
        return source_samples[expr.source_id]
    if isinstance(expr, UnaryExpr):
        values = _evaluate_samples(expr.operand, size, rng, source_samples)
        return [expr.function(value) for value in values]
    if isinstance(expr, BinaryExpr):
        left = _evaluate_samples(expr.left, size, rng, source_samples)
        right = _evaluate_samples(expr.right, size, rng, source_samples)
        return _apply_binary(expr.function, left, right)
    raise TypeError(f"Unsupported expression: {type(expr)!r}")


def _quantile(samples: Iterable[float], q: float) -> float:
    if not 0 <= q <= 1:
        raise ValueError("q must be between 0 and 1")
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    position = q * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def Normal(mu: Number, sigma: Number, name: Optional[str] = None) -> RandomVariable:
    return RandomVariable(
        Source(NormalDistribution(float(mu), float(sigma)), uuid4(), name)
    )


def LogNormal(mu: Number, sigma: Number, name: Optional[str] = None) -> RandomVariable:
    return RandomVariable(
        Source(LogNormalDistribution(float(mu), float(sigma)), uuid4(), name)
    )


def Empirical(samples: Iterable[Number], name: Optional[str] = None) -> RandomVariable:
    return RandomVariable(Source(EmpiricalDistribution(samples), uuid4(), name))


def P(
    event: Union[Event, CompoundEvent], size: int = 10000, seed: Optional[int] = 0
) -> float:
    return event.probability(size=size, seed=seed)


def exp_(value: Union[Number, RandomVariable]) -> RandomVariable:
    return RandomVariable(UnaryExpr(math.exp, _as_expr(value), "exp"))


def log_(value: Union[Number, RandomVariable]) -> RandomVariable:
    return RandomVariable(UnaryExpr(math.log, _as_expr(value), "log"))


exp = exp_
log = log_

__all__ = [
    "Distribution",
    "NormalDistribution",
    "LogNormalDistribution",
    "EmpiricalDistribution",
    "RandomVariable",
    "EvaluationResult",
    "Event",
    "CompoundEvent",
    "Normal",
    "LogNormal",
    "Empirical",
    "P",
    "exp",
    "log",
]

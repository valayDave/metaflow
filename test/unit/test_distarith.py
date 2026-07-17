import math

from distarith import Empirical, LogNormal, Normal, P, StudentT


def test_reusing_source_preserves_identity() -> None:
    x = Normal(0, 1)

    samples = (x - x).sample(1000, seed=1)

    assert samples == [0.0] * 1000


def test_iid_creates_independent_source() -> None:
    x = Normal(0, 1)
    y = x.iid()

    variance = (x - y).variance(size=20000, seed=1)

    assert 1.85 < variance < 2.15


def test_profit_event_probability_uses_joint_samples() -> None:
    revenue = LogNormal(mu=5.0, sigma=0.4)
    cost = Normal(mu=120, sigma=15)
    profit = revenue - cost

    probability = P(profit < 0, size=20000, seed=2)

    assert 0.15 < probability < 0.4


def test_empirical_quantile_and_compound_event() -> None:
    returns = Empirical([0.0, 1.0, 2.0, 3.0])
    net = returns * 2 - 1

    assert math.isclose(net.quantile(0.5, size=4000, seed=3), 3.0, abs_tol=0.2)
    assert 0.4 < P((net > 0) & (net < 5), size=4000, seed=3) < 0.6


def test_student_t_supports_fat_tail_study_with_normal_sum() -> None:
    normal_noise = Normal(0, 1)
    fat_tail_shock = StudentT(df=3, loc=0, scale=1)

    normal_samples = normal_noise.sample(30000, seed=4)
    combined_samples = (normal_noise + fat_tail_shock).sample(30000, seed=4)

    normal_tail_probability = sum(abs(sample) > 4 for sample in normal_samples) / len(
        normal_samples
    )
    combined_tail_probability = sum(
        abs(sample) > 4 for sample in combined_samples
    ) / len(combined_samples)

    assert combined_tail_probability > normal_tail_probability * 5

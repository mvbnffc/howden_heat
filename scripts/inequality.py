"""
Inequality metrics: concentration index, concentration curve, quantile ratio.

Adapted from flood-risk-inequality (github.com/mvbnffc/flood-risk-inequality).
All functions operate on flat numpy arrays of (population, wealth, risk) values
with NaNs and zero-population cells already removed by the caller.
"""

import numpy as np
import pandas as pd


def calculate_CI(pop: np.ndarray, wealth: np.ndarray, risk: np.ndarray) -> float:
    """
    Calculate the concentration index (CI) of risk with respect to wealth.

    A negative CI indicates risk is concentrated among the poor;
    a positive CI indicates risk is concentrated among the wealthy.

    Parameters
    ----------
    pop : np.ndarray
        Population counts per grid cell.
    wealth : np.ndarray
        Wealth indicator (e.g. RWI) per grid cell.
    risk : np.ndarray
        Risk/exposure value per grid cell (e.g. productivity loss).

    Returns
    -------
    float
        Concentration index, or np.nan if calculation is not possible.
    """
    df = pd.DataFrame({"pop": pop, "wealth": wealth, "risk": risk})
    df = df.sort_values("wealth", ascending=True)

    total_pop = df["pop"].sum()
    if total_pop == 0:
        return np.nan

    df["cum_pop"] = df["pop"].cumsum()
    df["rank"] = (df["cum_pop"] - 0.5 * df["pop"]) / total_pop

    weighted_mean_risk = np.average(df["risk"], weights=df["pop"])
    if weighted_mean_risk == 0:
        return np.nan

    sum_xR = (df["risk"] * df["rank"] * df["pop"]).sum()
    CI = (2 * sum_xR) / (total_pop * weighted_mean_risk) - 1

    return float(CI)


def calculate_concentration_curve(
    pop: np.ndarray,
    wealth: np.ndarray,
    risk: np.ndarray,
    n_points: int = 100,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Calculate the concentration curve.

    Returns two arrays:
    - cumulative population share (x-axis), from 0 to 1 (poorest to wealthiest)
    - cumulative risk share (y-axis), from 0 to 1

    Parameters
    ----------
    pop, wealth, risk : np.ndarray
        As in calculate_CI.
    n_points : int
        Number of interpolated points on the curve.

    Returns
    -------
    cum_pop_share : np.ndarray
    cum_risk_share : np.ndarray
    """
    df = pd.DataFrame({"pop": pop, "wealth": wealth, "risk": risk})
    df = df.sort_values("wealth", ascending=True)

    total_pop = df["pop"].sum()
    total_risk = (df["pop"] * df["risk"]).sum()

    if total_pop == 0 or total_risk == 0:
        return np.array([0.0, 1.0]), np.array([0.0, 1.0])

    df["cum_pop_share"] = df["pop"].cumsum() / total_pop
    df["cum_risk_share"] = (df["pop"] * df["risk"]).cumsum() / total_risk

    # Prepend origin
    cum_pop = np.concatenate([[0.0], df["cum_pop_share"].values])
    cum_risk = np.concatenate([[0.0], df["cum_risk_share"].values])

    # Interpolate to n_points for consistent plotting
    x_interp = np.linspace(0, 1, n_points)
    y_interp = np.interp(x_interp, cum_pop, cum_risk)

    return x_interp, y_interp


def prepare_arrays(
    pop: np.ndarray,
    wealth: np.ndarray,
    risk: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray] | None:
    """
    Flatten and clean arrays: remove NaNs and zero-population cells.

    Returns None if fewer than 10 valid cells remain.
    """
    mask = (
        ~np.isnan(pop) &
        ~np.isnan(wealth) &
        ~np.isnan(risk) &
        (pop > 0)
    )
    if mask.sum() < 10:
        return None

    return pop[mask], wealth[mask], risk[mask]

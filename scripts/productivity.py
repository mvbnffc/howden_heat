"""
Exposure-response function: WBGT -> labour productivity loss.

The ERF maps daily WBGT (°C) to a productivity fraction [0, 1].
Values below the lower threshold retain full productivity (1.0).
Values above the upper threshold have zero productivity (0.0).
Intermediate values are linearly interpolated from the lookup table.
"""

import numpy as np


# ---------------------------------------------------------------------------
# ERF lookup table — fill in the values below
# Format: wbgt_thresholds[i] maps to productivity_values[i]
# Example shape: [23, 24, 25, ..., 39] -> [1.0, 0.9, 0.8, ..., 0.0]
# ---------------------------------------------------------------------------

WBGT_THRESHOLDS = [26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39] # list of WBGT breakpoints (°C)
PRODUCTIVITY_VALUES = [1.0, 0.923, 0.846, 0.769, 0.692, 0.615, 0.539, 0.462, 0.385, 0.308, 0.231, 0.154, 0.077, 0.0]   # corresponding productivity fractions


def wbgt_to_productivity(wbgt_array: np.ndarray) -> np.ndarray:
    """
    Apply the exposure-response function to a WBGT array.

    Parameters
    ----------
    wbgt_array : np.ndarray
        Array of WBGT values in degrees Celsius. NaN cells are preserved.

    Returns
    -------
    np.ndarray
        Array of productivity fractions in [0, 1], same shape as input.
    """
    if WBGT_THRESHOLDS is None or PRODUCTIVITY_VALUES is None:
        raise ValueError(
            "ERF not defined. Set WBGT_THRESHOLDS and PRODUCTIVITY_VALUES in scripts/productivity.py."
        )

    thresholds = np.array(WBGT_THRESHOLDS, dtype=float)
    productivity = np.array(PRODUCTIVITY_VALUES, dtype=float)

    # Linear interpolation; clamp outside the defined range
    result = np.interp(wbgt_array, thresholds, productivity,
                       left=productivity[0], right=productivity[-1])

    # Preserve NaNs
    result = np.where(np.isnan(wbgt_array), np.nan, result)

    return result


def wbgt_to_productivity_loss(wbgt_array: np.ndarray) -> np.ndarray:
    """
    Return productivity *loss* (1 - productivity fraction).

    Parameters
    ----------
    wbgt_array : np.ndarray
        Array of WBGT values in degrees Celsius.

    Returns
    -------
    np.ndarray
        Array of productivity loss fractions in [0, 1].
    """
    return 1.0 - wbgt_to_productivity(wbgt_array)

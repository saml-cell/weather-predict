#!/usr/bin/env python3
"""
Meteorological Physics Module — Layer 2 of the prediction system.

Contains standard meteorological formulas used by meteorologists to assess
atmospheric conditions. These provide bounded corrections to the statistical
ensemble (Layer 1).

Formulas:
  - Dew Point (Magnus-Tetens)
  - Dew Point Depression → precipitation likelihood
  - Pressure Tendency → weather change prediction (Buys-Ballot)
  - Heat Index (Rothfusz/NWS regression)
  - Wind Chill (NWS formula)
  - Clausius-Clapeyron precipitation intensity scaling
  - Combined physics correction function
"""

import math


# ---------------------------------------------------------------------------
# 0. Sea-Level Pressure Correction (Hypsometric Equation)
# ---------------------------------------------------------------------------
def sea_level_pressure(station_pressure_hpa: float, elevation_m: float, temp_c: float = 15.0) -> float:
    """Reduce station pressure to sea level using hypsometric equation."""
    return station_pressure_hpa * math.exp(elevation_m / (29.3 * (temp_c + 273.15)))


# ---------------------------------------------------------------------------
# 1. Dew Point — Magnus-Tetens Formula
# ---------------------------------------------------------------------------
def dew_point(temp_c, humidity_pct):
    """Calculate dew point temperature using Magnus-Tetens approximation.

    Accurate for -45C to 60C range.
    The dew point is the temperature at which air becomes saturated and
    condensation (fog, dew, rain) begins to form.

    Args:
        temp_c: Air temperature in Celsius
        humidity_pct: Relative humidity (0-100)

    Returns:
        Dew point temperature in Celsius
    """
    if humidity_pct is None or temp_c is None:
        return None
    if humidity_pct <= 0:
        return None

    a = 17.625
    b = 243.04
    rh = max(humidity_pct, 0.1) / 100.0  # clamp to avoid log(0)
    alpha = math.log(rh) + (a * temp_c) / (b + temp_c)
    return round(b * alpha / (a - alpha), 2)


def dew_point_depression(temp_c, humidity_pct):
    """Calculate dew point depression (T - Td).

    Small depression (<2.5C) = high moisture, condensation likely.
    Large depression (>15C) = very dry air.
    """
    td = dew_point(temp_c, humidity_pct)
    if td is None:
        return None
    return round(temp_c - td, 2)


# ---------------------------------------------------------------------------
# 2. Dew Point Depression → Precipitation Adjustment
# ---------------------------------------------------------------------------
def precip_adjustment_from_dewpoint(temp_c, humidity_pct):
    """Adjust precipitation probability based on dew point depression.

    Meteorological principle: When the air temperature and dew point are
    very close (small depression), the air is nearly saturated and
    precipitation is much more likely.

    Returns:
        Adjustment in percentage points (-5 to +15)
    """
    dpd = dew_point_depression(temp_c, humidity_pct)
    if dpd is None:
        return 0

    if dpd < 1.5:
        return 15    # Fog/rain almost certain — air is nearly saturated
    elif dpd < 2.5:
        return 10    # High condensation risk
    elif dpd < 4.0:
        return 5     # Moderate moisture
    elif dpd > 15.0:
        return -5    # Very dry air — precipitation less likely
    else:
        return 0     # No adjustment


# ---------------------------------------------------------------------------
# 3. Pressure Tendency → Weather Change (Buys-Ballot Principle)
# ---------------------------------------------------------------------------
def precip_adjustment_from_pressure(pressure_hpa, trend_hpa_per_3h):
    """Adjust precipitation probability based on barometric pressure trend.

    Buys-Ballot's Law: Falling pressure indicates approaching low-pressure
    system (storms, rain). Rising pressure indicates clearing weather.

    Args:
        pressure_hpa: Current barometric pressure in hPa
        trend_hpa_per_3h: Pressure change over 3 hours (negative = falling)

    Returns:
        Adjustment in percentage points (-10 to +15)
    """
    if trend_hpa_per_3h is None:
        # No trend data — use absolute pressure as fallback
        if pressure_hpa is not None:
            if pressure_hpa < 1000:
                return 5   # Low pressure system present
            elif pressure_hpa > 1025:
                return -5  # High pressure = stable/clear
        return 0

    if trend_hpa_per_3h < -2.0:
        return 15    # Rapid drop = storm/front approaching
    elif trend_hpa_per_3h < -1.0:
        return 10    # Moderate drop
    elif trend_hpa_per_3h < -0.5:
        return 5     # Slow drop
    elif trend_hpa_per_3h > 2.0:
        return -10   # Rapid rise = clearing fast
    elif trend_hpa_per_3h > 1.0:
        return -5    # Moderate rise
    else:
        return 0


def pressure_stability_index(pressure_hpa, elevation_m=None, temp_c=15.0):
    """Classify atmospheric stability based on absolute pressure.

    If elevation_m is provided, pressure is first corrected to sea level
    using the hypsometric equation for a fair comparison against thresholds.

    Returns: 'low' (<1005), 'normal' (1005-1020), 'high' (>1020)
    """
    if pressure_hpa is None:
        return "unknown"
    if elevation_m is not None and elevation_m > 0:
        pressure_hpa = sea_level_pressure(pressure_hpa, elevation_m, temp_c)
    if pressure_hpa < 1005:
        return "low"
    elif pressure_hpa > 1020:
        return "high"
    else:
        return "normal"


# ---------------------------------------------------------------------------
# 4. Heat Index — Rothfusz Regression (NWS Standard)
# ---------------------------------------------------------------------------
def heat_index(temp_c, humidity_pct):
    """Calculate Heat Index using NWS Rothfusz regression equation.

    Only applies when T >= 27C and RH >= 40%. Below these thresholds,
    heat index equals the air temperature.

    This is the official formula used by the US National Weather Service.

    Args:
        temp_c: Air temperature in Celsius
        humidity_pct: Relative humidity (0-100)

    Returns:
        Heat index in Celsius
    """
    if temp_c is None or humidity_pct is None:
        return temp_c
    if temp_c < 27 or humidity_pct < 40:
        return temp_c

    # Convert to Fahrenheit for the regression (developed in imperial units)
    T = temp_c * 9.0 / 5.0 + 32.0
    RH = humidity_pct

    HI = (-42.379
           + 2.04901523 * T
           + 10.14333127 * RH
           - 0.22475541 * T * RH
           - 0.00683783 * T * T
           - 0.05481717 * RH * RH
           + 0.00122874 * T * T * RH
           + 0.00085282 * T * RH * RH
           - 0.00000199 * T * T * RH * RH)

    # Adjustment for low humidity at high temps
    if RH < 13 and 80 < T < 112:
        adjustment = -((13 - RH) / 4) * math.sqrt((17 - abs(T - 95)) / 17)
        HI += adjustment

    # Adjustment for high humidity at moderate temps
    if RH > 85 and 80 < T < 87:
        adjustment = ((RH - 85) / 10) * ((87 - T) / 5)
        HI += adjustment

    # Convert back to Celsius
    return round((HI - 32) * 5.0 / 9.0, 1)


# ---------------------------------------------------------------------------
# 5. Wind Chill — NWS Formula
# ---------------------------------------------------------------------------
def wind_chill(temp_c, wind_kmh):
    """Calculate Wind Chill using NWS formula.

    Only applies when T <= 10C and wind > 4.8 km/h. Outside these ranges,
    wind chill equals the air temperature.

    Args:
        temp_c: Air temperature in Celsius
        wind_kmh: Wind speed in km/h

    Returns:
        Wind chill in Celsius
    """
    if temp_c is None or wind_kmh is None:
        return temp_c
    if temp_c > 10 or wind_kmh <= 4.8:
        return temp_c

    wc = (13.12
          + 0.6215 * temp_c
          - 11.37 * (wind_kmh ** 0.16)
          + 0.3965 * temp_c * (wind_kmh ** 0.16))
    return round(wc, 1)


# ---------------------------------------------------------------------------
# 6. Clausius-Clapeyron Precipitation Intensity
# ---------------------------------------------------------------------------
def precip_intensity_factor(temp_c):
    """Scale expected precipitation intensity based on Clausius-Clapeyron relation.

    Warmer air holds approximately 7% more moisture per 1C above the baseline.
    This means rain events in warmer conditions tend to be heavier.

    Args:
        temp_c: Air temperature in Celsius

    Returns:
        Multiplier for expected precipitation amount (1.0 = baseline at 15C)
    """
    if temp_c is None:
        return 1.0
    baseline = 15.0
    return round(1.07 ** (temp_c - baseline), 3)


# ---------------------------------------------------------------------------
# 7. Feels-Like Temperature (composite)
# ---------------------------------------------------------------------------
def feels_like(temp_c, humidity_pct, wind_kmh):
    """Calculate the most appropriate 'feels like' temperature.

    Uses Heat Index when hot+humid, Wind Chill when cold+windy,
    or raw temperature when neither condition applies.
    """
    if temp_c is None:
        return None

    # Try heat index first (hot conditions)
    hi = heat_index(temp_c, humidity_pct)
    if hi != temp_c:
        return hi

    # Try wind chill (cold conditions)
    wc = wind_chill(temp_c, wind_kmh)
    if wc != temp_c:
        return wc

    # Neither applies — use raw temp
    return temp_c


# ---------------------------------------------------------------------------
# 8. Combined Physics Correction (Layer 2)
# ---------------------------------------------------------------------------
def apply_physics_corrections(forecast, pressure_hpa=None, humidity_pct=None,
                               temp_c=None, wind_kmh=None, pressure_trend=None,
                               max_precip_adj=15, max_temp_adj=2, elevation_m=None,
                               apply_cc=True):
    """Apply bounded meteorological corrections to a Layer 1 ensemble forecast.

    This is the main entry point for Layer 2. It takes the weighted ensemble
    output and applies physics-based adjustments that are:
    - Bounded (hard caps prevent wild swings)
    - Transparent (all adjustments are logged)
    - Conservative (only activate when signals are strong)

    Args:
        forecast: dict with keys like 'precip_prob', 'precip_mm', 'temp_high_c', etc.
        pressure_hpa: Average barometric pressure from sources
        humidity_pct: Average humidity from sources
        temp_c: Current temperature for feels-like calculation
        wind_kmh: Current wind speed
        pressure_trend: Pressure change in hPa per 3 hours (if available)
        max_precip_adj: Maximum precipitation probability adjustment (percentage points)
        max_temp_adj: Maximum temperature adjustment (Celsius)

    Returns:
        (adjusted_forecast, corrections_log)
        adjusted_forecast: dict with same keys as input, adjusted values
        corrections_log: list of human-readable strings explaining each adjustment
    """
    adjusted = dict(forecast)  # shallow copy
    corrections = []

    # --- Precipitation adjustments ---
    if humidity_pct is not None and temp_c is not None:
        dp_adj = precip_adjustment_from_dewpoint(temp_c, humidity_pct)
    else:
        dp_adj = 0

    pr_adj = precip_adjustment_from_pressure(pressure_hpa, pressure_trend)

    total_precip_adj = max(-max_precip_adj, min(max_precip_adj, dp_adj + pr_adj))

    if total_precip_adj != 0 and "precip_prob" in adjusted and adjusted["precip_prob"] is not None:
        old_prob = adjusted["precip_prob"]
        adjusted["precip_prob"] = max(0, min(100, old_prob + total_precip_adj))
        parts = []
        if dp_adj != 0:
            dpd = dew_point_depression(temp_c, humidity_pct)
            parts.append(f"dew point depression={dpd:.1f}C -> {dp_adj:+d}%")
        if pr_adj != 0:
            parts.append(f"pressure={'trend ' + str(pressure_trend) if pressure_trend else str(pressure_hpa) + 'hPa'} -> {pr_adj:+d}%")
        corrections.append(
            f"Precip: {old_prob:.0f}% -> {adjusted['precip_prob']:.0f}% ({', '.join(parts)})"
        )

    # --- Precipitation intensity scaling (skip for NWP sources that already model moisture physics) ---
    if apply_cc and temp_c is not None and "precip_mm" in adjusted and adjusted["precip_mm"] is not None:
        factor = precip_intensity_factor(temp_c)
        if abs(factor - 1.0) > 0.05:  # Only note if >5% adjustment (up or down)
            old_mm = adjusted["precip_mm"]
            adjusted["precip_mm"] = round(old_mm * factor, 1)
            if old_mm > 0:
                if factor >= 1.0:
                    corrections.append(
                        f"Precip amount: {old_mm}mm -> {adjusted['precip_mm']}mm "
                        f"(Clausius-Clapeyron: {temp_c}C air holds {(factor-1)*100:.0f}% more moisture)"
                    )
                else:
                    corrections.append(
                        f"Precip amount: {old_mm}mm -> {adjusted['precip_mm']}mm "
                        f"(Clausius-Clapeyron: {temp_c}C cold air holds {(1-factor)*100:.0f}% less moisture)"
                    )

    # --- Feels-like temperature ---
    if temp_c is not None:
        fl = feels_like(temp_c, humidity_pct, wind_kmh)
        adjusted["feels_like_c"] = fl
        if fl != temp_c:
            if fl > temp_c:
                corrections.append(f"Feels like: {fl}C (heat index, humidity={humidity_pct}%)")
            else:
                corrections.append(f"Feels like: {fl}C (wind chill, wind={wind_kmh}km/h)")

    # --- Pressure stability context ---
    if pressure_hpa is not None:
        stability = pressure_stability_index(pressure_hpa, elevation_m=elevation_m,
                                              temp_c=temp_c if temp_c is not None else 15.0)
        adjusted["pressure_stability"] = stability
        slp_note = ""
        if elevation_m is not None and elevation_m > 0:
            slp = sea_level_pressure(pressure_hpa, elevation_m,
                                      temp_c if temp_c is not None else 15.0)
            adjusted["sea_level_pressure_hpa"] = round(slp, 1)
            slp_note = f" (SLP: {round(slp, 1)}hPa)"
        if stability == "low":
            corrections.append(f"Pressure: {pressure_hpa}hPa{slp_note} (LOW — unsettled weather likely)")
        elif stability == "high":
            corrections.append(f"Pressure: {pressure_hpa}hPa{slp_note} (HIGH — stable/clear conditions)")

    # --- Dew point context ---
    if temp_c is not None and humidity_pct is not None:
        td = dew_point(temp_c, humidity_pct)
        dpd = dew_point_depression(temp_c, humidity_pct)
        adjusted["dew_point_c"] = td
        adjusted["dew_point_depression_c"] = dpd

    return adjusted, corrections


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== Meteorological Physics Module — Self-Test ===\n")

    # Test dew point
    td = dew_point(25, 60)
    print(f"Dew point at 25C/60%RH: {td}C (expect ~16.7C)")

    td = dew_point(10, 95)
    print(f"Dew point at 10C/95%RH: {td}C (expect ~9.3C)")

    # Test dew point depression
    dpd = dew_point_depression(10, 95)
    print(f"DPD at 10C/95%RH: {dpd}C (very small = fog likely)")

    # Test precip adjustments
    adj = precip_adjustment_from_dewpoint(10, 95)
    print(f"Precip adjustment from DPD={dpd}: {adj:+d}%")

    adj = precip_adjustment_from_pressure(None, -3.0)
    print(f"Precip adjustment from rapid pressure drop: {adj:+d}%")

    # Test heat index
    hi = heat_index(35, 70)
    print(f"\nHeat index at 35C/70%RH: {hi}C (expect ~48C — dangerous)")

    hi = heat_index(20, 50)
    print(f"Heat index at 20C/50%RH: {hi}C (expect 20C — no effect)")

    # Test wind chill
    wc = wind_chill(-5, 30)
    print(f"\nWind chill at -5C/30kmh: {wc}C (expect ~-12C)")

    wc = wind_chill(15, 20)
    print(f"Wind chill at 15C/20kmh: {wc}C (expect 15C — no effect)")

    # Test Clausius-Clapeyron
    f = precip_intensity_factor(30)
    print(f"\nPrecip intensity at 30C: {f}x (expect ~2.05x)")

    # Test feels-like
    fl = feels_like(35, 70, 10)
    print(f"\nFeels like at 35C/70%RH/10kmh wind: {fl}C (heat index)")

    fl = feels_like(-5, 50, 30)
    print(f"Feels like at -5C/50%RH/30kmh wind: {fl}C (wind chill)")

    # Test combined corrections
    print("\n--- Combined Physics Corrections ---")
    forecast = {"precip_prob": 40, "precip_mm": 5.0}
    adjusted, corrections = apply_physics_corrections(
        forecast, pressure_hpa=998, humidity_pct=90, temp_c=22, wind_kmh=15
    )
    print(f"Input:  precip_prob=40%, precip_mm=5.0mm")
    print(f"Output: precip_prob={adjusted['precip_prob']}%, precip_mm={adjusted['precip_mm']}mm")
    for c in corrections:
        print(f"  -> {c}")

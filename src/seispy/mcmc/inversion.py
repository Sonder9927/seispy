"""The per-grid inversion point and its layer configuration.

One :class:`InversionPoint` is the complete, validated description of a single
MCMC inversion: its coordinates, the shallow layer that is active there, the
depth of the Moho and model bottom, and the reference Vs profile used to build
the prior.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from seispy.mcmc.config import Config
from seispy.mcmc.inputs import COORD_NDIGITS, coordinate_pair_key
from seispy.mcmc.velocity import VsProfile

_METRES_PER_KM = 1000.0

# ``section`` values shared by the prior, writer and plotting layers.
SEDIMENT = "sediment"
CRUST = "crust"
MANTLE = "mantle"


@dataclass(frozen=True, eq=False)
class InversionPoint:
    """One MCMC grid point: coordinates, layer interfaces and reference model."""

    lon: float
    lat: float
    water_depth: float
    sediment_thickness: float
    moho_depth: float
    max_depth: float
    water_threshold: float
    sediment_threshold: float
    smooth_on: int
    ice_on: int
    vs_profile: VsProfile

    @property
    def folder_name(self) -> str:
        return f"{self.lon:.2f}_{self.lat:.2f}"

    @property
    def water_on(self) -> int:
        # Sediment takes priority when both grid-data thresholds are exceeded.
        return int(not self.sediment_on and self.water_depth > self.water_threshold)

    @property
    def sediment_on(self) -> int:
        return int(self.sediment_thickness > self.sediment_threshold)

    @property
    def shallow_interface_depth(self) -> float:
        if self.water_on:
            return self.water_depth
        if self.sediment_on:
            return self.sediment_thickness
        return 0.0

    @property
    def crustal_spline_top(self) -> float:
        return self.shallow_interface_depth

    def validate(self) -> None:
        scalar_values = {
            "lon": self.lon,
            "lat": self.lat,
            "water_depth": self.water_depth,
            "sediment_thickness": self.sediment_thickness,
            "moho_depth": self.moho_depth,
            "max_depth": self.max_depth,
            "water_threshold": self.water_threshold,
            "sediment_threshold": self.sediment_threshold,
        }
        for name, value in scalar_values.items():
            if not np.isfinite(float(value)):
                raise ValueError(
                    f"{self.folder_name}: {name} must be finite, got {value}"
                )

        if not -90.0 <= self.lat <= 90.0:
            raise ValueError(f"{self.folder_name}: invalid latitude {self.lat}")
        if self.water_depth < 0:
            raise ValueError(f"{self.folder_name}: water_depth must be >= 0 km")
        if self.sediment_thickness < 0:
            raise ValueError(f"{self.folder_name}: sediment_thickness must be >= 0 km")
        if self.moho_depth <= 0:
            raise ValueError(f"{self.folder_name}: moho_depth must be > 0 km")
        if self.max_depth <= 0:
            raise ValueError(f"{self.folder_name}: max_depth must be > 0 km")
        if self.water_threshold < 0 or self.sediment_threshold < 0:
            raise ValueError(
                f"{self.folder_name}: water/sediment thresholds must be non-negative"
            )
        if self.water_depth >= self.moho_depth:
            raise ValueError(
                f"{self.folder_name}: water depth ({self.water_depth:.3f} km) must be "
                f"shallower than Moho ({self.moho_depth:.3f} km)"
            )
        if self.sediment_thickness >= self.moho_depth:
            raise ValueError(
                f"{self.folder_name}: sediment thickness "
                f"({self.sediment_thickness:.3f} km) must be smaller than Moho depth "
                f"({self.moho_depth:.3f} km)"
            )
        if self.moho_depth <= self.crustal_spline_top:
            raise ValueError(
                f"{self.folder_name}: Moho depth ({self.moho_depth}) must be deeper "
                f"than crustal spline top ({self.crustal_spline_top})"
            )
        if self.max_depth <= self.moho_depth:
            raise ValueError(
                f"{self.folder_name}: max depth ({self.max_depth}) must be deeper "
                f"than Moho ({self.moho_depth})"
            )

        self.vs_profile.validate()
        profile_key = coordinate_pair_key(self.vs_profile.lon, self.vs_profile.lat)
        grid_key = coordinate_pair_key(self.lon, self.lat)
        if profile_key != grid_key:
            raise ValueError(
                f"{self.folder_name}: reference Vs profile coordinate "
                f"({self.vs_profile.lon:.{COORD_NDIGITS}f}, "
                f"{self.vs_profile.lat:.{COORD_NDIGITS}f}) does not match "
                f"grid coordinate ({self.lon:.{COORD_NDIGITS}f}, "
                f"{self.lat:.{COORD_NDIGITS}f})"
            )


def build_inversion_point(
    lon: float,
    lat: float,
    topo: float,
    sediment: float,
    moho: float,
    vs_profile: VsProfile,
    cfg: Config,
) -> InversionPoint:
    """Construct one MCMC grid point from standardized source values.

    ``topo`` is elevation in metres (negative below sea level), while
    ``sediment`` and ``moho`` are positive thickness/depth values in km.
    """

    lon = float(lon)
    lat = float(lat)
    topo = float(topo)
    sediment = float(sediment)
    moho = float(moho)

    raw_values = {
        "lon": lon,
        "lat": lat,
        "topo": topo,
        "sediment": sediment,
        "moho": moho,
    }
    for name, value in raw_values.items():
        if not np.isfinite(value):
            raise ValueError(f"Grid source value {name} must be finite, got {value}")

    if sediment < 0:
        raise ValueError(
            f"Sediment thickness must be positive in km, got {sediment} "
            f"at ({lon}, {lat})"
        )
    if moho <= 0:
        raise ValueError(
            f"Moho must be positive depth in km, got {moho} at ({lon}, {lat})"
        )

    point = InversionPoint(
        lon=lon,
        lat=lat,
        smooth_on=cfg.sm_on,
        ice_on=cfg.ice_on,
        water_depth=max(0.0, -topo / _METRES_PER_KM),
        sediment_thickness=sediment,
        moho_depth=moho,
        max_depth=float(cfg.zmax_Bs),
        water_threshold=float(cfg.water_threshold),
        sediment_threshold=float(cfg.sediment_threshold),
        vs_profile=vs_profile,
    )
    point.validate()
    return point

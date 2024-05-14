"""Creates an empty grid representing the entire US in the web mercator projection with 1 km resolution."""

from pathlib import Path

import numpy as np
import rioxarray  # noqa: F401
import xarray as xr

US_BOUNDS: tuple[float, float, float, float] = (-125, 24, -66, 50)


def generate_reference_area_nc(output_file: Path) -> None:
    """Create an empty grid representing the entire US in the web mercator projection
    with 1 km resolution.
    """
    # Create a 4x4 grid covering the extent of the bounds
    x = np.linspace(US_BOUNDS[0], US_BOUNDS[2], 4)
    y = np.linspace(US_BOUNDS[1], US_BOUNDS[3], 4)

    # Create a dataarray and set WGS84 coordinates
    da = xr.DataArray(np.zeros((4, 4)), coords=[("y", y), ("x", x)])
    da.rio.set_spatial_dims("x", "y", inplace=True)
    da.rio.write_crs("epsg:4326", inplace=True)

    # Reproject to web mercator and 1km resolution
    da = da.rio.reproject("EPSG:3857", resolution=1000, bounds=US_BOUNDS, inplace=True)

    # Save the dataarray
    da.to_netcdf(output_file)


if __name__ == "__main__":
    generate_reference_area_nc(Path(__file__).parent / "assets/reference_area.nc")

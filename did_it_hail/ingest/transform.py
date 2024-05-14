"""Process data arrays prepared by scan_downloader.py and animate them."""

from dataclasses import dataclass

import numpy as np
import xarray as xr
from scipy.interpolate import griddata

HAIL_CLASSIFICATION_RANGE: tuple[int, int] = (10, 12)


def get_hail_index(da: xr.DataArray) -> xr.DataArray:
    """Calculate the hail index from the given data array."""
    # Filter to hail hydrometeor classification numbers
    hail = da.where(da >= HAIL_CLASSIFICATION_RANGE[0]).where(
        da <= HAIL_CLASSIFICATION_RANGE[1],
    )

    # Subtract 9 from hail data to get a 1-3 scale
    hail = hail - 9

    # Set nans to 0
    hail = hail.fillna(0)

    hail.name = f"{da.attrs['site_id']}_hail_index_{da.attrs['product_time']}"

    return hail


@dataclass
class GridData:
    """Represents resampled grid properties."""

    y_values: np.ndarray
    x_values: np.ndarray
    grid_y_values: np.ndarray
    grid_x_values: np.ndarray


def _get_regular_grid_from_data(
    y_source: np.ndarray,
    x_source: np.ndarray,
    scale_factor: int,
) -> GridData:
    """Generate a regular grid from the given latitude and longitude data arrays."""
    y_min, y_max, y_size = (
        y_source.min(),
        y_source.max(),
        len(y_source) * scale_factor,
    )
    x_min, x_max, x_size = (
        x_source.min(),
        x_source.max(),
        len(x_source) * scale_factor,
    )
    y_values = np.linspace(y_min, y_max, y_size)
    x_values = np.linspace(x_min, x_max, x_size)
    new_grid_x, new_grid_y = np.meshgrid(x_values, y_values)

    return GridData(y_values, x_values, new_grid_y, new_grid_x)


def resample_to_regular_grid(
    da: xr.DataArray,
    scale_factor: int = 2,
) -> xr.DataArray:
    """Resample the given data array to a regular grid."""
    new_grid = _get_regular_grid_from_data(
        da.coords["y"].values,
        da.coords["x"].values,
        scale_factor,
    )

    # Use scipy to interpolate irregularly spaced data to a regular grid
    data_new = griddata(
        (
            da.coords["y"].values.ravel(),
            da.coords["x"].values.ravel(),
        ),
        da.values.ravel(),
        (new_grid.grid_y_values, new_grid.grid_x_values),
        method="linear",
    )

    # Flip vertically and rotate left 90 degrees because of weirdness
    # with interpolation/raveling.
    data_new = np.rot90(np.flipud(data_new), 3)

    da = xr.DataArray(
        data_new,
        dims=["x", "y"],
        coords={"y": new_grid.y_values, "x": new_grid.x_values, "time": da.time},
        attrs=da.attrs,
        name=da.name,
    )

    # Set CRS
    da.rio.write_crs("epsg:4326", inplace=True)

    return da


def resample_to_reference_area(
    da: xr.DataArray,
    reference: xr.DataArray | xr.Dataset,
) -> xr.DataArray:
    """Resample to common area."""
    # Resample scan to the common area
    return da.rio.reproject_match(reference)


def minimize_data(da: xr.DataArray) -> xr.DataArray:
    """Remove all empty rows and columns from the data array."""
    da.dropna("x", how="all")
    da.dropna("y", how="all")

    return da

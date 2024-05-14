import gc
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import rioxarray  # noqa: F401
import structlog
import xarray as xr

SOURCE_DIR: Path = Path("repository/radar-scans")
INTERMEDIATE_DIR: Path = Path("repository/radar-scans-intermediate")
NC_OUTPUT_DIR: Path = Path("repository/mosaics-nc")
TIFF_OUTPUT_DIR: Path = Path("repository/mosaics-tiff")

# Get subdirectories of the source directory
day_subdirs: list[Path] = [x for x in INTERMEDIATE_DIR.iterdir() if x.is_dir()]

# Initialize the logger
logger = structlog.get_logger()


def get_radar_data(day_subdir: Path) -> dict[datetime, xr.Dataset]:
    """Load radar data from the provided directory."""
    logger.info("Loading radar data", directory=day_subdir)

    # Get all radar data files in the directory by time
    return {
        datetime.fromisoformat(str(file.stem).split("_")[-1]): xr.open_dataset(file)
        for file in day_subdir.glob("*.nc")
    }


def slice_by_time(
    data_by_time: dict[datetime, Any],
    time_delta: timedelta,
) -> dict[tuple[datetime, datetime], list[Any]]:
    """Slice the data by time."""
    logger.info("Slicing data by time")

    # Create a dictionary to store the sliced data
    sliced_data = {}

    start = min(data_by_time.keys())
    end = max(data_by_time.keys())

    # Iterate over the data and times
    while start < end:
        # Get the end time
        end_time = start + time_delta

        # Get the data for the current time slice
        matches = [da for time, da in data_by_time.items() if start <= time < end_time]

        logger.info(
            "Found matches for time slice",
            start=start,
            end=end_time,
            matches=len(matches),
        )

        # Add the data to the dictionary
        sliced_data[(start, end_time)] = matches

        # Increment the start time
        start = end_time

    return sliced_data


def create_output_dir(day: str) -> tuple[Path, Path]:
    """Create output directories for the given day."""
    logger.info("Creating output directories")

    # Create output directories for the day
    nc_output_dir = NC_OUTPUT_DIR / day
    nc_output_dir.mkdir(parents=True, exist_ok=True)
    tiff_output_dir = TIFF_OUTPUT_DIR / day
    tiff_output_dir.mkdir(parents=True, exist_ok=True)

    return nc_output_dir, tiff_output_dir


def convert_to_numpy_arrays(xr_arrays: list[xr.DataArray]) -> list[np.ndarray]:
    """Strip the xarray DataArray to a numpy array."""
    return [da.values for da in xr_arrays]


def main() -> None:
    """Run the mosaic pipeline."""
    logger.info("Starting mosaic pipeline")

    # Create top level output directories
    NC_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TIFF_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load empty reference grid
    empty_grid = xr.open_dataset("repository/empty-grid.nc", engine="rasterio")

    days: list[str] = [x.name for x in day_subdirs]
    logger.info("Found data for the following days", days=days)

    for day_subdir in day_subdirs:
        logger.info("Processing data for day", day=day_subdir.name)

        # Get radar data
        scans: dict[datetime, xr.Dataset] = get_radar_data(day_subdir)

        # Slice data by time
        slices: dict[tuple[datetime, datetime], list[xr.Dataset]] = slice_by_time(
            scans,
            timedelta(minutes=5),
        )
        del scans
        gc.collect()

        # Get max by time slice
        logger.info("Getting max by time slice")
        slices_reduced: dict[tuple[datetime, datetime], xr.Dataset] = {
            time_slice: xr.concat(data, dim="z").max("z")
            for time_slice, data in slices.items()
        }
        del slices
        gc.collect()

        # Sum the slices
        logger.info("Summing slices")
        sum_: xr.Dataset = xr.concat(
            list(slices_reduced.values()),
            dim="z",
        ).sum("z")

        # Create output directories
        nc_output_dir, tiff_output_dir = create_output_dir(day_subdir.name)

        # Save the mosaic as a NetCDF file
        sum_.to_netcdf(nc_output_dir / "mosaic.nc")

        # Save the mosaic as a GeoTIFF file
        sum_.rio.to_raster(tiff_output_dir / "mosaic.tif")


if __name__ == "__main__":
    main()

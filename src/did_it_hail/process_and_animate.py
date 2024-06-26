"""Process data arrays prepared by scan_downloader.py and animate them."""

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Pool, set_start_method
from pathlib import Path
from typing import TYPE_CHECKING

import cartopy.crs as ccrs
import ffmpeg
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import scipy
import xarray as xr
from metpy.plots import USCOUNTIES
from utils import clear_dir, file_order_generator

if TYPE_CHECKING:
    from collections.abc import Iterator

set_start_method("spawn", force=True)

INPUT_NC_DIR: Path = Path("download_cache")
OUTPUT_ROOT: Path = Path("output")
LIMIT_N_FRAMES: int = 0  # Limit for testing, 0 to disable
ANIMATION_RESOLUTION: tuple[int, int] = (1440, 1440)
ANIMATION_FRAMERATE: int = 12
RESAMPLE_SCALE_FACTOR: int = 2
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

    hail.name = f"{hail.name}_hail"

    return hail


@dataclass
class GridData:
    """Represents resampled grid properties."""

    latitudes: np.ndarray
    longitudes: np.ndarray
    grid_latitudes: np.ndarray
    grid_longitudes: np.ndarray


def get_regular_grid_from_data(
    lat_source: np.ndarray,
    lon_source: np.ndarray,
) -> GridData:
    """Generate a regular grid from the given latitude and longitude data arrays."""
    lat_min, lat_max, lat_size = (
        lat_source.min(),
        lat_source.max(),
        len(lat_source) * RESAMPLE_SCALE_FACTOR,
    )
    lon_min, lon_max, lon_size = (
        lon_source.min(),
        lon_source.max(),
        len(lon_source) * RESAMPLE_SCALE_FACTOR,
    )
    latitudes = np.linspace(lat_min, lat_max, lat_size)
    longitudes = np.linspace(lon_min, lon_max, lon_size)
    new_grid_lon, new_grid_lat = np.meshgrid(longitudes, latitudes)

    return GridData(latitudes, longitudes, new_grid_lat, new_grid_lon)


def sum_in_steps(data: list[xr.DataArray], frame_dir: Path) -> None:
    """Calculate the sum of data arrays in steps and save each resulting sum as an image."""  # noqa: E501
    sum_: xr.DataArray = data[0]

    new_grid = get_regular_grid_from_data(
        sum_.coords["lat"].values,
        sum_.coords["lon"].values,
    )

    # Use scipy to interpolate irregularly spaced data to a regular grid
    sum_new = scipy.interpolate.griddata(
        (
            sum_.coords["lat"].values.ravel(),
            sum_.coords["lon"].values.ravel(),
        ),
        sum_.values.ravel(),
        (new_grid.grid_latitudes, new_grid.grid_longitudes),
        method="linear",
    )

    # Flip vertically and rotate left 90 degrees because of weirdness
    # with interpolation/raveling.
    sum_new = np.rot90(np.flipud(sum_new), 3)

    sum_ = xr.DataArray(
        sum_new,
        dims=["lon", "lat"],
        coords={"lat": new_grid.latitudes, "lon": new_grid.longitudes},
    )
    del sum_new

    name_gen: Iterator[str] = file_order_generator()

    for i in range(1, len(data)):
        print(f"Resampling and adding frame {i}")
        data_new = scipy.interpolate.griddata(
            (
                data[i].coords["lat"].values.ravel(),
                data[i].coords["lon"].values.ravel(),
            ),
            data[i].values.ravel(),
            (new_grid.grid_latitudes, new_grid.grid_longitudes),
            method="linear",
        )

        # Flip vertically and rotate left 90 degrees because of weirdness
        # with interpolation/raveling.
        data_new = np.rot90(np.flipud(data_new), 3)

        sum_.values = np.add(sum_.values, data_new)
        sum_.name = f"hail_sum_frame_{next(name_gen)}"
        sum_.attrs = data[i].attrs

        data[i] = xr.DataArray()  # Clear memory, keep type system happy

        plot_and_save(sum_, frame_dir)


# Plot and save figure to output dir
def plot_and_save(da: xr.DataArray, dest: Path) -> None:
    """Plot and save the given DataArray as an image."""
    # Filter out zero values to make transparent
    da = da.where(da > 0)

    print(f"Plotting {da.name}")

    # Create a figure and axis
    mpl.rcParams["figure.dpi"] = 600
    fig = plt.figure(figsize=(11, 8.5))
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8], projection=ccrs.PlateCarree())  # type: ignore

    # Plot the data
    da.plot.pcolormesh(
        ax=ax,
        x="lon",
        y="lat",
        transform=ccrs.PlateCarree(),
        cmap="plasma",
        add_colorbar=True,
        cbar_kwargs={"label": "Hail Index"},
    )  # type: ignore

    ax.set_aspect("auto")

    # Add map features
    ax.add_feature(USCOUNTIES.with_scale("5m"), edgecolor="black", linewidth=0.1)  # type: ignore

    # Save the figure
    plt.savefig(dest / f"{da.name}.png")
    plt.close()


def animate_image_dir_with_ffmpeg(image_dir: Path, output_file: Path) -> None:
    """Animate the images in the specified directory using ffmpeg."""
    ffmpeg.input(
        str(image_dir / "*.png"),
        pattern_type="glob",
        framerate=ANIMATION_FRAMERATE,
    ).output(
        str(output_file),
        pix_fmt="yuv420p",
        vf=f"scale={ANIMATION_RESOLUTION[0]}:{ANIMATION_RESOLUTION[1]}",
    ).run(overwrite_output=True)


def plot_in_pool(data: list[xr.DataArray], plot_dir: Path) -> None:
    """Plot and save multiple DataArrays in parallel using multiprocessing.Pool."""
    print(f"Plotting {len(data)} frames")
    with Pool(4) as p:
        p.starmap(plot_and_save, [(da, plot_dir) for da in data])
        print("Finished plotting")


def create_output_dirs(root_dir: Path) -> tuple[Path, Path, Path]:
    """Create output directories for snapshots, running sum, and video."""
    snapshot_dir = root_dir / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    running_sum_dir = root_dir / "running_sum"
    running_sum_dir.mkdir(parents=True, exist_ok=True)
    video_dir = root_dir / "video"
    video_dir.mkdir(parents=True, exist_ok=True)

    return snapshot_dir, running_sum_dir, video_dir


def main() -> None:
    """Execute the processing and animation of hail data."""
    files: Iterator[Path] = INPUT_NC_DIR.glob("*")

    """
    Expects netCDF-packaged Xarray DataArrays with hydrometeor classification
    radar data, produced by scan_downloader.py (see get_da_from_scan() for details).
    """
    print("Clearing output directory")
    clear_dir(OUTPUT_ROOT)

    print("Creating output directories")
    snapshot_dir, running_sum_dir, video_dir = create_output_dirs(OUTPUT_ROOT)

    mpl.use("agg")

    scans: dict[datetime, xr.DataArray] = {}

    print("Loading radar scans")
    for file in files:
        da = xr.open_dataarray(file)
        time: datetime = datetime.fromisoformat(da.attrs["product_time"])
        scans[time] = da

    # Sort scans by time key
    scans = OrderedDict(sorted(scans.items()))

    # Limit for testing
    if LIMIT_N_FRAMES > 0:
        print(f"Limiting to {LIMIT_N_FRAMES} frames")
        scans = dict(list(scans.items())[:LIMIT_N_FRAMES])

    print("Extracting hail data")
    hail_data: list[xr.DataArray] = [get_hail_index(da) for da in scans.values()]
    del scans

    # Preserve order with name generator
    print("Naming hail frames")
    name_gen: Iterator[str] = file_order_generator()
    for da in hail_data:
        da.name = f"hail_frame_{next(name_gen)}"

    plot_in_pool(hail_data, snapshot_dir)

    print("Animating hail movement")
    animate_image_dir_with_ffmpeg(snapshot_dir, video_dir / "hail_movement.mp4")

    print("Summing hail frames")
    sum_in_steps(hail_data, running_sum_dir)

    print("Animating hail sum")
    animate_image_dir_with_ffmpeg(running_sum_dir, video_dir / "hail_sum.mp4")


if __name__ == "__main__":
    main()

import numpy as np
from dataclasses import dataclass
import xarray as xr
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
from metpy.plots import USCOUNTIES

import cartopy.crs as ccrs
import ffmpeg
from datetime import datetime
from collections import OrderedDict

import scipy
from multiprocessing import Pool
from typing import Iterator
from utils import file_order_generator, clear_dir


INPUT_NC_DIR: Path = Path("download_cache")
OUTPUT_ROOT: Path = Path("output")
LIMIT_N_FRAMES: int = 0  # Limit for testing
ANIMATION_RESOLUTION: tuple[int, int] = (1440, 1440)


def get_hail_index(da: xr.DataArray) -> xr.DataArray:
    # Filter to hail hydrometeor classification numbers
    hail = da.where(da >= 10).where(da <= 12)

    # Subtract 9 from hail data to get a 1-3 scale
    hail = hail - 9

    # Set nans to 0
    hail = hail.fillna(0)

    hail.name = f"{hail.name}_hail"

    return hail


@dataclass
class GridData:
    latitudes: np.ndarray
    longitudes: np.ndarray
    grid_latitudes: np.ndarray
    grid_longitudes: np.ndarray


def get_regular_grid_from_data(
    lat_source: np.ndarray, lon_source: np.ndarray
) -> GridData:
    lat_min, lat_max, lat_size = (
        lat_source.min(),
        lat_source.max(),
        len(lat_source) * 2,
    )
    lon_min, lon_max, lon_size = (
        lon_source.min(),
        lon_source.max(),
        len(lon_source) * 2,
    )
    latitudes = np.linspace(lat_min, lat_max, lat_size)
    longitudes = np.linspace(lon_min, lon_max, lon_size)
    new_grid_lon, new_grid_lat = np.meshgrid(longitudes, latitudes)

    return GridData(latitudes, longitudes, new_grid_lat, new_grid_lon)


def sum_in_steps(data: list[xr.DataArray], frame_dir: Path) -> None:
    sum: xr.DataArray = data[0]

    new_grid = get_regular_grid_from_data(
        sum.coords["lat"].values, sum.coords["lon"].values
    )

    # Use scipy to interpolate irregularly spaced data to a regular grid
    sum_new = scipy.interpolate.griddata(
        (
            sum.coords["lat"].values.ravel(),
            sum.coords["lon"].values.ravel(),
        ),
        sum.values.ravel(),
        (new_grid.grid_latitudes, new_grid.grid_longitudes),
        method="linear",
    )

    # Flip vertically and rotate left 90 degrees because of weirdness
    # with interpolation/raveling.
    sum_new = np.rot90(np.flipud(sum_new), 3)

    sum = xr.DataArray(
        sum_new,
        dims=["lon", "lat"],
        coords={"lat": new_grid.latitudes, "lon": new_grid.longitudes},
    )
    del sum_new

    name_gen: Iterator[str] = file_order_generator()

    for i in range(1, len(data)):
        print(f"Summing frame {i}")
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

        data[i] = xr.DataArray(
            data_new,
            dims=["lon", "lat"],
            coords={"lon": new_grid.longitudes, "lat": new_grid.latitudes},
        )
        sum.values = np.add(sum.values, data[i].values)
        sum.name = f"hail_sum_frame_{next(name_gen)}"

        plot_and_save(sum, frame_dir)


# Plot and save figure to output dir
def plot_and_save(da: xr.DataArray, dest: Path) -> None:
    # Filter out zero values to make transparent
    da = da.where(da > 0)

    print(f"Plotting {da.name}")

    # Create a figure and axis
    matplotlib.rcParams["figure.dpi"] = 600
    fig = plt.figure(figsize=(11, 8.5))  # noqa
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
    ffmpeg.input(str(image_dir / "*.png"), pattern_type="glob", framerate=5).output(
        str(output_file),
        pix_fmt="yuv420p",
        vf=f"scale={ANIMATION_RESOLUTION[0]}:{ANIMATION_RESOLUTION[1]}",
    ).run(overwrite_output=True)


def plot_in_pool(data: list[xr.DataArray], plot_dir: Path) -> None:
    print(f"Plotting {len(data)} frames")
    with Pool(8) as p:
        p.starmap(plot_and_save, [(da, plot_dir) for da in data])
        print("Finished plotting")


def create_output_dirs(root_dir: Path) -> tuple[Path, Path, Path]:
    snapshot_dir = root_dir / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    running_sum_dir = root_dir / "running_sum"
    running_sum_dir.mkdir(parents=True, exist_ok=True)
    video_dir = root_dir / "video"
    video_dir.mkdir(parents=True, exist_ok=True)

    return snapshot_dir, running_sum_dir, video_dir


def main():
    files: Iterator[Path] = INPUT_NC_DIR.glob("*")

    """
    Expects netCDF-packaged Xarray DataArrays with hydrometeor classification
    radar data, produced by scan_downloader.py (see get_da_from_scan() for details).
    """

    clear_dir(OUTPUT_ROOT)
    snapshot_dir, running_sum_dir, video_dir = create_output_dirs(OUTPUT_ROOT)

    matplotlib.use("agg")

    scans: dict[datetime, xr.DataArray] = {}

    for file in files:
        da = xr.open_dataarray(file)
        time: datetime = datetime.fromisoformat(da.attrs["product_time"])
        scans[time] = da

    # Sort scans by time key
    scans = OrderedDict(sorted(scans.items()))

    # Limit for testing
    if LIMIT_N_FRAMES > 0:
        scans = dict(list(scans.items())[:LIMIT_N_FRAMES])

    hail_data: list[xr.DataArray] = [get_hail_index(da) for da in scans.values()]
    del scans

    # Preserve order with name generator
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

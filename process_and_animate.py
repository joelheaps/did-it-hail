import numpy as np
from dataclasses import dataclass
import xarray as xr
from metpy.calc import azimuth_range_to_lat_lon
from metpy.io import Level3File
from metpy.units import units
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
from metpy.plots import USCOUNTIES

import cartopy.crs as ccrs
from natsort import natsorted
import ffmpeg

import scipy
from multiprocessing import Pool
from typing import Iterator


INPUT_RADAR_DIR: Path = Path("input")
OUTPUT_ROOT: Path = Path("output")


def file_order_generator() -> Iterator[str]:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    for first in alphabet:
        for second in alphabet:
            for third in alphabet:
                yield first + second + third


def get_hail_data(file: Path) -> xr.DataArray:
    print(f"Loading {file}")
    f = Level3File(file)

    # Pull the data out of the file object
    datadict = f.sym_block[0][0]

    # Turn into an array using the scale specified by the file
    data = f.map_data(datadict["data"])

    # Grab azimuths and calculate a range based on number of gates,
    # both with their respective units
    azimuth = units.Quantity(
        np.array(datadict["start_az"] + [datadict["end_az"][-1]]), "degrees"
    )
    range = units.Quantity(
        np.linspace(0, f.max_range, data.shape[-1] + 1), "kilometers"
    )

    # Extract central latitude and longitude from the file
    cent_lon = f.lon
    cent_lat = f.lat

    # Convert az,range to x,y
    xlocs, ylocs = azimuth_range_to_lat_lon(azimuth, range, cent_lon, cent_lat)

    # First and last row/column are duplicates, so drop the last of each
    ylocs = ylocs[:-1, :-1]
    xlocs = xlocs[:-1, :-1]

    # Pull the data out of the file object
    datadict = f.sym_block[0][0]

    # Turn into an array using the scale specified by the file
    data = f.map_data(datadict["data"])

    # Create an Xarray dataarray with the data
    latitudes = xr.DataArray(ylocs, dims=["y", "x"], name="latitude")
    longitudes = xr.DataArray(xlocs, dims=["y", "x"], name="longitude")
    data = xr.DataArray(
        data, dims=["y", "x"], coords={"latitude": latitudes, "longitude": longitudes}
    )

    # Filter to hail hydrometeor classification numbers
    hail = data.where(data >= 10).where(data <= 12)

    # Subtract 9 from hail data to get a 1-3 scale
    hail = hail - 9

    # Set nans to 0
    hail = hail.fillna(0)

    hail.name = file.name

    return hail


@dataclass
class GridData:
    latitudes: np.ndarray
    longitudes: np.ndarray
    grid_latitudes: np.ndarray
    grid_longitudes: np.ndarray


def get_regular_grid_from_data(data: xr.DataArray) -> GridData:
    lat_min, lat_max, lat_size = (
        data.coords["latitude"].min(),
        data.coords["latitude"].max(),
        len(data.coords["latitude"]),
    )
    lon_min, lon_max, lon_size = (
        data.coords["longitude"].min(),
        data.coords["longitude"].max(),
        len(data.coords["longitude"]),
    )
    latitudes = np.linspace(lat_min, lat_max, lat_size)
    longitudes = np.linspace(lon_min, lon_max, lon_size)
    new_grid_lon, new_grid_lat = np.meshgrid(longitudes, latitudes)

    return GridData(latitudes, longitudes, new_grid_lat, new_grid_lon)


def sum_in_steps(data: list[xr.DataArray], frame_dir: Path) -> xr.DataArray:
    sum: xr.DataArray = data[0].copy()

    new_grid = get_regular_grid_from_data(sum)

    # Use scipy to interpolate irregularly spaced data to a regular grid
    sum_new = scipy.interpolate.griddata(
        (
            sum.coords["latitude"].values.ravel(),
            sum.coords["longitude"].values.ravel(),
        ),
        sum.values.ravel(),
        (new_grid.grid_latitudes, new_grid.grid_longitudes),
        method="linear",
    )

    sum = xr.DataArray(
        sum_new,
        dims=["longitude", "latitude"],
        coords={"latitude": new_grid.latitudes, "longitude": new_grid.longitudes},
    )

    name_gen: Iterator[str] = file_order_generator()

    for i in range(1, len(data)):
        print(f"Summing frame {i}")
        data_new = scipy.interpolate.griddata(
            (
                data[i].coords["latitude"].values.ravel(),
                data[i].coords["longitude"].values.ravel(),
            ),
            data[i].values.ravel(),
            (new_grid.grid_latitudes, new_grid.grid_longitudes),
            method="linear",
        )

        data[i] = xr.DataArray(
            data_new,
            dims=["longitude", "latitude"],
            coords={"longitude": new_grid.longitudes, "latitude": new_grid.latitudes},
        )
        sum.values = np.add(sum.values, data[i].values)
        sum.name = f"hail_sum_frame_{next(name_gen)}"

        plot_and_save(sum, frame_dir)


# Plot and save figure to output dir
def plot_and_save(array: xr.DataArray, dest: Path) -> None:
    plot_array = array.where(array > 0).copy()

    print(f"Plotting {array.name}")

    # Create a figure and axis
    # Make sure array plot fills entire window
    fig = plt.figure(figsize=(50, 50))
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Set extent to min/max of data
    ax.set_extent(
        [
            plot_array.longitude.min(),
            plot_array.longitude.max(),
            plot_array.latitude.min(),
            plot_array.latitude.max(),
        ]
    )

    # Plot the data
    plot_array.plot.pcolormesh(
        ax=ax,
        x="longitude",
        y="latitude",
        transform=ccrs.PlateCarree(),
        cmap="viridis",
        add_colorbar=False,
    )

    ax.set_aspect("auto")

    # Add map features
    ax.add_feature(USCOUNTIES.with_scale("5m"), edgecolor="black", linewidth=0.8)

    # Save the figure
    plt.savefig(dest / f"{array.name}.png")
    plt.close()


def animate_image_dir_with_ffmpeg(image_dir: Path, output_file: Path) -> None:
    ffmpeg.input(str(image_dir / "*.png"), pattern_type="glob", framerate=12).output(
        str(output_file), vcodec="libx265", crf=20
    ).run()


def plot_in_pool(data: list[xr.DataArray], plot_dir: Path) -> None:
    print(f"Plotting {len(data)} frames")
    with Pool(8) as p:
        p.starmap(plot_and_save, [(array, plot_dir) for array in data])
        print("Finished plotting")


def main():
    files: list[Path] = INPUT_RADAR_DIR.glob("*")
    files: list[Path] = natsorted(files)  # Sort files in natural order

    # Create output subfolders, for hail snapshots and running sum
    # Delete output root
    if OUTPUT_ROOT.exists():
        for file in OUTPUT_ROOT.rglob("*"):
            if file.is_file():
                file.unlink()
    snapshot_dir = OUTPUT_ROOT / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    running_sum_dir = OUTPUT_ROOT / "running_sum"
    running_sum_dir.mkdir(parents=True, exist_ok=True)
    video_dir = OUTPUT_ROOT / "video"
    video_dir.mkdir(parents=True, exist_ok=True)

    matplotlib.use("agg")

    data = [get_hail_data(file) for file in files]
    name_gen: Iterator[str] = file_order_generator()

    for da in data:
        da_name = f"hail_frame_{next(name_gen)}"
        print(f"Setting name of {da.name} to {da_name}")
        da.name = da_name

    plot_in_pool(data, snapshot_dir)

    print("Animating hail movement")
    animate_image_dir_with_ffmpeg(snapshot_dir, video_dir / "hail_movement.mp4")

    print("Summing hail frames")
    sum_in_steps(data, running_sum_dir)

    print("Animating hail sum")
    animate_image_dir_with_ffmpeg(running_sum_dir, video_dir / "hail_sum.mp4")


if __name__ == "__main__":
    main()

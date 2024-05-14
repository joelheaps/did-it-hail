from datetime import UTC, datetime, timedelta
from pathlib import Path

import cartopy.crs as ccrs
import matplotlib as mpl
import matplotlib.pyplot as plt
import xarray as xr
from metpy.plots import USCOUNTIES


def save_nc(da: xr.DataArray, dest_file: Path) -> None:
    """Save the given DataArray as a NetCDF file."""
    comp = {"zlib": True, "complevel": 5}
    da.to_netcdf(dest_file, encoding={da.name: comp}, engine="netcdf4")


def save_plot(da: xr.DataArray, dest_file: Path) -> None:
    """Plot and save the given DataArray as an image."""
    # Filter out zero values to make transparent
    da = da.where(da > 0)

    # Create a figure and axis
    mpl.rcParams["figure.dpi"] = 600
    fig = plt.figure(figsize=(11, 8.5))
    ax = fig.add_axes((0.1, 0.1, 0.8, 0.8), projection=ccrs.PlateCarree())

    # Plot the data
    da.plot.pcolormesh(
        ax=ax,
        x="x",
        y="y",
        transform=ccrs.PlateCarree(),
        cmap="plasma",
        add_colorbar=True,
        cbar_kwargs={"label": "Hail Index"},
    )  # type: ignore

    ax.set_aspect("auto")

    # Add map features
    ax.add_feature(USCOUNTIES.with_scale("5m"), edgecolor="black", linewidth=0.1)  # type: ignore

    # Save the figure
    plt.savefig(dest_file.resolve(), bbox_inches="tight")
    plt.close()


class Repository:
    """Handles storing hail data in a defined folder structure."""

    def __init__(self, root: Path, sub: str) -> None:
        """Initialize the Repository class."""
        self.root = root
        self.sub = sub
        self.path = self.root / self.sub

        # Create the root directory if it doesn't exist
        self.path.mkdir(parents=True, exist_ok=True)

    def __get_cdt_iso_date(self, iso_time: str) -> str:
        utc_time = datetime.fromisoformat(iso_time).replace(tzinfo=UTC)
        cdt = utc_time - timedelta(hours=5)
        return cdt.strftime("%Y-%m-%d")

    def store(self, da: xr.DataArray) -> Path:
        """Store the given DataArray in the repository."""
        date_str = self.__get_cdt_iso_date(da.attrs["product_time"])

        # Create a directory for the date
        date_dir: Path = self.path / date_str
        date_dir.mkdir(parents=True, exist_ok=True)

        # Save the NetCDF file
        nc_file = date_dir / f"{da.name}.nc"
        save_nc(da, nc_file)
        return nc_file

"""Downloads radar scans from the NOAA FTP server and converts them to netCDF files."""

import ftplib
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

import numpy as np
import rioxarray  # noqa: F401
import xarray as xr
from dateutil.parser import parse
from metpy.calc import azimuth_range_to_lat_lon
from metpy.io import Level3File
from metpy.units import units

from did_it_hail.enums import RadarSite

if TYPE_CHECKING:
    from collections.abc import Iterator


def _convert_l3_product_to_da(l3f: Level3File) -> xr.DataArray:
    """Convert radar scan data to a DataArray."""
    payload: dict = l3f.sym_block[0][0]
    product_time: datetime = l3f.metadata["prod_time"]

    # Convert payload to numpy array
    data: np.ndarray = l3f.map_data(payload["data"])

    range_steps: int = data.shape[-1]  # Probably 1200
    ranges = units.Quantity(np.linspace(0, l3f.max_range, range_steps), "kilometers")
    averaged_azimuths = _get_azimuth_midpoints(
        np.array(payload["start_az"]),
        np.array(payload["end_az"]),
    )
    azimuths = units.Quantity(averaged_azimuths, "degrees")

    # Convert azimuths and ranges to lat/lon
    lons, lats = azimuth_range_to_lat_lon(azimuths, ranges, l3f.lon, l3f.lat)

    # Construct DataArray
    da = xr.DataArray(
        data,
        coords={
            "time": product_time,
            "range": ranges,
            "azimuth": azimuths,
            "y": (("azimuth", "range"), lats),
            "x": (("azimuth", "range"), lons),
        },
        dims=["azimuth", "range"],
    )

    # Set metadata
    da.name = f"{l3f.siteID}_{l3f.product_name}_{product_time.isoformat()}"
    da.attrs["site_id"] = l3f.siteID
    da.attrs["site_lat"] = l3f.lat
    da.attrs["site_lon"] = l3f.lon
    da.attrs["max_range"] = l3f.max_range
    da.attrs["product_name"] = l3f.product_name
    da.attrs["product_time"] = product_time.isoformat()

    return da


def _get_azimuth_midpoints(
    start_azimuths: np.ndarray,
    end_azimuths: np.ndarray,
) -> np.ndarray:
    """Average start_azimuths[n] and end_azimuths[n] to get the midpoint azimuth."""
    return (start_azimuths + end_azimuths) / 2


class NoaaRadarProductDownloader:
    """Class for downloading radar hydrometeor classification product from the NOAA FTP
    server.
    """

    latest_filename: str = "sn.last"

    def __init__(self, ftp_server: str, product_path: str) -> None:
        """Initialize the FtpRadarDownloader class."""
        self.ftp_server = ftp_server

        assert not product_path.endswith("/")
        self.product_path = product_path

        self.ftp = ftplib.FTP(self.ftp_server)  # noqa: S321
        self.ftp.login()
        self.ftp.cwd(self.product_path)

    def get_product(self, filename: str) -> xr.DataArray:
        """Download a radar scan from the FTP server."""
        with NamedTemporaryFile() as output_file:
            self.ftp.retrbinary(f"RETR {filename}", output_file.write)
            output_file.seek(0)
            l3f: Level3File = Level3File(output_file.name)
            return _convert_l3_product_to_da(l3f)

    def get_latest_product_by_site(self, site: RadarSite) -> xr.DataArray:
        """Write the latest radar file to output_file."""
        site_str = f"SI.{site}"
        return self.get_product(f"{site_str}/{self.latest_filename}")

    def list_product_by_timestamp(self, site: RadarSite) -> dict[datetime, str]:
        """Get a list of all radar files and their timestamps."""
        site_str = f"SI.{site}"
        files: Iterator[tuple[str, dict[str, str]]] = self.ftp.mlsd(site_str)
        files_by_time = {}

        for file in files:
            name = file[0]
            timestamp = file[1]["modify"]

            # Parse timestamp according to the format used by the FTP server
            # YYYYMMDDHHMMSS[.sss]
            timestamp = parse(timestamp)
            files_by_time[timestamp] = name

        return files_by_time

    def __del__(self) -> None:
        """Close the FTP connection."""
        self.ftp.quit()

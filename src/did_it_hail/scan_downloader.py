"""Downloads radar scans from the NOAA FTP server and converts them to netCDF files."""

import ftplib
import time
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import xarray as xr
from metpy.calc import azimuth_range_to_lat_lon
from metpy.io import Level3File
from metpy.units import units

FTP_URL: str = "tgftp.nws.noaa.gov"
PRODUCT_PATH: str = "/SL.us008001/DF.of/DC.radar/DS.165h0/SI.koax"
DEST_DIR: Path = Path("download_cache")


class _FtpRadarDownloader:
    latest_filename: str = "sn.last"

    def __init__(self, ftp_url: str, product_path: str) -> None:
        self.ftp_url = ftp_url
        self.product_path = product_path
        self.ftp = ftplib.FTP(self.ftp_url)  # noqa: S321
        self.ftp.login()
        self.ftp.cwd(self.product_path)

    def get_latest_radar_file(self, output_file: Path) -> None:
        """Write the latest radar file to output_file."""
        with output_file.open("wb") as f:
            self.ftp.retrbinary(f"RETR {self.latest_filename}", f.write)

    def __del__(self) -> None:
        self.ftp.quit()


def get_azimuth_midpoints(
    start_azimuths: np.ndarray,
    end_azimuths: np.ndarray,
) -> np.ndarray:
    """Average start_azimuths[n] and end_azimuths[n] to get the midpoint azimuth."""
    return (start_azimuths + end_azimuths) / 2


def get_da_from_scan(radar_file: Path) -> xr.DataArray:
    """Convert radar scan data to a DataArray."""
    l3f: Level3File = Level3File(radar_file)
    payload: dict = l3f.sym_block[0][0]
    product_time: datetime = l3f.metadata["prod_time"]

    # Convert payload to numpy array
    data: np.ndarray = l3f.map_data(payload["data"])

    range_steps: int = data.shape[-1]  # Probably 1200
    ranges = units.Quantity(np.linspace(0, l3f.max_range, range_steps), "kilometers")
    averaged_azimuths = get_azimuth_midpoints(
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
            "lat": (("azimuth", "range"), lats),
            "lon": (("azimuth", "range"), lons),
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


def download_and_convert_last_radar_scan() -> None:
    """Download and convert the latest radar scan to a netcdf file."""
    ftp_downloader = _FtpRadarDownloader(FTP_URL, PRODUCT_PATH)
    with NamedTemporaryFile() as tmp_file:
        ftp_downloader.get_latest_radar_file(Path(tmp_file.name))
        da = get_da_from_scan(Path(tmp_file.name))
    print(da)

    # Save da
    da.to_netcdf(DEST_DIR / f"{da.name}.nc")


def main() -> None:
    """Download the latest radar scan and save it to a netcdf file, in a loop."""
    # Every 120s, download the latest radar scan and save it to a netcdf file
    while True:
        try:
            download_and_convert_last_radar_scan()
        except Exception as e:  # noqa: BLE001
            print(f"Error: {e}")
        print("Sleeping for 120s...")
        time.sleep(120)


if __name__ == "__main__":
    main()

import ftplib
from pathlib import Path
from tempfile import NamedTemporaryFile
import xarray as xr
import numpy as np
from metpy.io import Level3File
from metpy.calc import azimuth_range_to_lat_lon
from datetime import datetime
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from metpy.units import units
import time

FTP_URL: str = "tgftp.nws.noaa.gov"
PRODUCT_PATH: str = "/SL.us008001/DF.of/DC.radar/DS.165h0/SI.koax"
DEST_DIR: Path = Path("download_cache")


class FtpRadarDownloader:
    latest_filename: str = "sn.last"

    def __init__(self, ftp_url: str, product_path: str):
        self.ftp_url = ftp_url
        self.product_path = product_path
        self.ftp = ftplib.FTP(self.ftp_url)
        self.ftp.login()
        self.ftp.cwd(self.product_path)

    def get_latest_radar_file(self, output_file: Path) -> None:
        "Write the latest radar file to output_file"
        with output_file.open("wb") as f:
            self.ftp.retrbinary(f"RETR {self.latest_filename}", f.write)

    def __del__(self):
        self.ftp.quit()


def get_azimuth_midpoints(
    start_azimuths: np.ndarray, end_azimuths: np.ndarray
) -> np.ndarray:
    """
    Average start_azimuths[n] and end_azimuths[n] to get the midpoint azimuth.
    """
    return (start_azimuths + end_azimuths) / 2


def get_da_from_scan(radar_file: Path) -> xr.DataArray:
    l3f: Level3File = Level3File(radar_file)
    payload: dict = l3f.sym_block[0][0]
    product_time: datetime = l3f.metadata["prod_time"]

    # Convert payload to numpy array
    data: np.ndarray = l3f.map_data(payload["data"])

    range_steps: int = data.shape[-1]  # Probably 1200
    ranges = units.Quantity(np.linspace(0, l3f.max_range, range_steps), "kilometers")
    azimuths = units.Quantity(
        get_azimuth_midpoints(
            np.array(payload["start_az"]), np.array(payload["end_az"])
        ),
        "degrees",
    )

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
    da.attrs["product_name"] = l3f.product_name
    da.attrs["product_time"] = product_time.isoformat()
    da.attrs["max_range"] = l3f.max_range
    da.attrs["site_lat"] = l3f.lat
    da.attrs["site_lon"] = l3f.lon

    return da


def download_da():
    ftp_downloader = FtpRadarDownloader(FTP_URL, PRODUCT_PATH)
    with NamedTemporaryFile() as tmp_file:
        ftp_downloader.get_latest_radar_file(Path(tmp_file.name))
        da = get_da_from_scan(Path(tmp_file.name))
    print(da)

    # Save da
    da.to_netcdf(DEST_DIR / f"{da.name}.nc")


def main():
    # Every 120s, download the latest radar scan and save it to a netcdf file
    while True:
        try:
            download_da()
        except Exception as e:
            print(f"Error: {e}")
        print("Sleeping for 120s...")
        time.sleep(120)


if __name__ == "__main__":
    main()

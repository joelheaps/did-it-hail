import time
from collections.abc import Iterator
from pathlib import Path

import schedule
import structlog
import xarray as xr

from did_it_hail.config import ENABLED_SITES
from did_it_hail.ingest.download import NoaaRadarProductDownloader
from did_it_hail.ingest.store import Repository
from did_it_hail.ingest.transform import (
    get_hail_index,
    minimize_data,
    resample_to_reference_area,
    resample_to_regular_grid,
)
from did_it_hail.utils.generate_grid import generate_reference_area_nc

logger = structlog.get_logger()

FTP_SERVER: str = "tgftp.nws.noaa.gov"
FTP_SUBDIR: str = "/SL.us008001/DF.of/DC.radar/DS.165h0"
_REFERENCE_AREA_FILE: Path = Path(__file__).parent / "assets/reference_area.nc"
REPOSITORY: Path = Path(__file__).parent.parent / "repository"


def get_reference_area() -> xr.DataArray:
    if not _REFERENCE_AREA_FILE.exists():
        generate_reference_area_nc(_REFERENCE_AREA_FILE)
    return xr.open_dataarray(_REFERENCE_AREA_FILE)


def pipeline() -> None:
    """Run the pipeline."""
    logger.info("Starting pipeline")

    downloader = NoaaRadarProductDownloader(
        ftp_server=FTP_SERVER,
        product_path=FTP_SUBDIR,
    )
    repo = Repository(root=REPOSITORY, sub="hail_index")
    referendce_area: xr.DataArray = get_reference_area()

    data: Iterator[xr.DataArray] = (
        downloader.get_latest_product_by_site(site) for site in ENABLED_SITES
    )
    data = (get_hail_index(da) for da in data)
    data = (resample_to_regular_grid(da) for da in data)
    data = (resample_to_reference_area(da, referendce_area) for da in data)
    data = (minimize_data(da) for da in data)
    paths = (repo.store(da) for da in data)

    logger.info("Pipeline completed", files=paths)


if __name__ == "__main__":
    # Run the pipeline every 3 minutes, starting now
    logger.info("Scheduling pipeline")
    schedule.every(4).minutes.do(pipeline)
    pipeline()
    while True:
        schedule.run_pending()
        time.sleep(1)

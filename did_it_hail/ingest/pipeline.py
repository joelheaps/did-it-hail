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

logger = structlog.get_logger()

FTP_SERVER: str = "tgftp.nws.noaa.gov"
FTP_SUBDIR: str = "/SL.us008001/DF.of/DC.radar/DS.165h0"
_REFERENCE_AREA_FILE: Path = Path(__file__).parent / "assets/reference_area.nc"
REFERENCE_AREA: xr.DataArray = xr.open_dataarray(_REFERENCE_AREA_FILE)
REPOSITORY: Path = Path(__file__).parent.parent / "repository"


def pipeline() -> None:
    """Run the pipeline."""
    logger.info("Starting pipeline")

    downloader = NoaaRadarProductDownloader(
        ftp_server=FTP_SERVER,
        product_path=FTP_SUBDIR,
    )
    repo = Repository(root=REPOSITORY, sub="hail_index")

    data: Iterator[xr.DataArray] = (
        downloader.get_latest_product_by_site(site) for site in ENABLED_SITES
    )
    data = (get_hail_index(da) for da in data)
    data = (resample_to_regular_grid(da) for da in data)
    data = (resample_to_reference_area(da, REFERENCE_AREA) for da in data)
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

import logging
import sys

from dotenv import load_dotenv

from src.process_new_email.common import TableUpdater
from src.process_new_email.helper_table_updaters.countries import CountriesUpdater
from src.process_new_email.helper_table_updaters.companies import CompaniesUpdater
from src.process_new_email.helper_table_updaters.operating_sites import OperatingSitesUpdater
from src.process_new_email.SR_processors.companies.MÁV import MavUpdater
from src.process_new_email.SR_processors.companies.GYSEV import GysevUpdater
from src.OSM_processors.downloader import OsmDownloader


# future: mark all packages as namespace packages in the IDE when https://youtrack.jetbrains.com/issue/PY-55212/ is fixed


def main(demonstration=False) -> None:
    if demonstration:
        logging.basicConfig(
            encoding="utf-8",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler("kalauz.log"),
            ],
            format="%(asctime)s \t %(message)s",
            level=logging.INFO,
        )
        load_dotenv()
    else:
        logging.basicConfig(
            encoding="utf-8",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("kalauz.log"),
            ],
            format="%(asctime)s \t %(levelname)s \t %(name)s.%(funcName)s(): %(message)s",
            level=logging.DEBUG,
        )
        load_dotenv()

    # future: remove comments below when https://github.com/python/mypy/issues/10160 or https://github.com/python/mypy/issues/9756 is fixed
    updaters_to_run: list[TableUpdater] = [  # type: ignore
        # CountriesUpdater,  # type: ignore
        # CompaniesUpdater,  # type: ignore
        # OperatingSitesUpdater,  # type: ignore
        # MavUpdater,  # type: ignore
        # GysevUpdater,  # type: ignore
    ]
    for updater in updaters_to_run:
        # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-52210/ is fixed
        # noinspection PyCallingNonCallable

        # future: report bug (false positive) to mypy developers
        updater = updater()  # type: ignore
        updater.process_data()
        updater.store_data()

        updater.logger.info(f"Table `{updater.TABLE_NAME}` sucessfully updated!")

    OsmDownloader().run()


if __name__ == "__main__":
    main()

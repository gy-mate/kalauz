import logging
import sys

from dotenv import load_dotenv

from src.new_data_processors.SR_processors.files import NewFilesRegistrar
from src.new_data_processors.helper_table_updaters.countries import CountriesUpdater
from src.new_data_processors.helper_table_updaters.companies import CompaniesUpdater
from src.new_data_processors.helper_table_updaters.operating_sites import (
    OperatingSitesUpdater,
)
from src.new_data_processors.SR_processors.companies.MÁV import MavUpdater
from src.new_data_processors.SR_processors.companies.GYSEV import GysevUpdater
from src.OSM_processors.mapper import Mapper


# future: mark all packages as namespace packages in the IDE when https://youtrack.jetbrains.com/issue/PY-55212/ is fixed


def main(
    demonstration=False,
    show_lines_with_no_data=True,
) -> None:
    configure_logging(demonstration)
    logging.getLogger(__name__).info("Program started...")
    
    load_dotenv()

    # CountriesUpdater().run()
    # CompaniesUpdater().run()
    # OperatingSitesUpdater().run()
    #
    # NewFilesRegistrar().run()
    # MavUpdater().run()
    # GysevUpdater().run()

    Mapper(show_lines_with_no_data).run()

    logging.getLogger(__name__).info("...program finished!")


def configure_logging(demonstration: bool) -> None:
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
    else:
        logging.basicConfig(
            encoding="utf-8",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("kalauz.log"),
            ],
            format="%(asctime)s  %(levelname)s \t \"%(pathname)s:%(lineno)d\": %(message)s",
            level=logging.DEBUG,
        )


if __name__ == "__main__":
    main()

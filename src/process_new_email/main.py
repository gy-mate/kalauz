import logging

from dotenv import load_dotenv

from src.process_new_email.database_connection import Database
from src.process_new_email.table_updaters.common import HelperTableUpdater

from src.process_new_email.table_updaters.companies import CompaniesUpdater
from src.process_new_email.table_updaters.country_codes import CountryCodesUpdater


def main() -> None:
    logging.basicConfig(
        encoding="utf-8",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("kalauz.log"),
        ],
        format="%(asctime)s – %(levelname)s @ %(name)s.%(funcName)s(): %(message)s",
        level=logging.DEBUG,
    )
    load_dotenv()

    database = Database("kalauz")
    # TODO: remove comments below when https://github.com/python/mypy/issues/10160 or https://github.com/python/mypy/issues/9756 is fixed
    updaters_to_run: list[HelperTableUpdater] = [  # type: ignore
        CountryCodesUpdater,  # type: ignore
        CompaniesUpdater,  # type: ignore
    ]

    for updater in updaters_to_run:
        # TODO: remove the line below when https://youtrack.jetbrains.com/issue/PY-52210/ is fixed
        # noinspection PyCallingNonCallable
        
        # TODO: report false positive bug to mypy developers
        updater = updater(database)  # type: ignore
        
        updater.process_data()
        updater.store_data()

        updater.logger.info(f"Table `{updater.TABLE_NAME}` sucessfully updated!")


if __name__ == "__main__":
    main()

import logging

from dotenv import load_dotenv

from src.process_new_email.database_connection import DatabaseServer
from src.process_new_email.table_updaters.country_codes import CountryCodesUpdater


def main() -> None:
    logging.basicConfig(
        encoding="utf-8",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler("kalauz.log")],
        format="%(asctime)s – %(levelname)s @ %(name)s @ %(funcName)s: %(message)s",
        level=logging.DEBUG,
    )
    
    load_dotenv()

    database_connection = DatabaseServer("kalauz")

    update_country_codes(
        database_connection, "https://uic.org/spip.php?action=telecharger&arg="
    )


def update_country_codes(database_connection, uic_root_url: str) -> None:
    country_codes_updater = CountryCodesUpdater(
        database_connection, f"{uic_root_url}322", f"{uic_root_url}320"
    )

    country_codes_updater.process_data()
    country_codes_updater.store_data()

    country_codes_updater.logger.info("Table `country_codes` sucessfully updated!")


if __name__ == "__main__":
    main()

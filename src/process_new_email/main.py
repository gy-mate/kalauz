from dotenv import load_dotenv

from src.process_new_email.table_updaters.country_codes import CountryCodesUpdater


def main():
    load_dotenv()
    
    update_country_codes('https://uic.org/spip.php?action=telecharger&arg=')
    
    
def update_country_codes(uic_root_url: str) -> None:
    country_codes_updater = CountryCodesUpdater(
        f'{uic_root_url}322',
        f'{uic_root_url}320'
    )
    
    country_codes_updater.process_data()
    country_codes_updater.store_data()
    
    country_codes_updater.logger.info("Table `country_codes` sucessfully updated!")


if __name__ == '__main__':
    main()

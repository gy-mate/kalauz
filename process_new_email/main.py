from process_new_email.table_updaters.country_codes import CountryCodesUpdater


def update_country_codes(uic_root_url: str) -> None:
    country_codes_updater = CountryCodesUpdater(
        f'{uic_root_url}322',
        f'{uic_root_url}320'
    )
    
    country_codes_updater.data_to_process = country_codes_updater.download_data(
        country_codes_updater.data_url
    )
    country_codes_updater.xsd_to_process = country_codes_updater.download_data(
        country_codes_updater.xsd_url
    )
    
    country_codes_updater.process_data()


def main():
    update_country_codes('https://uic.org/spip.php?action=telecharger&arg=')


if __name__ == '__main__':
    main()

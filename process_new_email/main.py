from process_new_email.table_updaters.country_codes import CountryCodesUpdater


def update_country_codes(uic_root_url: str) -> None:
    country_codes_updater = CountryCodesUpdater(
        f'{uic_root_url}322',
        f'{uic_root_url}320'
    )
    
    country_codes_updater.process_data()
    country_codes_updater.store_data()


def main():
    # table_name = 'countries'
    # check_table_query = \
    #     f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    #
    # self.cursor.execute(check_table_query)
    # table_exists = self.cursor.fetchone()[0] == 1
    update_country_codes('https://uic.org/spip.php?action=telecharger&arg=')


if __name__ == '__main__':
    main()
